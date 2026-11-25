package dostoevsky

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
)

// The in-memory model of the segmented log's durable state (design v2, change (a)). It is
// what a checkpoint captures and what replay of the frames after a checkpoint maintains:
// the partition-id table, each partition's retention floor and next offset, each consumer
// group's position, and the manifest. It holds no per-record data, so its size is a
// function of the number of partitions and groups, never of the number of records.
//
// Nothing in the engine uses it yet. The semantics mirror Node.index (records must arrive
// at the next offset; a commit must come from the group's current position and may not pass
// the end of the partition) so the engine switch can adopt it without a change in behaviour.

var (
	errLogUndefinedPartition = errors.New("log state: record or commit for a partition id with no definition")
	errLogOffset             = errors.New("log state: record offset is not the partition's next offset")
	errLogConflict           = errors.New("log state: commit does not follow the group's position")
	errLogLagging            = errors.New("log state: a group is behind the requested retention floor")
	errLogInvalid            = errors.New("log state: invalid")
)

type partState struct {
	base, next int64 // the retention floor and the next offset to assign
	groups     map[string]Position
}

type logState struct {
	config  []byte // the manifest (JSON of Config), as the last checkpoint carried it
	byPID   map[uint32]Partition
	byPart  map[Partition]uint32
	parts   map[uint32]*partState
	nextPID uint32
}

func newLogState() *logState {
	return &logState{byPID: map[uint32]Partition{}, byPart: map[Partition]uint32{}, parts: map[uint32]*partState{}}
}

// cloneState copies a byte slice and normalizes empty to nil, so two states that differ only in
// nil versus empty compare equal.
func cloneState(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}

func (s *logState) empty() bool {
	return len(s.config) == 0 && len(s.byPID) == 0
}

func (s *logState) setConfig(b []byte) { s.config = cloneState(b) }

// define maps a partition id to its topic and number. Defining the same pair again is a no-op;
// a conflicting definition (the id for another partition, or the partition under another id) is
// an error.
func (s *logState) define(pid uint32, p Partition) error {
	if old, ok := s.byPID[pid]; ok {
		if old != p {
			return fmt.Errorf("%w: partition id %d is %v, redefined as %v", errLogInvalid, pid, old, p)
		}
		return nil
	}
	if other, ok := s.byPart[p]; ok {
		return fmt.Errorf("%w: %v already has id %d, redefined as %d", errLogInvalid, p, other, pid)
	}
	s.byPID[pid], s.byPart[p] = p, pid
	s.parts[pid] = &partState{}
	if pid >= s.nextPID {
		s.nextPID = pid + 1
	}
	return nil
}

// ensure returns the partition's id, allocating the next one if it has none. created tells the
// writer to emit a topic-define frame before the partition's first record.
func (s *logState) ensure(p Partition) (pid uint32, created bool, err error) {
	if pid, ok := s.byPart[p]; ok {
		return pid, false, nil
	}
	if s.nextPID == math.MaxUint32 {
		return 0, false, errors.New("log state: partition ids exhausted")
	}
	pid = s.nextPID
	return pid, true, s.define(pid, p)
}

func (s *logState) applyRecord(pid uint32, offset uint64) error {
	ps := s.parts[pid]
	if ps == nil {
		return fmt.Errorf("%w: id %d", errLogUndefinedPartition, pid)
	}
	if offset != uint64(ps.next) {
		return fmt.Errorf("%w: partition id %d expects %d, got %d", errLogOffset, pid, ps.next, offset)
	}
	if ps.next == math.MaxInt64 {
		return fmt.Errorf("%w: partition id %d is full", errLogOffset, pid)
	}
	ps.next++
	return nil
}

// applyCommit moves a group's position. As in Node.index: the commit names the offset it
// expects the group to be at (the explicit Expected, else Next-1), that must be the group's
// current position, and Next must be past it and no further than the end of the partition.
func (s *logState) applyCommit(c commit3) error {
	ps := s.parts[c.Partition]
	if ps == nil {
		return fmt.Errorf("%w: id %d", errLogUndefinedPartition, c.Partition)
	}
	if c.Next == 0 && !c.HasExpected {
		return fmt.Errorf("%w: next offset 0 without an expected offset", errLogConflict)
	}
	expected := c.Next - 1
	if c.HasExpected {
		expected = c.Expected
	}
	name := string(c.Group)
	if expected != uint64(ps.groups[name].Next) || c.Next <= expected || c.Next > uint64(ps.next) {
		return fmt.Errorf("%w: group %q at %d, commit expects %d and moves to %d (partition ends at %d)", errLogConflict, name, ps.groups[name].Next, expected, c.Next, ps.next)
	}
	if ps.groups == nil {
		ps.groups = map[string]Position{}
	}
	ps.groups[name] = Position{Next: int64(c.Next), State: cloneState(c.State)}
	return nil
}

// retain raises partitions' floors. Every floor is checked before any is applied: it must not
// go backwards or past the end, and no group may be behind it.
func (s *logState) retain(floors map[uint32]int64) error {
	for pid, floor := range floors {
		ps := s.parts[pid]
		if ps == nil {
			return fmt.Errorf("%w: id %d", errLogUndefinedPartition, pid)
		}
		if floor < ps.base || floor > ps.next {
			return fmt.Errorf("%w: floor %d outside [%d, %d] for partition id %d", errLogInvalid, floor, ps.base, ps.next, pid)
		}
		for g, pos := range ps.groups {
			if pos.Next < floor {
				return fmt.Errorf("%w: group %q is at %d, floor %d, partition id %d", errLogLagging, g, pos.Next, floor, pid)
			}
		}
	}
	for pid, floor := range floors {
		s.parts[pid].base = floor
	}
	return nil
}

// snapshot is the checkpoint of the current state. The order is fixed (partition ids ascending,
// then group names ascending), so equal states produce identical bytes.
func (s *logState) snapshot(id, segment uint64) checkpoint3 {
	cp := checkpoint3{ID: id, Segment: segment, Config: cloneState(s.config)}
	pids := make([]uint32, 0, len(s.byPID))
	for pid := range s.byPID {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	for _, pid := range pids {
		p, ps := s.byPID[pid], s.parts[pid]
		cp.Topics = append(cp.Topics, topicDef3{Partition: pid, Number: uint32(p.Number), Topic: []byte(p.Topic)})
		cp.Partitions = append(cp.Partitions, cpPartition3{Partition: pid, Base: uint64(ps.base), Next: uint64(ps.next)})
		names := make([]string, 0, len(ps.groups))
		for g := range ps.groups {
			names = append(names, g)
		}
		slices.Sort(names)
		for _, g := range names {
			pos := ps.groups[g]
			cp.Groups = append(cp.Groups, cpGroup3{Partition: pid, Group: []byte(g), Next: uint64(pos.Next), State: cloneState(pos.State)})
		}
	}
	return cp
}

// load builds the state a checkpoint describes, validating it as a whole: unique ids and
// partitions, one partition entry per defined id, groups only for defined ids and not past the
// partition's end, floors not past the end, offsets that fit an int64.
func (s *logState) load(cp checkpoint3) error {
	n := newLogState()
	n.config = cloneState(cp.Config)
	for _, t := range cp.Topics {
		if int(t.Number) < 0 {
			return fmt.Errorf("%w: partition number %d", errLogInvalid, t.Number)
		}
		if err := n.define(t.Partition, Partition{string(t.Topic), int(t.Number)}); err != nil {
			return err
		}
	}
	seen := map[uint32]bool{}
	for _, p := range cp.Partitions {
		ps := n.parts[p.Partition]
		if ps == nil || seen[p.Partition] {
			return fmt.Errorf("%w: partition entry for id %d is undefined or repeated", errLogInvalid, p.Partition)
		}
		if p.Base > p.Next || p.Next > math.MaxInt64 {
			return fmt.Errorf("%w: partition id %d has floor %d and next %d", errLogInvalid, p.Partition, p.Base, p.Next)
		}
		seen[p.Partition] = true
		ps.base, ps.next = int64(p.Base), int64(p.Next)
	}
	if len(seen) != len(n.byPID) {
		return fmt.Errorf("%w: %d partitions defined, %d described", errLogInvalid, len(n.byPID), len(seen))
	}
	for _, g := range cp.Groups {
		ps := n.parts[g.Partition]
		if ps == nil {
			return fmt.Errorf("%w: group for undefined partition id %d", errLogInvalid, g.Partition)
		}
		if g.Next > uint64(ps.next) {
			return fmt.Errorf("%w: group %q at %d, past the end %d of partition id %d", errLogInvalid, g.Group, g.Next, ps.next, g.Partition)
		}
		name := string(g.Group)
		if _, dup := ps.groups[name]; dup {
			return fmt.Errorf("%w: group %q repeated for partition id %d", errLogInvalid, name, g.Partition)
		}
		if ps.groups == nil {
			ps.groups = map[string]Position{}
		}
		ps.groups[name] = Position{Next: int64(g.Next), State: cloneState(g.State)}
	}
	*s = *n
	return nil
}

// equal reports whether two states are the same, and says what differs when they are not.
func (s *logState) equal(o *logState) error {
	if !bytes.Equal(s.config, o.config) {
		return errors.New("manifest differs")
	}
	if len(s.byPID) != len(o.byPID) {
		return fmt.Errorf("%d partitions against %d", len(s.byPID), len(o.byPID))
	}
	for pid, p := range s.byPID {
		if o.byPID[pid] != p {
			return fmt.Errorf("partition id %d is %v against %v", pid, p, o.byPID[pid])
		}
		a, b := s.parts[pid], o.parts[pid]
		if b == nil || a.base != b.base || a.next != b.next || len(a.groups) != len(b.groups) {
			return fmt.Errorf("partition id %d (%v) differs", pid, p)
		}
		for g, pos := range a.groups {
			if q, ok := b.groups[g]; !ok || q.Next != pos.Next || !bytes.Equal(q.State, pos.State) {
				return fmt.Errorf("group %q of partition id %d differs", g, pid)
			}
		}
	}
	return nil
}

// checkpointFollows reports whether o, the state a checkpoint describes, is what s becomes when
// the frames in between were only retention: the same manifest, partitions, next offsets and
// groups, with floors that are the same or higher. Retention is recorded only by a checkpoint, so
// a floor that moved is expected; anything else that differs is a disagreement.
func (s *logState) checkpointFollows(o *logState) error {
	if !bytes.Equal(s.config, o.config) {
		return errors.New("manifest differs")
	}
	if len(s.byPID) != len(o.byPID) {
		return fmt.Errorf("%d partitions against %d", len(s.byPID), len(o.byPID))
	}
	for pid, p := range s.byPID {
		if o.byPID[pid] != p {
			return fmt.Errorf("partition id %d is %v against %v", pid, p, o.byPID[pid])
		}
		a, b := s.parts[pid], o.parts[pid]
		if b == nil || a.next != b.next || b.base < a.base || len(a.groups) != len(b.groups) {
			return fmt.Errorf("partition id %d (%v): next %d, floor %d here; the checkpoint says next %d, floor %d", pid, p, a.next, a.base, b.next, b.base)
		}
		for g, pos := range a.groups {
			if q, ok := b.groups[g]; !ok || q.Next != pos.Next || !bytes.Equal(q.State, pos.State) {
				return fmt.Errorf("group %q of partition id %d differs", g, pid)
			}
		}
	}
	return nil
}

func (s *logState) String() string {
	var sb strings.Builder
	pids := make([]uint32, 0, len(s.byPID))
	for pid := range s.byPID {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	for _, pid := range pids {
		ps := s.parts[pid]
		fmt.Fprintf(&sb, "%d=%v[%d,%d) groups=%d; ", pid, s.byPID[pid], ps.base, ps.next, len(ps.groups))
	}
	return sb.String()
}
