// Package dostoevsky embeds durable, partitioned event streams in Go applications.
package dostoevsky

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"slices"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid  = errors.New("invalid argument")
	ErrOwner    = errors.New("partition belongs to another node")
	ErrLimit    = errors.New("size limit exceeded")
	ErrClosed   = errors.New("node closed")
	ErrConflict = errors.New("checkpoint or message ID conflict")
	ErrExpired  = errors.New("offset expired by retention")
	ErrEmpty    = errors.New("offset at end of log")
)

const MaxValue = 1 << 20

var name = regexp.MustCompile("^[a-zA-Z0-9_-]{1,96}$")

// Config fixes partition placement. Node identifies this process in the ordered Nodes list.
type Config struct {
	Node   string
	Nodes  []string
	Topics map[string]int
}
type Partition struct {
	Topic  string
	Number int
}
type Message struct {
	Key   string
	Value []byte
	ID    string
}
type Record struct {
	Partition
	Message
	Offset int64
	Time   time.Time
}

// Position holds the next offset and the most recently committed processor state.
type Position struct {
	Next  int64
	State []byte
}
type checkpoint struct {
	Expected *int64 `json:",omitempty"`
	Partition
	Group string
	Position
}
type stream struct {
	base    int64
	firstID string
	first   location
	tail    *recordTail
	groups  map[string]Position
	wait    *streamWait
	seeded  bool // a retention seed set base; further seeds for it may add groups
}

type recordTail struct {
	records []location
	packed  *locationIndex
	ids     map[string]int64
}

type streamWait struct {
	changed chan struct{}
	waiters int
}

func (s *stream) count() int64 {
	if s.first.size == 0 {
		return 0
	}
	if s.tail == nil {
		return 1
	}
	if s.tail.packed != nil {
		return 1 + s.tail.packed.count
	}
	return 1 + int64(len(s.tail.records))
}

func (s *stream) location(index int64) location {
	if index == 0 {
		return s.first
	}
	if s.tail.packed != nil {
		return s.tail.packed.location(index - 1)
	}
	return s.tail.records[index-1]
}

func (s *stream) addRecord(r *Record, loc location) { s.addRecordFields(r.ID, r.Offset, loc) }

func (s *stream) addRecordFields(id string, offset int64, loc location) {
	if s.first.size == 0 {
		s.first, s.firstID = loc, id
		return
	}
	if s.tail == nil {
		s.tail = &recordTail{}
	}
	t := s.tail
	if t.packed == nil && len(t.records) == locationStride {
		t.packed = new(locationIndex)
		for _, old := range t.records {
			t.packed.append(old)
		}
		t.records = nil
	}
	if t.packed != nil {
		t.packed.append(loc)
	} else {
		t.records = append(t.records, loc)
	}
	if id != "" {
		if s.tail.ids == nil {
			s.tail.ids = make(map[string]int64)
		}
		s.tail.ids[id] = offset
	}
}

func (s *stream) lookupID(id string) (int64, bool) {
	if id != "" && id == s.firstID {
		return s.base, true
	}
	if s.tail == nil {
		return 0, false
	}
	off, ok := s.tail.ids[id]
	return off, ok
}

func (s *stream) signal() {
	if s.wait != nil && s.wait.changed != nil {
		close(s.wait.changed)
		s.wait.changed = nil
	}
}

type Node struct {
	publishes publishQueue
	mu        sync.RWMutex
	j         *journal
	cfg       Config
	topology  string
	streams   map[Partition]*stream
	closed    bool
	// retired is the journal file Retain replaced. Its lock stays held, so a
	// process that opened the path before the rename cannot lock the orphaned
	// inode and acknowledge writes into it.
	retired *os.File
	// truncated is what Open cut off the end of the journal.
	truncated Truncation
	// maxBytes caps the journal; see SetJournalCap.
	maxBytes int64
}

// Truncation is an incomplete final frame that Open removed from the journal: an append that was interrupted, so was
// not acknowledged. The zero value means nothing was removed.
type Truncation struct {
	At    int64 // where the journal was cut
	Bytes int64 // how many bytes were removed
}

// Truncated reports what Open removed. Applications that want to know about interrupted writes should log it.
func (n *Node) Truncated() Truncation { return n.truncated }

func (c Config) validate() error {
	seen := map[string]bool{}
	for _, n := range c.Nodes {
		if !name.MatchString(n) || seen[n] {
			return ErrInvalid
		}
		seen[n] = true
	}
	if !seen[c.Node] || len(c.Topics) == 0 {
		return ErrInvalid
	}
	for t, n := range c.Topics {
		if !(name.MatchString(t) || t == "*" && len(c.Topics) == 1) || n < 1 || n > 65536 {
			return ErrInvalid
		}
	}
	return nil
}

// fastRecordReplay lets Open take Record frames from the scanner in recordscan.go.
// Tests turn it off to replay through encoding/json alone, as before, and compare.
var fastRecordReplay = true

// Open creates or recovers a node. A directory can have one open writer.
func Open(dir string, c Config) (*Node, error) {
	if e := c.validate(); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(c)
	if e != nil {
		return nil, e
	}
	var own Config
	if e = json.Unmarshal(raw, &own); e != nil {
		return nil, e
	}
	manifest := own
	manifest.Node = ""
	b, _ := json.Marshal(manifest)
	j, e := openJournal(dir)
	if e != nil {
		return nil, e
	}
	n := &Node{cfg: own, j: j, streams: make(map[Partition]*stream), topology: fmt.Sprintf("%x", sha256.Sum256(b))}
	// Before recovery, which writes: a journal that needs an upgrade is not touched.
	if old, e := legacyOnly(j.file); e != nil || old {
		j.close()
		if e != nil {
			return nil, e
		}
		return nil, upgradeRequired(dir)
	}
	var declared int
	var record func(Partition, int64, string, location) error
	if fastRecordReplay {
		record = func(p Partition, offset int64, id string, loc location) error {
			if declared == 0 {
				return errors.New("missing manifest")
			}
			return n.indexRecord(p, offset, id, loc)
		}
	}
	e = j.recover(record, func(ev entry, loc location) error {
		if ev.Config != nil {
			saved := ev.Config
			if saved.Node != own.Node || !slices.Equal(saved.Nodes, own.Nodes) {
				return errors.New("data directory node layout mismatch")
			}
			for topic, count := range saved.Topics {
				if own.Topics[topic] != count {
					return fmt.Errorf("topic %s removed or resized", topic)
				}
			}
			declared = len(saved.Topics)
			return nil
		}
		if declared == 0 {
			return errors.New("missing manifest")
		}
		return n.index(ev, loc)
	})
	if e == nil && declared != len(own.Topics) {
		_, e = j.append(entry{Config: &own})
	}
	if e == nil && !j.fenced {
		// A 0.2.x journal: mark it so a 0.2.x binary refuses the directory from now on.
		_, e = j.append(entry{Fence: 1})
	}
	if e != nil {
		j.close()
		return nil, e
	}
	n.truncated = j.torn
	return n, nil
}

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	for _, s := range n.streams {
		s.signal()
	}
	err := n.j.close()
	if n.retired != nil {
		err = errors.Join(err, n.retired.Close())
		n.retired = nil
	}
	return err
}
func (n *Node) owner(p Partition) (string, error) {
	count := n.partitionCount(p.Topic)
	if p.Number < 0 || p.Number >= count {
		return "", ErrInvalid
	}
	if len(n.cfg.Nodes) == 1 {
		return n.cfg.Nodes[0], nil
	}
	slot := uint64(p.Number)
	if n.cfg.Topics["*"] > 0 {
		h := fnv.New32a()
		h.Write([]byte(p.Topic))
		slot += uint64(h.Sum32())
	}
	return n.cfg.Nodes[slot%uint64(len(n.cfg.Nodes))], nil
}
func (n *Node) partitionCount(t string) int {
	if count := n.cfg.Topics["*"]; count > 0 {
		if name.MatchString(t) {
			return count
		}
		return 0
	}
	return n.cfg.Topics[t]
}
func (n *Node) partition(t, key string) (Partition, error) {
	c := n.partitionCount(t)
	if c == 0 {
		return Partition{}, ErrInvalid
	}
	if c == 1 {
		return Partition{t, 0}, nil
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return Partition{t, int(h.Sum32() % uint32(c))}, nil
}
func (n *Node) stream(p Partition) (*stream, error) {
	if n.closed {
		return nil, ErrClosed
	}
	owner, e := n.owner(p)
	if e != nil {
		return nil, e
	}
	if owner != n.cfg.Node {
		return nil, ErrOwner
	}
	s := n.streams[p]
	if s == nil {
		s = &stream{}
		n.streams[p] = s
	}
	return s, nil
}

// indexRecord replays one Record frame from the fields replay needs.
func (n *Node) indexRecord(p Partition, offset int64, id string, loc location) error {
	s, err := n.stream(p)
	if err != nil {
		return err
	}
	if offset != s.base+s.count() {
		return ErrConflict
	}
	s.addRecordFields(id, offset, loc)
	s.signal()
	return nil
}
func (n *Node) index(e entry, loc location) error {
	if seed := e.Seed; seed != nil {
		s, err := n.stream(seed.Partition)
		if err != nil {
			return err
		}
		if seed.Base < 0 || s.count() != 0 {
			return ErrConflict
		}
		if s.seeded {
			if s.base != seed.Base {
				return ErrConflict
			}
		} else if s.base != 0 || len(s.groups) != 0 {
			return ErrConflict
		}
		// A partition's groups may arrive in several seed frames of the same base.
		for g, pos := range seed.Groups {
			if _, dup := s.groups[g]; dup {
				return ErrConflict
			}
			if s.groups == nil {
				s.groups = make(map[string]Position)
			}
			s.groups[g] = pos
		}
		s.base = seed.Base
		s.seeded = true
		return nil
	}
	if r := e.Record; r != nil {
		return n.indexRecord(r.Partition, r.Offset, r.ID, loc)
	}
	if c := e.Check; c != nil {
		s, err := n.stream(c.Partition)
		if err != nil {
			return err
		}
		expected := c.Next - 1
		if c.Expected != nil {
			expected = *c.Expected
		}
		if expected != s.groups[c.Group].Next || c.Next <= expected || c.Next > s.base+s.count() {
			return ErrConflict
		}
		if s.groups == nil {
			s.groups = make(map[string]Position)
		}
		s.groups[c.Group] = Position{c.Next, bytes.Clone(c.State)}
		return nil
	}
	if e.Fence != 0 {
		return nil
	}
	return errors.New("unknown journal entry")
}

// Publish routes by key. An optional ID makes retries idempotent within a partition.
func (n *Node) Publish(ctx context.Context, t string, m Message) (Record, error) {
	p, e := n.partition(t, m.Key)
	if e != nil {
		return Record{}, e
	}
	if len(m.Value) > MaxValue || len(m.ID) > 256 || len(m.Key) > 4096 {
		return Record{}, ErrLimit
	}
	if !validText(m.ID, m.Key) {
		return Record{}, ErrInvalid
	}
	return n.schedulePublish(ctx, p, m)
}

// publishLocked writes one record under the node lock.
func (n *Node) publishLocked(ctx context.Context, p Partition, m Message) (Record, error) {
	if e := ctx.Err(); e != nil {
		return Record{}, e
	}
	s, e := n.stream(p)
	if e != nil {
		return Record{}, e
	}
	if off, ok := s.lookupID(m.ID); m.ID != "" && ok {
		ev, e := n.j.read(s.location(off - s.base))
		if e != nil {
			return Record{}, e
		}
		r := ev.Record
		if r.Key != m.Key || !bytes.Equal(r.Value, m.Value) {
			return Record{}, ErrConflict
		}
		return *r, nil
	}
	r := Record{Partition: p, Message: m, Offset: s.base + s.count(), Time: time.Now().UTC()}
	ev := entry{Record: &r}
	loc, e := n.j.appendWithin(ev, n.maxBytes)
	if e == nil {
		s.addRecord(&r, loc)
		s.signal()
	} else if s.count() == 0 && s.wait == nil && s.base == 0 && len(s.groups) == 0 {
		// The lookup above may have created this entry. A refused write keeps no memory: otherwise publishing to ever new
		// topics while the journal is full would grow the index without bound.
		delete(n.streams, p)
	}
	return r, e
}

// Read returns one record. With wait=true it sleeps until the offset exists or ctx ends.
func (n *Node) Read(ctx context.Context, p Partition, offset int64, wait bool) (Record, error) {
	if offset < 0 {
		return Record{}, ErrInvalid
	}
	for {
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		n.mu.RLock()
		if n.closed {
			n.mu.RUnlock()
			return Record{}, ErrClosed
		}
		owner, e := n.owner(p)
		if e != nil || owner != n.cfg.Node {
			n.mu.RUnlock()
			if e != nil {
				return Record{}, e
			}
			return Record{}, ErrOwner
		}
		s := n.streams[p]
		if s != nil && offset < s.base {
			n.mu.RUnlock()
			return Record{}, ErrExpired
		}
		if s != nil && offset < s.base+s.count() {
			ev, e := n.j.read(s.location(offset - s.base))
			n.mu.RUnlock()
			if e != nil {
				return Record{}, e
			}
			return *ev.Record, nil
		}
		n.mu.RUnlock()
		if !wait {
			return Record{}, ErrEmpty
		}
		n.mu.Lock()
		s, e = n.stream(p)
		if e != nil {
			n.mu.Unlock()
			return Record{}, e
		}
		if offset < s.base+s.count() {
			n.mu.Unlock()
			continue
		}
		if s.wait == nil {
			s.wait = &streamWait{}
		}
		if s.wait.changed == nil {
			s.wait.changed = make(chan struct{})
		}
		ch := s.wait.changed
		s.wait.waiters++
		n.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-ch:
		}
		n.mu.Lock()
		s.wait.waiters--
		if s.wait.waiters == 0 {
			if s.count() == 0 && s.base == 0 && len(s.groups) == 0 {
				delete(n.streams, p)
			}
			s.wait = nil
		}
		n.mu.Unlock()
	}
}
func (n *Node) Position(ctx context.Context, p Partition, g string) (Position, error) {
	if !name.MatchString(g) {
		return Position{}, ErrInvalid
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	s, e := n.stream(p)
	if e != nil {
		return Position{}, e
	}
	if s.count() == 0 && s.wait == nil && s.base == 0 && len(s.groups) == 0 {
		delete(n.streams, p)
	}
	pos := s.groups[g]
	pos.State = bytes.Clone(pos.State)
	return pos, ctx.Err()
}

// Commit advances exactly one offset and atomically replaces this group's partition-local state.
func (n *Node) Commit(ctx context.Context, p Partition, g string, offset int64, state []byte) error {
	return n.CommitBatch(ctx, p, g, offset, offset+1, state)
}
func (n *Node) CommitBatch(ctx context.Context, p Partition, g string, offset, next int64, state []byte) error {
	if !name.MatchString(g) || offset < 0 || next <= offset {
		return ErrInvalid
	}
	if len(state) > MaxValue {
		return ErrLimit
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	s, e := n.stream(p)
	if e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if s.groups[g].Next != offset || next > s.base+s.count() {
		if s.count() == 0 && s.wait == nil && s.base == 0 && len(s.groups) == 0 {
			delete(n.streams, p)
		}
		return ErrConflict
	}
	ev := entry{Check: &checkpoint{Partition: p, Group: g, Position: Position{next, state}}}
	if next != offset+1 {
		ev.Check.Expected = &offset
	}
	loc, e := n.j.appendWithin(ev, n.commitCeiling())
	if e != nil {
		return e
	}
	return n.index(ev, loc)
}
func validText(ss ...string) bool {
	for _, s := range ss {
		if !utf8.ValidString(s) {
			return false
		}
	}
	return true
}
