package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Checkpoint frames of the segmented log (design v2, change (a)). A checkpoint is the whole
// durable state (logState) written as a sequence of frames:
//
//	cp-begin   id u64 | segment id u64 | topics u32 | partitions u32 | groups u32
//	config     the manifest (JSON of Config)
//	topic-define x topics     (frame type 4, as for records)
//	cp-partition x partitions partition id u32 | base u64 | next u64
//	cp-group x groups         partition id u32 | group (u16 length, bytes) | next u64 | state
//	cp-end     id u64 | topics u32 | partitions u32 | groups u32
//
// It is valid only if cp-end is present with the same id and counts as cp-begin and every
// frame between them checks out. Within it, partition ids, topic definitions and groups are in
// one canonical ascending order without repeats (the writer produces that order, the reader
// requires it). A crash in the middle of writing one leaves an incomplete checkpoint, which
// replay ignores. Nothing in the engine uses these frames yet.
const (
	f3CpBegin     byte = 3
	f3Config      byte = 6
	f3CpPartition byte = 7
	f3CpGroup     byte = 8
	f3CpEnd       byte = 9
)

var (
	// errCheckpointTorn: the data ended inside a checkpoint (the usual result of a crash).
	errCheckpointTorn = errors.New("checkpoint: incomplete")
	// errCheckpointInvalid: the frames are there but are not a valid checkpoint.
	errCheckpointInvalid = errors.New("checkpoint: invalid")
)

type cpPartition3 struct {
	Partition  uint32
	Base, Next uint64
}

type cpGroup3 struct {
	Partition uint32
	Group     []byte
	Next      uint64
	State     []byte
}

type checkpoint3 struct {
	ID, Segment uint64
	Config      []byte
	Topics      []topicDef3
	Partitions  []cpPartition3
	Groups      []cpGroup3
}

func cpCounts(b []byte, topics, partitions, groups int) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(topics))
	b = binary.LittleEndian.AppendUint32(b, uint32(partitions))
	return binary.LittleEndian.AppendUint32(b, uint32(groups))
}

// appendCheckpoint3 appends the frames of one checkpoint to dst. On an error dst is returned
// unchanged in length.
func appendCheckpoint3(dst []byte, cp checkpoint3) ([]byte, error) {
	if uint64(len(cp.Topics)) > math.MaxUint32 || uint64(len(cp.Partitions)) > math.MaxUint32 || uint64(len(cp.Groups)) > math.MaxUint32 {
		return dst, ErrLimit
	}
	start := len(dst)
	fail := func(err error) ([]byte, error) { return dst[:start], err }
	var err error
	body := binary.LittleEndian.AppendUint64(nil, cp.ID)
	body = binary.LittleEndian.AppendUint64(body, cp.Segment)
	body = cpCounts(body, len(cp.Topics), len(cp.Partitions), len(cp.Groups))
	if dst, err = appendFrame3(dst, f3CpBegin, body); err != nil {
		return fail(err)
	}
	if dst, err = appendFrame3(dst, f3Config, cp.Config); err != nil {
		return fail(err)
	}
	for _, t := range cp.Topics {
		if dst, err = appendTopicDef3(dst, t); err != nil {
			return fail(err)
		}
	}
	for _, p := range cp.Partitions {
		body = binary.LittleEndian.AppendUint32(body[:0], p.Partition)
		body = binary.LittleEndian.AppendUint64(body, p.Base)
		body = binary.LittleEndian.AppendUint64(body, p.Next)
		if dst, err = appendFrame3(dst, f3CpPartition, body); err != nil {
			return fail(err)
		}
	}
	for _, g := range cp.Groups {
		if len(g.Group) > math.MaxUint16 {
			return fail(ErrLimit)
		}
		body = binary.LittleEndian.AppendUint32(body[:0], g.Partition)
		body = binary.LittleEndian.AppendUint16(body, uint16(len(g.Group)))
		body = append(body, g.Group...)
		body = binary.LittleEndian.AppendUint64(body, g.Next)
		body = append(body, g.State...)
		if dst, err = appendFrame3(dst, f3CpGroup, body); err != nil {
			return fail(err)
		}
	}
	body = binary.LittleEndian.AppendUint64(body[:0], cp.ID)
	body = cpCounts(body, len(cp.Topics), len(cp.Partitions), len(cp.Groups))
	if dst, err = appendFrame3(dst, f3CpEnd, body); err != nil {
		return fail(err)
	}
	return dst, nil
}

// cpBuilder collects the frames of one checkpoint as they are read, enforcing the order
// begin, config, topic-defines, partitions, groups, end, with the counts cp-begin gave.
type cpBuilder struct {
	cp                           checkpoint3
	wantTopics, wantParts, wantG int
	sawConfig                    bool
}

func cpInvalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errCheckpointInvalid, fmt.Sprintf(format, a...))
}

func parseCpCounts(b []byte) (topics, partitions, groups int) {
	return int(binary.LittleEndian.Uint32(b)), int(binary.LittleEndian.Uint32(b[4:])), int(binary.LittleEndian.Uint32(b[8:]))
}

func beginCheckpoint3(body []byte) (*cpBuilder, error) {
	if len(body) != 8+8+12 {
		return nil, cpInvalid("cp-begin of %d bytes", len(body))
	}
	b := &cpBuilder{}
	b.cp.ID = binary.LittleEndian.Uint64(body)
	b.cp.Segment = binary.LittleEndian.Uint64(body[8:])
	b.wantTopics, b.wantParts, b.wantG = parseCpCounts(body[16:])
	return b, nil
}

// add takes the next frame of the checkpoint. done is true after a valid cp-end.
func (b *cpBuilder) add(typ byte, body []byte) (done bool, err error) {
	switch {
	case !b.sawConfig:
		if typ != f3Config {
			return false, cpInvalid("expected the config frame, got type %d", typ)
		}
		b.cp.Config, b.sawConfig = bytes.Clone(body), true
	case len(b.cp.Topics) < b.wantTopics:
		if typ != f3TopicDef {
			return false, cpInvalid("expected a topic definition, got type %d", typ)
		}
		d, err := parseTopicDef3(body)
		if err != nil {
			return false, cpInvalid("%v", err)
		}
		if n := len(b.cp.Topics); n > 0 && d.Partition <= b.cp.Topics[n-1].Partition {
			return false, cpInvalid("topic definitions are not in ascending partition id order (id %d after %d)", d.Partition, b.cp.Topics[n-1].Partition)
		}
		d.Topic = bytes.Clone(d.Topic)
		b.cp.Topics = append(b.cp.Topics, d)
	case len(b.cp.Partitions) < b.wantParts:
		if typ != f3CpPartition || len(body) != 4+8+8 {
			return false, cpInvalid("expected a partition entry of 20 bytes, got type %d of %d bytes", typ, len(body))
		}
		pe := cpPartition3{binary.LittleEndian.Uint32(body), binary.LittleEndian.Uint64(body[4:]), binary.LittleEndian.Uint64(body[12:])}
		if n := len(b.cp.Partitions); n > 0 && pe.Partition <= b.cp.Partitions[n-1].Partition {
			return false, cpInvalid("partition entries are not in ascending id order (id %d after %d)", pe.Partition, b.cp.Partitions[n-1].Partition)
		}
		b.cp.Partitions = append(b.cp.Partitions, pe)
	case len(b.cp.Groups) < b.wantG:
		if typ != f3CpGroup || len(body) < 4+2+8 {
			return false, cpInvalid("expected a group entry, got type %d of %d bytes", typ, len(body))
		}
		g := int(binary.LittleEndian.Uint16(body[4:]))
		if len(body) < 4+2+g+8 {
			return false, cpInvalid("group name of %d bytes exceeds the entry", g)
		}
		ge := cpGroup3{
			Partition: binary.LittleEndian.Uint32(body),
			Group:     bytes.Clone(body[6 : 6+g]),
			Next:      binary.LittleEndian.Uint64(body[6+g:]),
			State:     cloneState(body[6+g+8:]),
		}
		if n := len(b.cp.Groups); n > 0 {
			prev := b.cp.Groups[n-1]
			if ge.Partition < prev.Partition || (ge.Partition == prev.Partition && bytes.Compare(ge.Group, prev.Group) <= 0) {
				return false, cpInvalid("group entries are not in ascending (partition id, name) order, or repeat")
			}
		}
		b.cp.Groups = append(b.cp.Groups, ge)
	default:
		if typ != f3CpEnd || len(body) != 8+12 {
			return false, cpInvalid("expected cp-end, got type %d of %d bytes", typ, len(body))
		}
		t, p, g := parseCpCounts(body[8:])
		if binary.LittleEndian.Uint64(body) != b.cp.ID || t != b.wantTopics || p != b.wantParts || g != b.wantG {
			return false, cpInvalid("cp-end does not match cp-begin")
		}
		return true, nil
	}
	return false, nil
}

// readCheckpoint3 reads one checkpoint from the start of b and returns it with the number of
// bytes it occupies. errCheckpointTorn means b ended inside it; errCheckpointInvalid means it
// is not a valid checkpoint (a checksum failure counts as invalid: the caller decides whether
// damage at the very end of a file is a torn write).
func readCheckpoint3(b []byte) (cp checkpoint3, size int, err error) {
	typ, body, n, err := readFrame3(b)
	if err != nil {
		return cp, 0, cpFrameError(err)
	}
	if typ != f3CpBegin {
		return cp, 0, cpInvalid("starts with type %d, not cp-begin", typ)
	}
	build, err := beginCheckpoint3(body)
	if err != nil {
		return cp, 0, err
	}
	size = n
	for {
		typ, body, n, err = readFrame3(b[size:])
		if err != nil {
			return checkpoint3{}, 0, cpFrameError(err)
		}
		size += n
		done, err := build.add(typ, body)
		if err != nil {
			return checkpoint3{}, 0, err
		}
		if done {
			return build.cp, size, nil
		}
	}
}

func cpFrameError(err error) error {
	if errors.Is(err, errFrame3Short) {
		return fmt.Errorf("%w: %v", errCheckpointTorn, err)
	}
	return fmt.Errorf("%w: %v", errCheckpointInvalid, err)
}

// ----- replay -----

// replayer3 applies a stream of frames to a logState, one frame at a time, the way Open will:
// records and commits advance the state, topic-defines extend the table, and a checkpoint
// (several frames) is verified and adopted when its cp-end arrives. A checkpoint adopted into an
// empty state is the starting point; one that arrives into a non-empty state must agree with it
// (same partitions, next offsets and groups; floors may have risen, because retention is recorded
// only by a checkpoint), so a checkpoint that disagrees with the frames before it is reported
// instead of trusted.
type replayer3 struct {
	st       *logState
	cp       *cpBuilder
	OnRecord func(record3) error // called after a record was applied to the state
	OnCommit func(commit3) error // called after a commit was applied to the state
	// WantSegment, when set, is the segment the frames come from: a checkpoint naming another
	// segment is refused (a checkpoint copied or restored from elsewhere must not be trusted).
	WantSegment *uint64
	Checkpoints int // complete checkpoints seen
	Fences      int
}

func newReplayer3(st *logState) *replayer3 { return &replayer3{st: st} }

// InCheckpoint is true between a cp-begin and its cp-end: if the data ends here the checkpoint
// is torn and has had no effect on the state.
func (r *replayer3) InCheckpoint() bool { return r.cp != nil }

func (r *replayer3) Frame(typ byte, body []byte) error {
	if r.cp != nil {
		done, err := r.cp.add(typ, body)
		if err != nil {
			return err
		}
		if !done {
			return nil
		}
		cp := r.cp.cp
		r.cp = nil
		if r.WantSegment != nil && cp.Segment != *r.WantSegment {
			return cpInvalid("checkpoint names segment %d inside segment %d", cp.Segment, *r.WantSegment)
		}
		r.Checkpoints++
		return r.adopt(cp)
	}
	switch typ {
	case f3CpBegin:
		b, err := beginCheckpoint3(body)
		if err != nil {
			return err
		}
		r.cp = b
	case f3Record:
		rec, err := parseRecord3(body)
		if err != nil {
			return err
		}
		if err = r.st.applyRecord(rec.Partition, rec.Offset); err != nil {
			return err
		}
		if r.OnRecord != nil {
			return r.OnRecord(rec)
		}
	case f3Commit:
		c, err := parseCommit3(body)
		if err != nil {
			return err
		}
		if err = r.st.applyCommit(c); err != nil {
			return err
		}
		if r.OnCommit != nil {
			return r.OnCommit(c)
		}
	case f3TopicDef:
		d, err := parseTopicDef3(body)
		if err != nil {
			return err
		}
		if int(d.Number) < 0 {
			return fmt.Errorf("%w: partition number %d", errLogInvalid, d.Number)
		}
		return r.st.define(d.Partition, Partition{string(d.Topic), int(d.Number)})
	case f3Fence:
		r.Fences++
	default:
		return fmt.Errorf("%w: frame type %d outside a checkpoint", errFrame3Invalid, typ)
	}
	return nil
}

func (r *replayer3) adopt(cp checkpoint3) error {
	other := newLogState()
	if err := other.load(cp); err != nil {
		return err
	}
	if !r.st.empty() {
		if err := r.st.checkpointFollows(other); err != nil {
			return fmt.Errorf("%w: a checkpoint disagrees with the frames before it: %v", errCheckpointInvalid, err)
		}
	}
	*r.st = *other
	return nil
}
