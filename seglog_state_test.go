package dostoevsky

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
)

// ----- helpers -----

func sampleState(t testing.TB) *logState {
	t.Helper()
	st := newLogState()
	st.setConfig([]byte(`{"Node":"a","Nodes":["a"],"Topics":{"words":2,"é":1}}`))
	for _, p := range []Partition{{"words", 0}, {"words", 1}, {"é", 0}} {
		pid, created, err := st.ensure(p)
		if err != nil || !created {
			t.Fatalf("ensure %v: %d %v %v", p, pid, created, err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := st.applyRecord(0, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := st.applyRecord(2, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []commit3{
		{Partition: 0, Group: []byte("billing"), Next: 3, HasExpected: true, Expected: 0, State: []byte("state-é")},
		{Partition: 0, Group: []byte("audit"), Next: 1},
		{Partition: 2, Group: []byte("g"), Next: 1, State: nil},
	} {
		if err := st.applyCommit(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.retain(map[uint32]int64{0: 1}); err != nil {
		t.Fatal(err)
	}
	return st
}

func cpEqual(t testing.TB, a, b checkpoint3) {
	t.Helper()
	if a.ID != b.ID || a.Segment != b.Segment || !bytes.Equal(a.Config, b.Config) ||
		len(a.Topics) != len(b.Topics) || len(a.Partitions) != len(b.Partitions) || len(a.Groups) != len(b.Groups) {
		t.Fatalf("checkpoints differ in header or counts: %+v vs %+v", a, b)
	}
	for i := range a.Topics {
		x, y := a.Topics[i], b.Topics[i]
		if x.Partition != y.Partition || x.Number != y.Number || !bytes.Equal(x.Topic, y.Topic) {
			t.Fatalf("topic %d differs", i)
		}
	}
	for i := range a.Partitions {
		if a.Partitions[i] != b.Partitions[i] {
			t.Fatalf("partition %d differs", i)
		}
	}
	for i := range a.Groups {
		x, y := a.Groups[i], b.Groups[i]
		if x.Partition != y.Partition || !bytes.Equal(x.Group, y.Group) || x.Next != y.Next || !bytes.Equal(x.State, y.State) {
			t.Fatalf("group %d differs", i)
		}
	}
}

// frames splits a byte stream into (type, body, offset) frames, stopping at the first frame that
// does not read cleanly; the offset of the end of the readable data is returned.
type frameRef struct {
	typ  byte
	body []byte
	at   int
}

func splitFrames(b []byte) (frames []frameRef, end int) {
	for end < len(b) {
		typ, body, n, err := readFrame3(b[end:])
		if err != nil {
			return
		}
		frames = append(frames, frameRef{typ, body, end})
		end += n
	}
	return
}

func replayAll(b []byte, st *logState) (*replayer3, error) {
	r := newReplayer3(st)
	frames, _ := splitFrames(b)
	for _, f := range frames {
		if err := r.Frame(f.typ, f.body); err != nil {
			return r, err
		}
	}
	return r, nil
}

// ----- the checkpoint codec -----

func TestCheckpoint3RoundTrip(t *testing.T) {
	st := sampleState(t)
	cp := st.snapshot(7, 3)
	b, err := appendCheckpoint3([]byte("prefix"), cp)
	if err != nil {
		t.Fatal(err)
	}
	b = b[len("prefix"):]
	got, size, err := readCheckpoint3(append(append([]byte(nil), b...), 9, 9, 9))
	if err != nil || size != len(b) {
		t.Fatalf("size %d/%d: %v", size, len(b), err)
	}
	cpEqual(t, cp, got)
	loaded := newLogState()
	if err = loaded.load(got); err != nil {
		t.Fatal(err)
	}
	if err = st.equal(loaded); err != nil {
		t.Fatalf("the loaded state differs: %v", err)
	}
	// An empty state is a valid checkpoint too.
	empty, err := appendCheckpoint3(nil, newLogState().snapshot(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = readCheckpoint3(empty); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpoint3IsDeterministic(t *testing.T) {
	build := func(order []Partition, groups []string) []byte {
		st := newLogState()
		st.setConfig([]byte("{}"))
		for _, p := range order {
			if _, _, err := st.ensure(p); err != nil {
				t.Fatal(err)
			}
		}
		pid := st.byPart[Partition{"a", 0}]
		for i := 0; i < 4; i++ {
			if err := st.applyRecord(pid, uint64(i)); err != nil {
				t.Fatal(err)
			}
		}
		for _, g := range groups {
			if err := st.applyCommit(commit3{Partition: pid, Group: []byte(g), Next: 2, HasExpected: true, Expected: 0, State: []byte(g)}); err != nil {
				t.Fatal(err)
			}
		}
		b, err := appendCheckpoint3(nil, st.snapshot(1, 1))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// the same partition ids and groups, defined and committed in different orders
	a := build([]Partition{{"a", 0}, {"b", 0}}, []string{"x", "y", "z"})
	b := build([]Partition{{"a", 0}, {"b", 0}}, []string{"z", "x", "y"})
	if !bytes.Equal(a, b) {
		t.Fatal("equal states produced different checkpoint bytes")
	}
}

func TestCheckpoint3LargeGroupsSplitIntoFrames(t *testing.T) {
	st := newLogState()
	st.setConfig([]byte("{}"))
	pid, _, _ := st.ensure(Partition{"t", 0})
	for i := 0; i < 5; i++ {
		_ = st.applyRecord(pid, uint64(i))
	}
	big := bytes.Repeat([]byte{7}, MaxValue)
	for i := 0; i < 5; i++ {
		if err := st.applyCommit(commit3{Partition: pid, Group: []byte(fmt.Sprintf("g%d", i)), Next: 1, State: big}); err != nil {
			t.Fatal(err)
		}
	}
	b, err := appendCheckpoint3(nil, st.snapshot(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 5*MaxValue {
		t.Fatalf("checkpoint of five 1 MiB group states is only %d bytes", len(b))
	}
	frames, _ := splitFrames(b)
	for _, f := range frames {
		if f.typ == f3CpGroup && len(f.body) > frame3Max {
			t.Fatalf("a group frame of %d bytes exceeds the frame limit", len(f.body))
		}
	}
	got, _, err := readCheckpoint3(b)
	if err != nil {
		t.Fatal(err)
	}
	loaded := newLogState()
	if err = loaded.load(got); err != nil || st.equal(loaded) != nil {
		t.Fatalf("large groups did not round-trip: %v", err)
	}
	// A group state that cannot fit a frame is refused instead of written torn.
	cp := st.snapshot(1, 1)
	cp.Groups[0].State = make([]byte, frame3Max)
	if out, err := appendCheckpoint3([]byte("keep"), cp); !errors.Is(err, ErrLimit) || string(out) != "keep" {
		t.Fatalf("oversized group state: %v, %q", err, out)
	}
}

func TestCheckpoint3TornAtEveryLengthIsIgnoredByReplay(t *testing.T) {
	b, err := appendCheckpoint3(nil, sampleState(t).snapshot(2, 1))
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(b); cut++ {
		if _, _, err := readCheckpoint3(b[:cut]); !errors.Is(err, errCheckpointTorn) {
			t.Fatalf("cut %d of %d: %v", cut, len(b), err)
		}
		st := newLogState()
		r, err := replayAll(b[:cut], st)
		if err != nil {
			t.Fatalf("cut %d: replay of a torn checkpoint failed: %v", cut, err)
		}
		if !st.empty() || r.Checkpoints != 0 {
			t.Fatalf("cut %d: a torn checkpoint changed the state", cut)
		}
		frames, _ := splitFrames(b[:cut])
		if want := len(frames) > 0; r.InCheckpoint() != want {
			t.Fatalf("cut %d: InCheckpoint %v, want %v", cut, r.InCheckpoint(), want)
		}
	}
}

func TestCheckpoint3EveryDamagedByteIsDetected(t *testing.T) {
	b, err := appendCheckpoint3(nil, sampleState(t).snapshot(2, 1))
	if err != nil {
		t.Fatal(err)
	}
	for i := range b {
		bad := append([]byte(nil), b...)
		bad[i] ^= 0x10
		if _, _, err := readCheckpoint3(bad); err == nil {
			t.Fatalf("damage at byte %d of %d was not detected", i, len(b))
		}
	}
}

func TestCheckpoint3StructureIsEnforced(t *testing.T) {
	frame := func(typ byte, body []byte) []byte {
		b, err := appendFrame3(nil, typ, body)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	u32 := func(v ...uint32) []byte {
		var b []byte
		for _, x := range v {
			b = append(b, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
		}
		return b
	}
	u64 := func(v uint64) []byte { return append(u32(uint32(v)), u32(uint32(v>>32))...) }
	begin := func(id uint64, topics, parts, groups uint32) []byte {
		return frame(f3CpBegin, append(append(u64(id), u64(1)...), u32(topics, parts, groups)...))
	}
	end := func(id uint64, topics, parts, groups uint32) []byte {
		return frame(f3CpEnd, append(u64(id), u32(topics, parts, groups)...))
	}
	cfg := frame(f3Config, []byte("{}"))
	td := func() []byte {
		d, _ := appendTopicDef3(nil, topicDef3{Partition: 0, Number: 0, Topic: []byte("t")})
		return d
	}()
	part := frame(f3CpPartition, append(append(u32(0), u64(0)...), u64(0)...))
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	tdN := func(pid uint32) []byte {
		d, _ := appendTopicDef3(nil, topicDef3{Partition: pid, Number: pid, Topic: []byte("t")})
		return d
	}
	partN := func(pid uint32) []byte { return frame(f3CpPartition, append(append(u32(pid), u64(0)...), u64(0)...)) }
	grp := func(name string) []byte {
		return frame(f3CpGroup, append(append(append(u32(0), byte(len(name)), 0), name...), u64(0)...))
	}

	if _, _, err := readCheckpoint3(cat(begin(1, 1, 1, 0), cfg, td, part, end(1, 1, 1, 0))); err != nil {
		t.Fatalf("a well-formed checkpoint was refused: %v", err)
	}
	for name, b := range map[string][]byte{
		"end with another id":       cat(begin(1, 1, 1, 0), cfg, td, part, end(2, 1, 1, 0)),
		"end with other counts":     cat(begin(1, 1, 1, 0), cfg, td, part, end(1, 1, 2, 0)),
		"no config":                 cat(begin(1, 1, 1, 0), td, part, end(1, 1, 1, 0)),
		"partition before topic":    cat(begin(1, 1, 1, 0), cfg, part, td, end(1, 1, 1, 0)),
		"missing partition":         cat(begin(1, 1, 1, 0), cfg, td, end(1, 1, 1, 0)),
		"extra frame before end":    cat(begin(1, 1, 1, 0), cfg, td, part, part, end(1, 1, 1, 0)),
		"record inside":             cat(begin(1, 1, 1, 0), cfg, td, mustRecord3(t, record3{}), end(1, 1, 1, 0)),
		"nested begin":              cat(begin(1, 1, 1, 0), cfg, begin(2, 0, 0, 0), end(1, 1, 1, 0)),
		"unknown type":              cat(begin(1, 1, 1, 0), cfg, frame(42, nil), end(1, 1, 1, 0)),
		"short begin":               frame(f3CpBegin, []byte{1, 2, 3}),
		"starts with config":        cat(cfg, end(1, 0, 0, 0)),
		"short partition entry":     cat(begin(1, 1, 1, 0), cfg, td, frame(f3CpPartition, []byte{1}), end(1, 1, 1, 0)),
		"topics out of order":       cat(begin(1, 2, 2, 0), cfg, tdN(1), tdN(0), partN(0), partN(1), end(1, 2, 2, 0)),
		"topics repeated":           cat(begin(1, 2, 2, 0), cfg, tdN(0), tdN(0), partN(0), partN(1), end(1, 2, 2, 0)),
		"partitions out of order":   cat(begin(1, 2, 2, 0), cfg, tdN(0), tdN(1), partN(1), partN(0), end(1, 2, 2, 0)),
		"groups out of order":       cat(begin(1, 1, 1, 2), cfg, td, part, grp("b"), grp("a"), end(1, 1, 1, 2)),
		"groups repeated":           cat(begin(1, 1, 1, 2), cfg, td, part, grp("a"), grp("a"), end(1, 1, 1, 2)),
		"group name past the entry": cat(begin(1, 1, 1, 1), cfg, td, part, frame(f3CpGroup, append(u32(0), 200, 0, 1, 2)), end(1, 1, 1, 1)),
	} {
		if _, _, err := readCheckpoint3(b); !errors.Is(err, errCheckpointInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// counts that promise more than the data holds look torn, and cannot allocate anything large
	if _, _, err := readCheckpoint3(cat(begin(1, 4_000_000_000, 4_000_000_000, 4_000_000_000), cfg)); !errors.Is(err, errCheckpointTorn) {
		t.Errorf("huge counts: %v", err)
	}
}

// ----- the state model -----

func TestLogStateSemantics(t *testing.T) {
	st := newLogState()
	if err := st.applyRecord(0, 0); !errors.Is(err, errLogUndefinedPartition) {
		t.Fatalf("record for an undefined partition: %v", err)
	}
	pid, created, err := st.ensure(Partition{"t", 0})
	if err != nil || pid != 0 || !created {
		t.Fatal(pid, created, err)
	}
	if again, created, _ := st.ensure(Partition{"t", 0}); again != 0 || created {
		t.Fatal("ensure is not idempotent")
	}
	if other, _, _ := st.ensure(Partition{"t", 1}); other != 1 {
		t.Fatalf("second partition got id %d", other)
	}
	if err = st.define(0, Partition{"x", 0}); !errors.Is(err, errLogInvalid) {
		t.Fatalf("redefining an id: %v", err)
	}
	if err = st.define(9, Partition{"t", 0}); !errors.Is(err, errLogInvalid) {
		t.Fatalf("a second id for a partition: %v", err)
	}
	if err = st.define(0, Partition{"t", 0}); err != nil {
		t.Fatalf("redefining identically: %v", err)
	}
	for _, off := range []uint64{1, 5} {
		if err = st.applyRecord(0, off); !errors.Is(err, errLogOffset) {
			t.Fatalf("offset %d on an empty partition: %v", off, err)
		}
	}
	for i := uint64(0); i < 4; i++ {
		if err = st.applyRecord(0, i); err != nil {
			t.Fatal(err)
		}
	}
	if err = st.applyRecord(0, 3); !errors.Is(err, errLogOffset) {
		t.Fatalf("a repeated offset: %v", err)
	}

	// commits: the group starts at 0; Next-1 is the expected offset unless one is given
	ok := commit3{Partition: 0, Group: []byte("g"), Next: 1}
	if err = st.applyCommit(ok); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]commit3{
		"stale expected":       {Partition: 0, Group: []byte("g"), Next: 2, HasExpected: true, Expected: 0},
		"implicit but stale":   {Partition: 0, Group: []byte("g"), Next: 1},
		"past the end":         {Partition: 0, Group: []byte("g"), Next: 9, HasExpected: true, Expected: 1},
		"not past expected":    {Partition: 0, Group: []byte("g"), Next: 1, HasExpected: true, Expected: 1},
		"next zero":            {Partition: 0, Group: []byte("g"), Next: 0},
		"new group, wrong pos": {Partition: 0, Group: []byte("h"), Next: 3},
		"undefined partition":  {Partition: 7, Group: []byte("g"), Next: 1},
	} {
		if err := st.applyCommit(c); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if err = st.applyCommit(commit3{Partition: 0, Group: []byte("g"), Next: 4, HasExpected: true, Expected: 1, State: []byte("s")}); err != nil {
		t.Fatalf("a batch commit to the end: %v", err)
	}
	if pos := st.parts[0].groups["g"]; pos.Next != 4 || string(pos.State) != "s" {
		t.Fatalf("%+v", pos)
	}

	// retention: floors are validated as a whole before any is applied
	st.parts[1].next = 2
	if err = st.retain(map[uint32]int64{1: 1, 0: 9}); !errors.Is(err, errLogInvalid) || st.parts[1].base != 0 {
		t.Fatalf("a bad floor must leave every floor alone: %v, base %d", err, st.parts[1].base)
	}
	if err = st.retain(map[uint32]int64{0: 5}); !errors.Is(err, errLogInvalid) {
		t.Fatalf("a floor past the end: %v", err)
	}
	if err = st.retain(map[uint32]int64{0: 3}); err != nil {
		t.Fatal(err)
	}
	if err = st.retain(map[uint32]int64{0: 2}); !errors.Is(err, errLogInvalid) {
		t.Fatalf("a floor going backwards: %v", err)
	}
	_ = st.applyCommit(commit3{Partition: 1, Group: []byte("slow"), Next: 1})
	if err = st.retain(map[uint32]int64{1: 2}); !errors.Is(err, errLogLagging) {
		t.Fatalf("a floor past a group: %v", err)
	}
}

func TestLogStateLoadRefusesInconsistentCheckpoints(t *testing.T) {
	good := sampleState(t).snapshot(1, 1)
	mutate := func(f func(*checkpoint3)) checkpoint3 {
		cp := sampleState(t).snapshot(1, 1)
		f(&cp)
		return cp
	}
	if err := newLogState().load(good); err != nil {
		t.Fatal(err)
	}
	for name, cp := range map[string]checkpoint3{
		"duplicate id":        mutate(func(c *checkpoint3) { c.Topics[1].Partition = c.Topics[0].Partition }),
		"duplicate partition": mutate(func(c *checkpoint3) { c.Topics[1].Topic, c.Topics[1].Number = c.Topics[0].Topic, c.Topics[0].Number }),
		"partition undefined": mutate(func(c *checkpoint3) { c.Partitions[0].Partition = 99 }),
		"partition repeated":  mutate(func(c *checkpoint3) { c.Partitions[1].Partition = c.Partitions[0].Partition }),
		"partition missing":   mutate(func(c *checkpoint3) { c.Partitions = c.Partitions[:1] }),
		"floor past the end":  mutate(func(c *checkpoint3) { c.Partitions[0].Base = c.Partitions[0].Next + 1 }),
		"next too large":      mutate(func(c *checkpoint3) { c.Partitions[0].Next = 1 << 63 }),
		"group undefined":     mutate(func(c *checkpoint3) { c.Groups[0].Partition = 99 }),
		"group past the end":  mutate(func(c *checkpoint3) { c.Groups[0].Next = c.Partitions[0].Next + 1 }),
		"group repeated":      mutate(func(c *checkpoint3) { c.Groups = append(c.Groups, c.Groups[0]) }),
	} {
		dst := newLogState()
		if err := dst.load(cp); err == nil {
			t.Errorf("%s was accepted", name)
		} else if !dst.empty() {
			t.Errorf("%s: a refused checkpoint changed the state", name)
		}
	}
}

// ----- replay: the property that makes reopen independent of history -----

// randomStream builds a valid frame stream of random operations with checkpoints interleaved and
// returns it with the final state and the byte offset of every checkpoint.
func randomStream(t testing.TB, seed int64, ops int) (stream []byte, final *logState, cps []int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	st := newLogState()
	st.setConfig([]byte(`{"Node":"a"}`))
	var segID, cpID uint64 = 1, 0
	emitCheckpoint := func() {
		cpID++
		cps = append(cps, len(stream))
		var err error
		if stream, err = appendCheckpoint3(stream, st.snapshot(cpID, segID)); err != nil {
			t.Fatal(err)
		}
	}
	emitCheckpoint()
	topics := []Partition{{"a", 0}, {"a", 1}, {"b", 0}, {"é", 0}}
	groups := []string{"g1", "g2", "g3"}
	for i := 0; i < ops; i++ {
		switch op := rng.Intn(20); {
		case op < 12: // a record, defining its partition first when it is new
			p := topics[rng.Intn(len(topics))]
			pid, created, err := st.ensure(p)
			if err != nil {
				t.Fatal(err)
			}
			if created {
				var d []byte
				if d, err = appendTopicDef3(nil, topicDef3{Partition: pid, Number: uint32(p.Number), Topic: []byte(p.Topic)}); err != nil {
					t.Fatal(err)
				}
				stream = append(stream, d...)
			}
			off := uint64(st.parts[pid].next)
			if err = st.applyRecord(pid, off); err != nil {
				t.Fatal(err)
			}
			if stream, err = appendRecord3(stream, record3{Partition: pid, Offset: off, Time: int64(i), Value: bytes.Repeat([]byte{byte(i)}, rng.Intn(20))}); err != nil {
				t.Fatal(err)
			}
		case op < 17: // a commit, when one is possible
			var pids []uint32
			for _, pid := range sortedPIDs(st) {
				if st.parts[pid].next > 0 {
					pids = append(pids, pid)
				}
			}
			if len(pids) == 0 {
				continue
			}
			pid := pids[rng.Intn(len(pids))]
			g := groups[rng.Intn(len(groups))]
			from, end := uint64(st.parts[pid].groups[g].Next), uint64(st.parts[pid].next)
			if from >= end {
				continue
			}
			next := from + 1 + uint64(rng.Int63n(int64(end-from)))
			c := commit3{Partition: pid, Group: []byte(g), Next: next, State: bytes.Repeat([]byte{byte(next)}, rng.Intn(10))}
			if next != from+1 {
				c.HasExpected, c.Expected = true, from
			}
			if err := st.applyCommit(c); err != nil {
				t.Fatal(err)
			}
			var err error
			if stream, err = appendCommit3(stream, c); err != nil {
				t.Fatal(err)
			}
		case op < 19: // retention, made durable by a checkpoint
			floors := map[uint32]int64{}
			for _, pid := range sortedPIDs(st) {
				ps := st.parts[pid]
				limit := ps.next
				for _, pos := range ps.groups {
					limit = min(limit, pos.Next)
				}
				if limit > ps.base && rng.Intn(2) == 0 {
					floors[pid] = ps.base + 1 + rng.Int63n(limit-ps.base)
				}
			}
			if len(floors) > 0 {
				if err := st.retain(floors); err != nil {
					t.Fatal(err)
				}
				emitCheckpoint()
			}
		default:
			if rng.Intn(3) == 0 {
				segID++
			}
			emitCheckpoint()
		}
	}
	return stream, st, cps
}

func sortedPIDs(st *logState) []uint32 {
	pids := make([]uint32, 0, len(st.parts))
	for pid := range st.parts {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	return pids
}

func TestReplayFromAnyCheckpointEqualsReplayOfEverything(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		stream, final, cps := randomStream(t, seed, 400)
		full := newLogState()
		r, err := replayAll(stream, full)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if err = full.equal(final); err != nil {
			t.Fatalf("seed %d: full replay differs from the model: %v", seed, err)
		}
		if r.Checkpoints != len(cps) || r.InCheckpoint() {
			t.Fatalf("seed %d: %d checkpoints replayed, %d written", seed, r.Checkpoints, len(cps))
		}
		for _, at := range cps {
			fromHere := newLogState()
			if _, err = replayAll(stream[at:], fromHere); err != nil {
				t.Fatalf("seed %d: replay from the checkpoint at %d: %v", seed, at, err)
			}
			if err = fromHere.equal(final); err != nil {
				t.Fatalf("seed %d: replay from the checkpoint at %d differs from replay of everything: %v", seed, at, err)
			}
		}
	}
}

func TestReplayCrashAtEveryByteOfTheTailKeepsTheCheckpointedState(t *testing.T) {
	stream, _, cps := randomStream(t, 99, 120)
	last := cps[len(cps)-1]
	// state as of the end of the last checkpoint, then every prefix of what follows
	st := newLogState()
	if _, err := replayAll(stream[:last], st); err != nil {
		t.Fatal(err)
	}
	_, size, err := readCheckpoint3(stream[last:])
	if err != nil {
		t.Fatal(err)
	}
	for cut := last; cut < last+size; cut++ { // a crash inside the last checkpoint
		from := newLogState()
		if _, err := replayAll(stream[:cut], from); err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if err := from.equal(st); err != nil {
			t.Fatalf("cut %d: a torn final checkpoint changed the state: %v", cut, err)
		}
	}
}

func TestReplayRefusesACheckpointThatDisagreesWithTheFramesBeforeIt(t *testing.T) {
	stream, _, cps := randomStream(t, 5, 150)
	if len(cps) < 3 {
		t.Skip("the random stream wrote too few checkpoints")
	}
	at := cps[len(cps)-1]
	cp, size, err := readCheckpoint3(stream[at:])
	if err != nil {
		t.Fatal(err)
	}
	cp.Partitions[0].Next++ // claims one more record than the frames before it produced
	forged, err := appendCheckpoint3(nil, cp)
	if err != nil {
		t.Fatal(err)
	}
	bad := append(append(append([]byte(nil), stream[:at]...), forged...), stream[at+size:]...)
	if _, err = replayAll(bad, newLogState()); !errors.Is(err, errCheckpointInvalid) || !strings.Contains(err.Error(), "disagrees") {
		t.Fatalf("a forged checkpoint was accepted: %v", err)
	}
}

func TestReplayRefusesFramesThatDoNotFollowTheState(t *testing.T) {
	st := newLogState()
	r := newReplayer3(st)
	td, _ := appendTopicDef3(nil, topicDef3{Partition: 0, Number: 0, Topic: []byte("t")})
	_, body, _, _ := readFrame3(td)
	if err := r.Frame(f3TopicDef, body); err != nil {
		t.Fatal(err)
	}
	skip := mustRecord3(t, record3{Partition: 0, Offset: 5})
	_, body, _, _ = readFrame3(skip)
	if err := r.Frame(f3Record, body); !errors.Is(err, errLogOffset) {
		t.Fatalf("a record that skips offsets: %v", err)
	}
	if err := r.Frame(f3Config, nil); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("a config frame outside a checkpoint: %v", err)
	}
	if err := r.Frame(99, nil); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("an unknown frame type: %v", err)
	}
	var seen []uint64
	r.OnRecord = func(rec record3) error { seen = append(seen, rec.Offset); return nil }
	for off := uint64(0); off < 3; off++ {
		_, body, _, _ := readFrame3(mustRecord3(t, record3{Partition: 0, Offset: off}))
		if err := r.Frame(f3Record, body); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 3 || seen[2] != 2 {
		t.Fatalf("OnRecord saw %v", seen)
	}
	fence := mustFrame3(t, f3Fence, nil)
	_, body, _, _ = readFrame3(fence)
	if err := r.Frame(f3Fence, body); err != nil || r.Fences != 1 {
		t.Fatalf("fence: %v %d", err, r.Fences)
	}
}

func TestReplayRefusesACheckpointFromAnotherSegment(t *testing.T) {
	cp, err := appendCheckpoint3(nil, sampleState(t).snapshot(1, 7))
	if err != nil {
		t.Fatal(err)
	}
	seven, eight := uint64(7), uint64(8)
	r := newReplayer3(newLogState())
	r.WantSegment = &seven
	frames, _ := splitFrames(cp)
	for _, f := range frames {
		if err := r.Frame(f.typ, f.body); err != nil {
			t.Fatalf("a checkpoint of the right segment was refused: %v", err)
		}
	}
	r = newReplayer3(newLogState())
	r.WantSegment = &eight
	var got error
	for _, f := range frames {
		if got = r.Frame(f.typ, f.body); got != nil {
			break
		}
	}
	if !errors.Is(got, errCheckpointInvalid) || !strings.Contains(got.Error(), "segment") || !r.st.empty() {
		t.Fatalf("a checkpoint from another segment was adopted: %v", got)
	}
}

// cx PR 32 M1: a checkpoint may raise a floor only where Retain could have: no group behind it.
func TestReplayRefusesARaisedFloorPastAGroup(t *testing.T) {
	st := newLogState()
	st.setConfig([]byte("{}"))
	pid, _, _ := st.ensure(Partition{"t", 0})
	for i := uint64(0); i < 10; i++ {
		if err := st.applyRecord(pid, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.applyCommit(commit3{Partition: pid, Group: []byte("slow"), Next: 3, HasExpected: true, Expected: 0}); err != nil {
		t.Fatal(err)
	}
	before, err := appendCheckpoint3(nil, st.snapshot(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	// a legitimate raise (to the group's position) is accepted ...
	ok := cloneLog(t, st)
	if err = ok.retain(map[uint32]int64{pid: 3}); err != nil {
		t.Fatal(err)
	}
	okCP, _ := appendCheckpoint3(nil, ok.snapshot(2, 1))
	good := newLogState()
	if _, err = replayAll(append(bytes.Clone(before), okCP...), good); err != nil || good.equal(ok) != nil {
		t.Fatalf("a floor raised to a group's position was refused: %v", err)
	}
	// ... one past the group is not, and the state before it stays intact
	cp := st.snapshot(2, 1)
	cp.Partitions[0].Base = 5
	forged, _ := appendCheckpoint3(nil, cp)
	have := newLogState()
	_, err = replayAll(append(bytes.Clone(before), forged...), have)
	if !errors.Is(err, errCheckpointInvalid) || !errors.Is(err, errLogLagging) {
		t.Fatalf("a floor raised past a group was accepted: %v", err)
	}
	only := newLogState()
	if _, err = replayAll(before, only); err != nil || have.equal(only) != nil {
		t.Fatalf("a refused checkpoint changed the state: %v", err)
	}
	// a group created behind an already established floor does not make the next checkpoint invalid
	late := cloneLog(t, ok) // floor 3 established
	if err = late.applyCommit(commit3{Partition: pid, Group: []byte("late"), Next: 1, HasExpected: true, Expected: 0}); err != nil {
		t.Fatal(err)
	}
	cpLate, _ := appendCheckpoint3(nil, late.snapshot(3, 1))
	commit, _ := appendCommit3(nil, commit3{Partition: pid, Group: []byte("late"), Next: 1, HasExpected: true, Expected: 0})
	chain := append(append(append(bytes.Clone(before), okCP...), commit...), cpLate...)
	if _, err = replayAll(chain, newLogState()); err != nil {
		t.Fatalf("a group behind an established floor made a later checkpoint invalid: %v", err)
	}
}

// cx PR 32 L1: the wire format declares an empty fence.
func TestReplayRefusesAFenceWithABody(t *testing.T) {
	r := newReplayer3(newLogState())
	if err := r.Frame(f3Fence, nil); err != nil || r.Fences != 1 {
		t.Fatalf("an empty fence: %v %d", err, r.Fences)
	}
	if err := r.Frame(f3Fence, []byte{1}); !errors.Is(err, errFrame3Invalid) || r.Fences != 1 {
		t.Fatalf("a fence with a body: %v %d", err, r.Fences)
	}
}
