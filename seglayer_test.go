package dostoevsky

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
)

// ----- a model file system that crashes at any operation and loses what was not synced -----

var errCrash = errors.New("simulated crash")

type memNode struct{ vol, dur []byte }

// memFS keeps two namespaces and two copies of every file's data: what the running process sees
// (ns, vol) and what a crash is guaranteed to leave behind (dns, dur). A file's data becomes
// durable on Sync or SyncData; a name becomes durable on SyncDir of its directory. Every
// operation that changes or syncs something counts as one step; step failAt "crashes": it does
// not happen (or, for a write with partial set, half of it does) and every later step fails.
type memFS struct {
	mu      sync.Mutex
	ns, dns map[string]*memNode
	ops     int
	failAt  int
	partial bool
	dead    bool
}

func newMemFS() *memFS {
	return &memFS{ns: map[string]*memNode{}, dns: map[string]*memNode{}}
}

// tick counts a step; it returns whether this very step is the crash.
func (m *memFS) tick() (crashNow bool, err error) {
	m.ops++
	if m.dead {
		return false, errCrash
	}
	if m.failAt > 0 && m.ops == m.failAt {
		m.dead = true
		return true, errCrash
	}
	return false, nil
}

type memFile struct {
	fs   *memFS
	node *memNode
}

func (m *memFS) OpenFile(path string, flag int, perm os.FileMode) (fsFile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.ns[path]
	if ok && flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		return nil, os.ErrExist
	}
	if !ok {
		if flag&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		if _, err := m.tick(); err != nil {
			return nil, err
		}
		n = &memNode{}
		m.ns[path] = n
	}
	return &memFile{m, n}, nil
}

func (m *memFS) Rename(a, b string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.tick(); err != nil {
		return err
	}
	n, ok := m.ns[a]
	if !ok {
		return os.ErrNotExist
	}
	delete(m.ns, a)
	m.ns[b] = n
	return nil
}

func (m *memFS) Remove(p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.tick(); err != nil {
		return err
	}
	if _, ok := m.ns[p]; !ok {
		return os.ErrNotExist
	}
	delete(m.ns, p)
	return nil
}

func (m *memFS) ReadDir(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for p := range m.ns {
		if filepath.Dir(p) == dir {
			names = append(names, filepath.Base(p))
		}
	}
	sort.Strings(names)
	return names, nil
}

func (m *memFS) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.tick(); err != nil {
		return err
	}
	for p := range m.dns {
		if filepath.Dir(p) == dir {
			delete(m.dns, p)
		}
	}
	for p, n := range m.ns {
		if filepath.Dir(p) == dir {
			m.dns[p] = n
		}
	}
	return nil
}

func (m *memFS) MkdirAll(string, os.FileMode) error { return nil }

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	crashNow, err := f.fs.tick()
	write := p
	if err != nil {
		if !(crashNow && f.fs.partial) {
			return 0, err
		}
		write = p[:len(p)/2] // a torn write: half of it reached the volatile data
	}
	if need := int(off) + len(write); need > len(f.node.vol) {
		f.node.vol = append(f.node.vol, make([]byte, need-len(f.node.vol))...)
	}
	copy(f.node.vol[off:], write)
	if err != nil {
		return len(write), err
	}
	return len(p), nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if off >= int64(len(f.node.vol)) {
		return 0, io.EOF
	}
	n := copy(p, f.node.vol[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if _, err := f.fs.tick(); err != nil {
		return err
	}
	f.node.dur = bytes.Clone(f.node.vol)
	return nil
}
func (f *memFile) Sync() error     { return f.sync() }
func (f *memFile) SyncData() error { return f.sync() }

func (f *memFile) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if _, err := f.fs.tick(); err != nil {
		return err
	}
	if int(size) <= len(f.node.vol) {
		f.node.vol = f.node.vol[:size]
	} else {
		f.node.vol = append(f.node.vol, make([]byte, int(size)-len(f.node.vol))...)
	}
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	return int64(len(f.node.vol)), nil
}

func (f *memFile) Close() error { return nil }

// image is what the disk holds after a crash: the namespace as last made durable (volNames
// false) or as the process left it (true), and for each surviving file its durable data, its
// volatile data, or the durable data plus half of what was written after it was last synced.
func (m *memFS) image(volNames bool, data string) *memFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.dns
	if volNames {
		src = m.ns
	}
	out := newMemFS()
	for p, n := range src {
		var d []byte
		switch data {
		case "durable":
			d = n.dur
		case "volatile":
			d = n.vol
		case "torn":
			d = n.dur
			if len(n.vol) > len(n.dur) && bytes.HasPrefix(n.vol, n.dur) {
				d = n.vol[:len(n.dur)+(len(n.vol)-len(n.dur))/2]
			}
		}
		node := &memNode{vol: bytes.Clone(d), dur: bytes.Clone(d)}
		out.ns[p], out.dns[p] = node, node
	}
	return out
}

// snapshot is a copy of the volatile namespace and data, to check that a refused Open changed nothing.
func (m *memFS) snapshot() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]string{}
	for p, n := range m.ns {
		out[p] = string(n.vol)
	}
	return out
}

// ----- scenarios -----

const testDir = "/log"

func cloneLog(t testing.TB, st *logState) *logState {
	t.Helper()
	c := newLogState()
	if err := c.load(st.snapshot(1, 1)); err != nil {
		t.Fatal(err)
	}
	return c
}

type segStep struct {
	name string
	run  func(l *segLog) error
}

// buildScenario returns the steps and, for each, the state the log holds once it is acknowledged:
// states[i] is the state after step i. Steps: start, three records, roll, two records, a
// retention checkpoint, roll, a record, a group commit.
func buildScenario(t testing.TB) (steps []segStep, states []*logState) {
	t.Helper()
	m := newLogState()
	m.setConfig([]byte(`{"Node":"a"}`))
	if _, _, err := m.ensure(Partition{"t", 0}); err != nil {
		t.Fatal(err)
	}
	var cpID uint64
	snap := func() checkpoint3 { cpID++; return m.snapshot(cpID, 0) }
	record := func(off uint64) []byte {
		b, err := appendRecord3(nil, record3{Partition: 0, Offset: off, Time: int64(off), Value: []byte(fmt.Sprintf("value-%d", off))})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	add := func(name string, apply func(), run func(l *segLog) error) {
		apply()
		steps = append(steps, segStep{name, run})
		states = append(states, cloneLog(t, m))
	}
	cp0 := snap()
	add("start", func() {}, func(l *segLog) error { return l.start(cp0) })
	for i := uint64(0); i < 3; i++ {
		off := i
		f := record(off)
		add(fmt.Sprintf("record %d", off), func() { _ = m.applyRecord(0, off) }, func(l *segLog) error { return l.append(f) })
	}
	var cp1 checkpoint3
	add("roll 1", func() { cp1 = snap() }, func(l *segLog) error { return l.roll(cp1) })
	for i := uint64(3); i < 5; i++ {
		off := i
		f := record(off)
		add(fmt.Sprintf("record %d", off), func() { _ = m.applyRecord(0, off) }, func(l *segLog) error { return l.append(f) })
	}
	var retention []byte
	add("retention", func() {
		if err := m.retain(map[uint32]int64{0: 2}); err != nil {
			t.Fatal(err)
		}
		var err error
		if retention, err = appendCheckpoint3(nil, m.snapshot(100, 2)); err != nil {
			t.Fatal(err)
		}
	}, func(l *segLog) error { return l.append(retention) })
	var cp2 checkpoint3
	add("roll 2", func() { cp2 = snap() }, func(l *segLog) error { return l.roll(cp2) })
	f5 := record(5)
	add("record 5", func() { _ = m.applyRecord(0, 5) }, func(l *segLog) error { return l.append(f5) })
	commit, err := appendCommit3(nil, commit3{Partition: 0, Group: []byte("g"), Next: 4, HasExpected: true, Expected: 0, State: []byte("s")})
	if err != nil {
		t.Fatal(err)
	}
	add("commit", func() {
		if err := m.applyCommit(commit3{Partition: 0, Group: []byte("g"), Next: 4, HasExpected: true, Expected: 0, State: []byte("s")}); err != nil {
			t.Fatal(err)
		}
	}, func(l *segLog) error { return l.append(commit) })
	return steps, states
}

// runSteps runs the steps on a fresh log in fs and returns how many were acknowledged.
func runSteps(t testing.TB, fs fsys, steps []segStep) (acked int, l *segLog) {
	t.Helper()
	l, err := openSegLog(fs, testDir)
	if err != nil {
		t.Fatalf("open of an empty directory: %v", err)
	}
	for _, s := range steps {
		if err := s.run(l); err != nil {
			return acked, l
		}
		acked++
	}
	return acked, l
}

func dirNames(t testing.TB, fs *memFS) []string {
	t.Helper()
	names, _ := fs.ReadDir(testDir)
	return names
}

// ----- tests -----

func TestSegNamesRoundTrip(t *testing.T) {
	for _, id := range []uint64{1, 2, 255, 1 << 40, 1<<64 - 1} {
		got, ok := parseSegName(segName(id))
		if !ok || got != id {
			t.Fatalf("%d: %d %v", id, got, ok)
		}
	}
	for _, bad := range []string{"seg-0000000000000000.log", "seg-1.log", "seg-000000000000000g.log", "seg-0000000000000001.tmp", "journal.v1", "seg-0000000000000001.logx", "SEG-0000000000000001.log"} {
		if _, ok := parseSegName(bad); ok {
			t.Errorf("%q parsed as a segment name", bad)
		}
	}
	if segTmpName(7) != "seg-0000000000000007.tmp" {
		t.Fatal(segTmpName(7))
	}
}

func TestSegLogWriteAndReopen(t *testing.T) {
	steps, states := buildScenario(t)
	fs := newMemFS()
	acked, l := runSteps(t, fs, steps)
	if acked != len(steps) {
		t.Fatalf("%d of %d steps acknowledged", acked, len(steps))
	}
	if got := dirNames(t, fs); len(got) != 3 || got[0] != segName(1) || got[2] != segName(3) {
		t.Fatalf("segments: %v", got)
	}
	l.close()
	// reopen from what a crash right now would leave: everything was synced, so even the strictest image has it all
	img := fs.image(false, "durable")
	l2, err := openSegLog(img, testDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = l2.state.equal(states[len(states)-1]); err != nil {
		t.Fatalf("recovered state differs: %v", err)
	}
	if l2.active.id != 3 || !slices.Equal(l2.ids, []uint64{1, 2}) {
		t.Fatalf("active %d sealed %v", l2.active.id, l2.ids)
	}
	// and the log is writable after reopen: the next record continues the offsets and survives another reopen
	next, _ := appendRecord3(nil, record3{Partition: 0, Offset: 6, Value: []byte("after")})
	if err = l2.append(next); err != nil {
		t.Fatal(err)
	}
	l3, err := openSegLog(img.image(false, "durable"), testDir)
	if err != nil || l3.state.parts[0].next != 7 {
		t.Fatalf("after another reopen: %v %v", err, l3.state)
	}
}

// The contract of the whole layer: crash at every step of the scenario, under every way a disk can
// lose the unsynced part, and Open must succeed, never report corruption, and recover either the
// last acknowledged state or the one that was in flight, with no staging file left behind and the
// log writable afterwards.
func TestSegLogCrashAtEveryOperation(t *testing.T) {
	steps, states := buildScenario(t)
	probe := newMemFS()
	runSteps(t, probe, steps)
	total := probe.ops
	if total < 20 {
		t.Fatalf("only %d file operations in the scenario", total)
	}
	cases := 0
	for failAt := 1; failAt <= total; failAt++ {
		for _, partial := range []bool{false, true} {
			for _, volNames := range []bool{false, true} {
				for _, data := range []string{"durable", "volatile", "torn"} {
					fs := newMemFS()
					fs.failAt, fs.partial = failAt, partial
					acked, _ := runSteps(t, fs, steps)
					img := fs.image(volNames, data)
					l, err := openSegLog(img, testDir)
					label := fmt.Sprintf("crash at op %d/%d (acked %d, partial %v, names volatile %v, data %s)", failAt, total, acked, partial, volNames, data)
					if err != nil {
						t.Fatalf("%s: Open failed: %v", label, err)
					}
					for _, n := range dirNames(t, img) {
						if strings.HasSuffix(n, ".tmp") {
							t.Fatalf("%s: a staging file survived Open: %s", label, n)
						}
					}
					// the recovered state is the last acknowledged one or the one in flight
					var candidates []*logState
					if acked > 0 {
						candidates = append(candidates, states[acked-1])
					} else {
						candidates = append(candidates, nil)
					}
					if acked < len(states) {
						candidates = append(candidates, states[acked])
					}
					ok := false
					for _, c := range candidates {
						switch {
						case c == nil && l.active == nil:
							ok = true
						case c != nil && l.active != nil && l.state.equal(c) == nil:
							ok = true
						}
					}
					if !ok {
						t.Fatalf("%s: recovered state %v is neither the acknowledged one nor the one in flight", label, l.state)
					}
					if l.active != nil {
						next, _ := appendRecord3(nil, record3{Partition: 0, Offset: uint64(l.state.parts[0].next), Value: []byte("more")})
						if err = l.append(next); err != nil {
							t.Fatalf("%s: the recovered log is not writable: %v", label, err)
						}
						again, err := openSegLog(img.image(false, "durable"), testDir)
						if err != nil || again.state.parts[0].next != l.state.parts[0].next+1 {
							t.Fatalf("%s: a record written after recovery was lost: %v", label, err)
						}
					}
					cases++
				}
			}
		}
	}
	t.Logf("%d crash scenarios over %d operations", cases, total)
}

func TestSegLogFailedRollFencesTheLog(t *testing.T) {
	steps, _ := buildScenario(t)
	// find the operation that is the directory sync of the first roll: the last step before roll 1 completes
	var syncDirOp int
	probe := newMemFS()
	l, _ := openSegLog(probe, testDir)
	for _, s := range steps {
		if err := s.run(l); err != nil {
			t.Fatal(err)
		}
		if s.name == "roll 1" {
			syncDirOp = probe.ops // the last file operation of the roll is the directory sync
			break
		}
	}
	fs := newMemFS()
	fs.failAt = syncDirOp
	acked, l := runSteps(t, fs, steps)
	if acked != 4 { // start, three records; the roll was not acknowledged
		t.Fatalf("acked %d", acked)
	}
	if !errors.Is(l.failed, errSegUncertain) && !errors.Is(l.failed, errCrash) {
		t.Fatalf("a failed roll did not fence the log: %v", l.failed)
	}
	rec, _ := appendRecord3(nil, record3{Partition: 0, Offset: 3})
	if err := l.append(rec); err == nil {
		t.Fatal("a fenced log accepted a write")
	}
	if err := l.roll(checkpoint3{}); err == nil {
		t.Fatal("a fenced log accepted a roll")
	}
}

// A torn append at every length of the last frame is truncated at the frame's start, durably.
func TestSegLogTornTailIsTruncatedAtEveryLength(t *testing.T) {
	steps, states := buildScenario(t)
	// run all but the last step, then append the last frame partially at every length
	fs := newMemFS()
	acked, l := runSteps(t, fs, steps[:len(steps)-1])
	if acked != len(steps)-1 {
		t.Fatal(acked)
	}
	l.close()
	before := l.active.end
	commit, _ := appendCommit3(nil, commit3{Partition: 0, Group: []byte("g"), Next: 4, HasExpected: true, Expected: 0, State: []byte("s")})
	for cut := 0; cut < len(commit); cut++ {
		img := fs.image(false, "durable")
		top := filepath.Join(testDir, segName(3))
		node := img.ns[top]
		node.vol = append(bytes.Clone(node.vol), commit[:cut]...)
		node.dur = bytes.Clone(node.vol)
		got, err := openSegLog(img, testDir)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if err = got.state.equal(states[len(states)-2]); err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if got.active.end != before || int64(len(img.ns[top].vol)) != before {
			t.Fatalf("cut %d: the torn tail was not truncated: end %d, file %d, want %d", cut, got.active.end, len(img.ns[top].vol), before)
		}
		if string(img.ns[top].dur) != string(img.ns[top].vol) {
			t.Fatalf("cut %d: the truncation was not made durable", cut)
		}
	}
}

// An incomplete retention checkpoint at the end is truncated at its first frame, whatever frame it is cut in.
func TestSegLogTornRetentionCheckpointIsTruncatedAtItsBegin(t *testing.T) {
	steps, states := buildScenario(t)
	// run through "record 4": the state before the retention step
	retIdx := -1
	for i, s := range steps {
		if s.name == "retention" {
			retIdx = i
		}
	}
	fs := newMemFS()
	acked, l := runSteps(t, fs, steps[:retIdx])
	if acked != retIdx {
		t.Fatal(acked)
	}
	l.close()
	before := l.active.end
	m := cloneLog(t, states[retIdx-1])
	if err := m.retain(map[uint32]int64{0: 2}); err != nil {
		t.Fatal(err)
	}
	group, err := appendCheckpoint3(nil, m.snapshot(100, 2))
	if err != nil {
		t.Fatal(err)
	}
	frames, _ := splitFrames(group)
	for cut := 1; cut < len(group); cut++ {
		img := fs.image(false, "durable")
		top := filepath.Join(testDir, segName(2))
		node := img.ns[top]
		node.vol = append(bytes.Clone(node.vol), group[:cut]...)
		node.dur = bytes.Clone(node.vol)
		got, err := openSegLog(img, testDir)
		if err != nil {
			t.Fatalf("cut %d of %d (frames at %v): %v", cut, len(group), frames[0].at, err)
		}
		if err = got.state.equal(states[retIdx-1]); err != nil {
			t.Fatalf("cut %d: a torn retention checkpoint changed the state: %v", cut, err)
		}
		if got.active.end != before {
			t.Fatalf("cut %d: truncated to %d, want the checkpoint's first frame at %d", cut, got.active.end, before)
		}
	}
	// the complete group is adopted
	img := fs.image(false, "durable")
	node := img.ns[filepath.Join(testDir, segName(2))]
	node.vol = append(bytes.Clone(node.vol), group...)
	node.dur = bytes.Clone(node.vol)
	got, err := openSegLog(img, testDir)
	if err != nil || got.state.equal(states[retIdx]) != nil {
		t.Fatalf("a complete retention checkpoint was not adopted: %v", err)
	}
}

func TestSegLogCorruptionIsAnErrorAndChangesNothing(t *testing.T) {
	steps, _ := buildScenario(t)
	build := func() *memFS {
		fs := newMemFS()
		acked, l := runSteps(t, fs, steps)
		if acked != len(steps) {
			t.Fatal(acked)
		}
		l.close()
		return fs.image(false, "durable")
	}
	top := filepath.Join(testDir, segName(3))
	mid := filepath.Join(testDir, segName(2))
	flip := func(path string, at int) func(fs *memFS) {
		return func(fs *memFS) {
			n := fs.ns[path]
			n.vol[at] ^= 0x40
			n.dur = bytes.Clone(n.vol)
		}
	}
	cases := map[string]func(fs *memFS){
		"header signature":      flip(top, 0),
		"header check":          flip(top, 13),
		"start checkpoint byte": flip(top, segHeaderSize+20),
		"interior frame byte":   func(fs *memFS) { n := fs.ns[top]; flip(top, len(n.vol)-5)(fs) },
		"first frame is not a begin": func(fs *memFS) {
			rec, _ := appendRecord3(nil, record3{Partition: 0, Offset: 0})
			n := fs.ns[top]
			n.vol = append(bytes.Clone(n.vol[:segHeaderSize]), rec...)
			n.dur = bytes.Clone(n.vol)
		},
		"header only, no checkpoint": func(fs *memFS) {
			n := fs.ns[top]
			n.vol = bytes.Clone(n.vol[:segHeaderSize])
			n.dur = bytes.Clone(n.vol)
		},
		"shorter than the header":       func(fs *memFS) { n := fs.ns[top]; n.vol = n.vol[:10]; n.dur = bytes.Clone(n.vol) },
		"empty segment":                 func(fs *memFS) { n := fs.ns[top]; n.vol = nil; n.dur = nil },
		"missing middle segment":        func(fs *memFS) { delete(fs.ns, mid); delete(fs.dns, mid) },
		"checkpoint of another segment": func(fs *memFS) { n := fs.ns[top]; n.vol = bytes.Clone(fs.ns[mid].vol); n.dur = bytes.Clone(n.vol) },
	}
	for name, damage := range cases {
		fs := build()
		damage(fs)
		before := fs.snapshot()
		_, err := openSegLog(fs, testDir)
		if !errors.Is(err, ErrCorruptLog) {
			t.Errorf("%s: %v", name, err)
			continue
		}
		after := fs.snapshot()
		if len(before) != len(after) {
			t.Errorf("%s: Open changed the set of files", name)
		}
		for p, d := range before {
			if after[p] != d {
				t.Errorf("%s: Open changed %s", name, filepath.Base(p))
			}
		}
	}
	// a leftover staging file is removed and is not corruption
	fs := build()
	fs.ns[filepath.Join(testDir, segTmpName(4))] = &memNode{vol: []byte("half a segment")}
	if _, err := openSegLog(fs, testDir); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(dirNames(t, fs), func(n string) bool { return strings.HasSuffix(n, ".tmp") }) {
		t.Fatal("the staging file was not removed")
	}
}

// The window refills correctly whatever its size: frames larger than the window, a window of one
// byte, and the default give the same state and the same record positions.
func TestSegLogScanIsIndependentOfTheReadWindow(t *testing.T) {
	m := newLogState()
	m.setConfig([]byte("{}"))
	_, _, _ = m.ensure(Partition{"t", 0})
	fs := newMemFS()
	l, err := openSegLog(fs, testDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.start(m.snapshot(1, 1)); err != nil {
		t.Fatal(err)
	}
	rng := []int{0, 1, 100, 5000, 70000, 3, 400000, 20}
	for i, size := range rng {
		f, _ := appendRecord3(nil, record3{Partition: 0, Offset: uint64(i), Value: bytes.Repeat([]byte{byte(i)}, size)})
		if err = l.append(f); err != nil {
			t.Fatal(err)
		}
	}
	l.close()
	old := replayChunk
	t.Cleanup(func() { replayChunk = old })
	type pos struct {
		at   int64
		size int
	}
	var ref []pos
	for _, chunk := range []int{1 << 20, 1, 7, 100, 4096, 100000} {
		replayChunk = chunk
		var got []pos
		seg, st, err := scanSegment(fs.image(false, "durable"), testDir, 1, func(rec record3, at int64, size int) error {
			got = append(got, pos{at, size})
			return nil
		})
		if err != nil {
			t.Fatalf("window %d: %v", chunk, err)
		}
		if st.parts[0].next != int64(len(rng)) || seg.end != int64(len(fs.image(false, "durable").ns[filepath.Join(testDir, segName(1))].vol)) {
			t.Fatalf("window %d: next %d end %d", chunk, st.parts[0].next, seg.end)
		}
		if ref == nil {
			ref = got
		} else if !slices.Equal(ref, got) {
			t.Fatalf("window %d: record positions differ", chunk)
		}
	}
	if len(ref) != len(rng) {
		t.Fatalf("%d records seen", len(ref))
	}
}

func TestSegLogRefusesToStartTwiceAndNeedsAStart(t *testing.T) {
	fs := newMemFS()
	l, _ := openSegLog(fs, testDir)
	rec, _ := appendRecord3(nil, record3{})
	if err := l.append(rec); err == nil {
		t.Fatal("an unstarted log accepted a write")
	}
	if err := l.roll(checkpoint3{}); err == nil {
		t.Fatal("an unstarted log accepted a roll")
	}
	st := newLogState()
	st.setConfig([]byte("{}"))
	if err := l.start(st.snapshot(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := l.start(st.snapshot(2, 1)); err == nil {
		t.Fatal("a started log was started again")
	}
}

func TestOSFileSystemWorksEndToEnd(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "log")
	steps, states := buildScenario(t)
	l, err := openSegLog(osFS{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range steps {
		if err := s.run(l); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
	}
	l.close()
	l2, err := openSegLog(osFS{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.close()
	if err = l2.state.equal(states[len(states)-1]); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 3 {
		t.Fatalf("%d files on disk", len(entries))
	}
}
