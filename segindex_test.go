package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"
)

func TestIDHashIsStable(t *testing.T) {
	// persisted in idx files: these values must never change
	for _, c := range []struct {
		pid  uint32
		id   string
		want uint64
	}{
		{0, "abc", 0x2e13aa12d98fddfb},
		{7, "", 0x6d3572669b2cde42},
		{1, "x", 0xd80d24aea7dc0494},
		{4294967295, "order-42", 0x6256698efa6259d8},
	} {
		if got := idHash(c.pid, []byte(c.id)); got != c.want {
			t.Errorf("idHash(%d, %q) = %#x, want %#x", c.pid, c.id, got, c.want)
		}
	}
	if idHash(1, []byte("a")) == idHash(2, []byte("a")) {
		t.Error("the partition id does not enter the hash")
	}
}

func TestIDTable(t *testing.T) {
	var tbl idTable
	tbl.lookup(5, func(uint64) bool { t.Fatal("an empty table returned an entry"); return true })
	rng := rand.New(rand.NewSource(1))
	want := map[uint64][]uint64{}
	for i := 0; i < 20000; i++ {
		h := uint64(rng.Intn(5000)) // many equal hashes: collisions coexist
		off := uint64(i)
		tbl.add(h, off)
		want[h] = append(want[h], off)
	}
	if tbl.len() != 20000 {
		t.Fatalf("len %d", tbl.len())
	}
	for h, offs := range want {
		var got []uint64
		tbl.lookup(h, func(off uint64) bool { got = append(got, off); return false })
		slices.Sort(got)
		if !slices.Equal(got, offs) {
			t.Fatalf("hash %d: %d entries, want %d", h, len(got), len(offs))
		}
	}
	var seen int
	tbl.each(func(h, off uint64) { seen++ })
	if seen != 20000 {
		t.Fatalf("each saw %d", seen)
	}
	// Exercise early stop on a hash guaranteed to have multiple candidates.
	tbl.add(0, 20000)
	tbl.add(0, 20001)
	var first int
	tbl.lookup(0, func(uint64) bool { first++; return true })
	if first != 1 {
		t.Fatalf("visit returning true must stop the lookup, saw %d", first)
	}
	var miss int
	tbl.lookup(999999999, func(uint64) bool { miss++; return false })
	if miss != 0 {
		t.Fatal("a hash that was never added matched")
	}
	// offset 0 is a real offset (the table stores offset+1)
	var z idTable
	z.add(42, 0)
	var got []uint64
	z.lookup(42, func(off uint64) bool { got = append(got, off); return false })
	if !slices.Equal(got, []uint64{0}) {
		t.Fatalf("offset 0: %v", got)
	}
}

func sampleIndex() (*segIndex, idxFooterInfo) {
	x := newSegIndex(map[uint32]uint64{0: 100, 1: 0, 5: 7})
	pos := int64(16 + 400)
	for i := uint64(0); i < 200; i++ {
		pid := uint32(i % 2)
		off := x.first(pid) + x.count(pid)
		var id []byte
		if i%3 == 0 {
			id = []byte(fmt.Sprintf("id-%d", i))
		}
		x.observe(record3{Partition: pid, Offset: off, Time: int64(1000 + i), ID: id}, pos)
		pos += 50
	}
	x.observe(record3{Partition: 9, Offset: 0, Time: 5000}, pos) // a partition defined inside the segment
	return x, idxFooterInfo{segment: 4, size: uint64(pos + 50), lastPos: uint64(pos), lastCRC: 0xdeadbeef}
}

func TestSparseIndexFindsEveryRecord(t *testing.T) {
	x := newSegIndex(map[uint32]uint64{3: 1000})
	posOf := map[uint64]int64{}
	pos := int64(500)
	for i := uint64(0); i < 1000; i++ {
		off := 1000 + i
		x.observe(record3{Partition: 3, Offset: off}, pos)
		posOf[off] = pos
		pos += 40
	}
	p := x.parts[3]
	if len(p.offs) > 1000/sparseStride+2 {
		t.Fatalf("%d sparse entries for 1000 records: not sparse", len(p.offs))
	}
	for off, want := range posOf {
		got, ok := x.find(3, off)
		if !ok || got > want {
			t.Fatalf("offset %d: position %d (want at or before %d), ok %v", off, got, want, ok)
		}
		// the walk from the entry reaches the record within one stride of this partition's records
		if (want-got)/40 >= sparseStride {
			t.Fatalf("offset %d: entry %d frames before the record", off, (want-got)/40)
		}
	}
	for _, off := range []uint64{0, 999, 2000, 1 << 40} {
		if _, ok := x.find(3, off); ok {
			t.Errorf("offset %d is outside the segment and was found", off)
		}
	}
	if _, ok := x.find(4, 1000); ok {
		t.Error("an unknown partition was found")
	}
}

func TestSegIndexFileRoundTripAndDamage(t *testing.T) {
	x, foot := sampleIndex()
	b := encodeSegIndex(x, foot)
	got, gotFoot, err := decodeSegIndex(b)
	if err != nil || gotFoot != foot || !got.sameAs(x, foot) {
		t.Fatalf("round trip: %v %+v", err, gotFoot)
	}
	if !bytes.Equal(b, encodeSegIndex(x, foot)) {
		t.Fatal("encoding is not deterministic")
	}
	// the decoded index answers like the original
	for pid, p := range x.parts {
		for off := p.first; off < p.first+p.count; off++ {
			a, ok1 := x.find(pid, off)
			c, ok2 := got.find(pid, off)
			if a != c || ok1 != ok2 {
				t.Fatalf("pid %d offset %d: %d/%v against %d/%v", pid, off, a, ok1, c, ok2)
			}
		}
	}
	h := idHash(0, []byte("id-0"))
	var hits int
	got.ids.lookup(h, func(uint64) bool { hits++; return false })
	if hits != 1 {
		t.Fatalf("the ID table lost its entries: %d", hits)
	}
	for i := range b { // every damaged byte is detected
		bad := bytes.Clone(b)
		bad[i] ^= 0x01
		if _, _, err := decodeSegIndex(bad); !errors.Is(err, errIdxInvalid) {
			t.Fatalf("damage at byte %d of %d: %v", i, len(b), err)
		}
	}
	for cut := 0; cut < len(b); cut++ { // every truncation is detected
		if _, _, err := decodeSegIndex(b[:cut]); !errors.Is(err, errIdxInvalid) {
			t.Fatalf("cut at %d: %v", cut, err)
		}
	}
	if _, _, err := decodeSegIndex(append(bytes.Clone(b), 0)); !errors.Is(err, errIdxInvalid) {
		t.Fatalf("a trailing byte: %v", err)
	}
}

// reseal makes damaged contents pass the whole-file check, to show the structural checks hold on their own.
func reseal(b []byte) []byte {
	binary.LittleEndian.PutUint32(b[len(b)-12:], crc32.Checksum(b[:len(b)-12], crcTable))
	return b
}

func TestSegIndexStructureIsValidatedBeyondTheCheck(t *testing.T) {
	x, foot := sampleIndex()
	good := encodeSegIndex(x, foot)
	parts := int(binary.LittleEndian.Uint32(good[36:]))
	sparseAt := idxHead + idxSummary + idxPartRec*parts
	partAt := func(i int) int { return idxHead + idxSummary + idxPartRec*i }
	for name, mutate := range map[string]func(b []byte){
		"partitions not ascending":       func(b []byte) { binary.LittleEndian.PutUint32(b[partAt(1):], 0) },
		"sparse start not contiguous":    func(b []byte) { binary.LittleEndian.PutUint32(b[partAt(1)+20:], 5) },
		"count without sparse entries":   func(b []byte) { binary.LittleEndian.PutUint32(b[partAt(0)+24:], 0) },
		"sparse offset out of range":     func(b []byte) { binary.LittleEndian.PutUint64(b[sparseAt:], 1<<50) },
		"record counts do not add up":    func(b []byte) { binary.LittleEndian.PutUint64(b[12:], 1) },
		"counts do not match the length": func(b []byte) { binary.LittleEndian.PutUint32(b[44:], 1<<20) },
		"signature":                      func(b []byte) { b[0] = 'X' },
		"version":                        func(b []byte) { binary.LittleEndian.PutUint32(b[8:], 99) },
	} {
		bad := bytes.Clone(good)
		mutate(bad)
		if _, _, err := decodeSegIndex(reseal(bad)); !errors.Is(err, errIdxInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// ID entries out of order
	idAt := sparseAt + 12*int(binary.LittleEndian.Uint32(good[40:]))
	bad := bytes.Clone(good)
	a, c := bytes.Clone(bad[idAt:idAt+16]), bytes.Clone(bad[idAt+16:idAt+32])
	copy(bad[idAt:], c)
	copy(bad[idAt+16:], a)
	if _, _, err := decodeSegIndex(reseal(bad)); !errors.Is(err, errIdxInvalid) {
		t.Errorf("unsorted ID entries: %v", err)
	}
}

// ----- the index inside a log -----

type logBuilder struct {
	t      *testing.T
	fs     *memFS
	l      *segLog
	st     *logState
	values map[[2]uint64][]byte
	cpID   uint64
}

func newLogBuilder(t *testing.T) *logBuilder {
	t.Helper()
	fs := newMemFS()
	l, err := openSegLog(fs, testDir)
	if err != nil {
		t.Fatal(err)
	}
	st := newLogState()
	st.setConfig([]byte("{}"))
	for _, p := range []Partition{{"a", 0}, {"b", 0}} {
		if _, _, err = st.ensure(p); err != nil {
			t.Fatal(err)
		}
	}
	b := &logBuilder{t: t, fs: fs, l: l, st: st, values: map[[2]uint64][]byte{}}
	if err = l.start(st.snapshot(b.nextCP(), 1)); err != nil {
		t.Fatal(err)
	}
	return b
}

func (b *logBuilder) nextCP() uint64 { b.cpID++; return b.cpID }

func (b *logBuilder) rec(pid uint32, id string) {
	b.t.Helper()
	off := uint64(b.st.parts[pid].next)
	val := []byte(fmt.Sprintf("p%d-o%d-%s", pid, off, bytes.Repeat([]byte("v"), int(off%17))))
	r := record3{Partition: pid, Offset: off, Time: int64(off), ID: []byte(id), Value: val}
	frame, err := appendRecord3(nil, r)
	if err != nil {
		b.t.Fatal(err)
	}
	pos := b.l.active.end
	if err = b.l.append(frame); err != nil {
		b.t.Fatal(err)
	}
	if err = b.st.applyRecord(pid, off); err != nil {
		b.t.Fatal(err)
	}
	b.l.observe(r, pos)
	b.values[[2]uint64{uint64(pid), off}] = val
}

func (b *logBuilder) roll() {
	b.t.Helper()
	if err := b.l.roll(b.st.snapshot(b.nextCP(), 0)); err != nil {
		b.t.Fatal(err)
	}
}

func (b *logBuilder) checkAll(l *segLog, label string) {
	b.t.Helper()
	for k, want := range b.values {
		rec, err := l.readRecord(uint32(k[0]), k[1])
		if err != nil || !bytes.Equal(rec.Value, want) || rec.Offset != k[1] || rec.Partition != uint32(k[0]) {
			b.t.Fatalf("%s: partition %d offset %d: %v %q", label, k[0], k[1], err, rec.Value)
		}
	}
}

// fill writes records of both partitions interleaved, with an ID on every fifth, rolling every
// perSeg records, ending with 'segments' segments in the log.
func (b *logBuilder) fill(segments, perSeg int) {
	n := 0
	for s := 0; s < segments; s++ {
		for i := 0; i < perSeg; i++ {
			id := ""
			if n%5 == 0 {
				id = fmt.Sprintf("id-%d", n)
			}
			b.rec(uint32(n%2), id)
			n++
		}
		if s < segments-1 {
			b.roll()
		}
	}
}

func TestEveryRecordIsReadableAcrossSegmentsAndAfterReopen(t *testing.T) {
	b := newLogBuilder(t)
	b.fill(5, 60)
	b.checkAll(b.l, "live")
	if len(b.l.ids) != 4 || b.l.active.id != 5 {
		t.Fatalf("ids %v active %d", b.l.ids, b.l.active.id)
	}
	b.l.close()
	img := b.fs.image(false, "durable")
	l, err := openSegLog(img, testDir)
	if err != nil {
		t.Fatal(err)
	}
	b.checkAll(l, "after reopen")
	if l.rebuilds != 0 {
		t.Fatalf("%d indexes were rebuilt although every idx file was written", l.rebuilds)
	}
	// the active segment's index, built by the scan at Open, equals the one built as records were written
	live := b.l.idx
	foot := idxFooterInfo{segment: 5, size: uint64(l.active.end), lastPos: uint64(l.active.lastPos), lastCRC: l.active.lastCRC}
	if !l.idx.sameAs(live, foot) {
		t.Fatal("the index rebuilt by Open differs from the one built while writing")
	}
	// a record that does not exist
	if _, err = l.readRecord(0, 1<<40); !errors.Is(err, errSegNotFound) {
		t.Fatalf("a record beyond the end: %v", err)
	}
	if _, err = l.readRecord(7, 0); !errors.Is(err, errSegNotFound) {
		t.Fatalf("an unknown partition: %v", err)
	}
}

func TestAnIdxFileIsAHintAndIsRebuiltWhenMissingOrWrong(t *testing.T) {
	b := newLogBuilder(t)
	b.fill(4, 50)
	b.l.close()
	good := b.fs.image(false, "durable")
	idx1 := filepath.Join(testDir, idxName(1))
	idx2 := filepath.Join(testDir, idxName(2))
	idx3 := filepath.Join(testDir, idxName(3))
	damages := map[string]func(fs *memFS){
		"missing":            func(fs *memFS) { delete(fs.ns, idx1); delete(fs.dns, idx1) },
		"empty":              func(fs *memFS) { fs.ns[idx1].vol, fs.ns[idx1].dur = nil, nil },
		"flipped byte":       func(fs *memFS) { n := fs.ns[idx1]; n.vol[len(n.vol)/2] ^= 1; n.dur = bytes.Clone(n.vol) },
		"truncated":          func(fs *memFS) { n := fs.ns[idx1]; n.vol = n.vol[:len(n.vol)-9]; n.dur = bytes.Clone(n.vol) },
		"another segment's":  func(fs *memFS) { n := fs.ns[idx1]; n.vol = bytes.Clone(fs.ns[idx2].vol); n.dur = bytes.Clone(n.vol) },
		"describes a longer": func(fs *memFS) { n := fs.ns[idx1]; n.vol = bytes.Clone(fs.ns[idx3].vol); n.dur = bytes.Clone(n.vol) },
	}
	for name, damage := range damages {
		fs := good.image(false, "durable")
		damage(fs)
		l, err := openSegLog(fs, testDir)
		if err != nil {
			t.Fatalf("%s: Open failed over a damaged hint: %v", name, err)
		}
		b.checkAll(l, name)
		if l.rebuilds < 1 {
			t.Errorf("%s: no index was rebuilt", name)
		}
		// the rebuilt idx file was written back and now verifies
		x, err := l.loadIdxFile(1)
		if err != nil || x == nil {
			t.Errorf("%s: the rebuilt idx file does not verify: %v", name, err)
		}
	}
	// a segment's own damage is not hidden by its idx: a flipped byte in a sealed segment shows up as corruption when read
	fs := good.image(false, "durable")
	seg1 := fs.ns[filepath.Join(testDir, segName(1))]
	seg1.vol[len(seg1.vol)-3] ^= 0x20
	seg1.dur = bytes.Clone(seg1.vol)
	l, err := openSegLog(fs, testDir)
	if err != nil {
		t.Fatal(err)
	}
	var corrupted bool
	for k := range b.values {
		if _, err := l.readRecord(uint32(k[0]), k[1]); errors.Is(err, ErrCorruptLog) {
			corrupted = true
		}
	}
	if !corrupted {
		t.Fatal("a damaged sealed segment was read without complaint")
	}
}

func TestIdxWriteCrashesLeaveReadsWorking(t *testing.T) {
	// crash at every operation of a run with two rolls (so two idx writes); recovery must always read every acknowledged record
	run := func(fs *memFS) (acked map[[2]uint64][]byte) {
		b := &logBuilder{t: t, fs: fs, values: map[[2]uint64][]byte{}}
		defer func() { recover() }() // the builder fatals on the injected crash: stop there
		l, _ := openSegLog(fs, testDir)
		b.l = l
		b.st = newLogState()
		b.st.setConfig([]byte("{}"))
		b.st.ensure(Partition{"a", 0})
		b.st.ensure(Partition{"b", 0})
		if err := l.start(b.st.snapshot(b.nextCP(), 1)); err != nil {
			return b.values
		}
		for s := 0; s < 3; s++ {
			for i := 0; i < 8; i++ {
				off := uint64(b.st.parts[uint32(i%2)].next)
				pid := uint32(i % 2)
				val := []byte(fmt.Sprintf("v-%d-%d", pid, off))
				r := record3{Partition: pid, Offset: off, Time: int64(off), ID: []byte(fmt.Sprintf("i%d", off)), Value: val}
				frame, _ := appendRecord3(nil, r)
				pos := l.active.end
				if err := l.append(frame); err != nil {
					return b.values
				}
				b.st.applyRecord(pid, off)
				l.observe(r, pos)
				b.values[[2]uint64{uint64(pid), off}] = val
			}
			if s < 2 {
				if err := l.roll(b.st.snapshot(b.nextCP(), 0)); err != nil {
					return b.values
				}
			}
		}
		return b.values
	}
	probe := newMemFS()
	run(probe)
	total := probe.ops
	for failAt := 1; failAt <= total; failAt++ {
		for _, volNames := range []bool{false, true} {
			fs := newMemFS()
			fs.failAt = failAt
			acked := run(fs)
			img := fs.image(volNames, "torn")
			l, err := openSegLog(img, testDir)
			if err != nil {
				t.Fatalf("crash at %d: %v", failAt, err)
			}
			for k, want := range acked {
				rec, err := l.readRecord(uint32(k[0]), k[1])
				if err != nil || !bytes.Equal(rec.Value, want) {
					t.Fatalf("crash at op %d of %d (names volatile %v): acknowledged record %v unreadable: %v", failAt, total, volNames, k, err)
				}
			}
		}
	}
}

func TestIDWindowLookupAcrossSegments(t *testing.T) {
	b := newLogBuilder(t)
	// ids "w-<segment>" planted one per segment in partition 0, four segments
	for s := 1; s <= 4; s++ {
		b.rec(0, fmt.Sprintf("w-%d", s))
		b.rec(1, "")
		if s < 4 {
			b.roll()
		}
	}
	find := func(l *segLog, id string, window int) (segs []uint64) {
		if err := l.findIDs(0, []byte(id), window, func(seg, off uint64, _ *segIndex) bool { segs = append(segs, seg); return false }); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, c := range []struct {
		id     string
		window int
		want   []uint64
	}{
		{"w-4", 0, []uint64{4}}, // the active segment, window zero
		{"w-3", 0, nil},         // one segment back is outside a window of zero
		{"w-3", 1, []uint64{3}}, // inside a window of one
		{"w-2", 1, nil},         // two back, outside a window of one
		{"w-2", 2, []uint64{2}}, // inside a window of two
		{"w-1", 3, []uint64{1}}, // the oldest, window of three
		{"w-1", 2, nil},         // outside
		{"absent", 8, nil},      // never published
		{"w-4", 8, []uint64{4}}, // newest first, and only where it is
	} {
		if got := find(b.l, c.id, c.window); !slices.Equal(got, c.want) {
			t.Errorf("%s with window %d: segments %v, want %v", c.id, c.window, got, c.want)
		}
	}
	// the same after a reopen, with one idx file missing (rebuilt: the window does not shrink)
	b.l.close()
	img := b.fs.image(false, "durable")
	delete(img.ns, filepath.Join(testDir, idxName(2)))
	delete(img.dns, filepath.Join(testDir, idxName(2)))
	l, err := openSegLog(img, testDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := find(l, "w-2", 2); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("after reopen with a missing idx: %v", got)
	}
	if l.rebuilds != 1 {
		t.Fatalf("%d rebuilds", l.rebuilds)
	}
	// a hash match is only a candidate: a different ID with the same hash is the caller's to reject
	var candidates int
	l.findIDs(0, []byte("w-4"), 0, func(seg, off uint64, _ *segIndex) bool { candidates++; return false })
	if candidates != 1 {
		t.Fatalf("%d candidates", candidates)
	}
}

func TestRetiredSegmentsAreUnlinkedWhenTheLastReaderLetsGo(t *testing.T) {
	b := newLogBuilder(t)
	b.fill(4, 20)
	l := b.l
	has := func(name string) bool { return slices.Contains(dirNames(t, b.fs), name) }
	if !has(segName(1)) || !has(idxName(1)) {
		t.Fatal("segment 1 or its idx is missing")
	}
	// a reader holds segment 2 while retention retires 1 and 2
	if !l.pin(2) {
		t.Fatal("pin refused")
	}
	l.retire(1)
	l.retire(2)
	if has(segName(1)) || has(idxName(1)) {
		t.Fatal("an unpinned retired segment was not removed at once")
	}
	if !has(segName(2)) || !has(idxName(2)) {
		t.Fatal("a pinned retired segment was removed under its reader")
	}
	if l.pin(2) {
		t.Fatal("a retired segment could be pinned again")
	}
	// records of the retired segments are no longer found
	if _, err := l.readRecord(0, 0); !errors.Is(err, errSegNotFound) {
		t.Fatalf("a retired record: %v", err)
	}
	// and the surviving segments still read
	for k, want := range b.values {
		if k[1] < 20 {
			continue // some of segments 1 and 2 (offsets are spread over two partitions)
		}
		if rec, err := l.readRecord(uint32(k[0]), k[1]); err != nil || !bytes.Equal(rec.Value, want) {
			t.Fatalf("%v: unreadable or wrong after retirement: %v", k, err)
		}
	}
	l.unpin(2)
	if has(segName(2)) || has(idxName(2)) {
		t.Fatal("the last reader letting go did not remove the retired segment")
	}
	if !has(segName(3)) || !has(segName(4)) {
		t.Fatal("a live segment was removed")
	}
}
