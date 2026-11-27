package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
)

// The per-segment index of the segmented log (design v1.1/v1.2, change (b2)): a sparse map from a
// record's offset to its position in the segment, a table of the IDs published in the segment
// (for the idempotency window), and a summary. It is built from the segment's own frames, held in
// memory for the active segment, and written beside a segment when it is sealed as seg-<id>.idx.
// The file is a HINT: it is verified against the segment before use and rebuilt by scanning that
// one segment when it is missing or wrong; it is never trusted over the log.

const sparseStride = 16 // a partition's records get an index entry at every offset divisible by this, and at the segment's first

// ----- the ID table -----

// idHash is a stable 64-bit hash of a partition id and an idempotency ID (FNV-1a). It is persisted
// in idx files, so it must never change; a collision is harmless because a hit is always confirmed
// against the log.
func idHash(pid uint32, id []byte) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < 4; i++ {
		h = (h ^ uint64(byte(pid>>(8*i)))) * 1099511628211
	}
	for _, c := range id {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return h
}

// idTable maps hashes to record offsets with open addressing. Slots hold (hash, offset+1); an offset
// slot of zero is empty. Equal hashes (distinct IDs that collide, or repeated entries) coexist.
type idTable struct {
	keys, vals []uint64
	n          int
}

func (t *idTable) slot(h uint64) int { return int((h ^ h>>32) & uint64(len(t.keys)-1)) }

func (t *idTable) add(h, off uint64) {
	if len(t.keys) == 0 || (t.n+1)*4 > len(t.keys)*3 {
		t.grow()
	}
	i := t.slot(h)
	for t.vals[i] != 0 {
		i = (i + 1) & (len(t.keys) - 1)
	}
	t.keys[i], t.vals[i] = h, off+1
	t.n++
}

func (t *idTable) grow() {
	old := *t
	size := 16
	if len(old.keys) > 0 {
		size = len(old.keys) * 2
	}
	t.keys, t.vals, t.n = make([]uint64, size), make([]uint64, size), 0
	for i, v := range old.vals {
		if v != 0 {
			t.add(old.keys[i], v-1)
		}
	}
}

// lookup calls visit with the offset of every entry whose hash is h, until visit returns true.
func (t *idTable) lookup(h uint64, visit func(off uint64) bool) {
	if len(t.keys) == 0 {
		return
	}
	for i := t.slot(h); t.vals[i] != 0; i = (i + 1) & (len(t.keys) - 1) {
		if t.keys[i] == h && visit(t.vals[i]-1) {
			return
		}
	}
}

func (t *idTable) each(fn func(h, off uint64)) {
	for i, v := range t.vals {
		if v != 0 {
			fn(t.keys[i], v-1)
		}
	}
}

func (t *idTable) len() int { return t.n }

// ----- the index -----

type idxPart struct {
	first, count uint64   // the partition's first offset in this segment and how many records it holds here
	offs         []uint64 // sparse entries: offset ...
	poss         []uint32 // ... and the position of that record's frame in the segment file
}

type segIndex struct {
	footer              idxFooterInfo     // identity verified on load or captured on seal
	startNext           map[uint32]uint64 // every partition's next offset when the segment began (its start checkpoint)
	parts               map[uint32]*idxPart
	ids                 idTable
	records             uint64
	firstTime, lastTime int64
}

func newSegIndex(startNext map[uint32]uint64) *segIndex {
	return &segIndex{startNext: startNext, parts: map[uint32]*idxPart{}}
}

func startNextOf(st *logState) map[uint32]uint64 {
	m := make(map[uint32]uint64, len(st.parts))
	for pid, ps := range st.parts {
		m[pid] = uint64(ps.next)
	}
	return m
}

// observe adds one record frame at pos. Positions must fit 32 bits (a segment is far smaller).
func (x *segIndex) observe(rec record3, pos int64) {
	p := x.parts[rec.Partition]
	if p == nil {
		first, ok := x.startNext[rec.Partition]
		if !ok {
			first = rec.Offset // a partition defined in this segment starts at its first record
		}
		p = &idxPart{first: first}
		x.parts[rec.Partition] = p
	}
	p.count++
	if len(p.offs) == 0 || rec.Offset%sparseStride == 0 {
		p.offs, p.poss = append(p.offs, rec.Offset), append(p.poss, uint32(pos))
	}
	if len(rec.ID) > 0 {
		x.ids.add(idHash(rec.Partition, rec.ID), rec.Offset)
	}
	if x.records == 0 {
		x.firstTime = rec.Time
	}
	x.lastTime = rec.Time
	x.records++
}

// first is the partition's first offset in this segment: its next offset at the segment's start,
// or 0 when it did not exist yet. It never decreases from one segment to the next, which is what
// lets a lookup binary-search the segments.
func (x *segIndex) first(pid uint32) uint64 {
	if p := x.parts[pid]; p != nil {
		return p.first
	}
	return x.startNext[pid]
}

func (x *segIndex) count(pid uint32) uint64 {
	if p := x.parts[pid]; p != nil {
		return p.count
	}
	return 0
}

// find returns the position of the sparse entry at or before offset, from which a walk reaches it.
func (x *segIndex) find(pid uint32, off uint64) (int64, bool) {
	p := x.parts[pid]
	if p == nil || off < p.first || off >= p.first+p.count {
		return 0, false
	}
	i := sort.Search(len(p.offs), func(i int) bool { return p.offs[i] > off }) - 1
	if i < 0 {
		return 0, false
	}
	return int64(p.poss[i]), true
}

// ----- the idx file -----

const (
	idxSig     = "DOSTIDX1"
	idxEnd     = "IDXEND01"
	idxVersion = 1
	idxHead    = 12
	idxSummary = 36
	idxPartRec = 28
	idxFooter  = 40
)

func idxName(id uint64) string    { return fmt.Sprintf("seg-%016x.idx", id) }
func idxTmpName(id uint64) string { return fmt.Sprintf("seg-%016x.idx.tmp", id) }

// idxFooterInfo is what an idx file says about the segment it was made from.
type idxFooterInfo struct {
	segment, size, lastPos uint64
	lastCRC                uint32
}

var errIdxInvalid = errors.New("idx file invalid")

// encodeSegIndex serializes x deterministically: partitions ascending, ID entries by (hash, offset).
func encodeSegIndex(x *segIndex, f idxFooterInfo) []byte {
	pids := make([]uint32, 0, len(x.startNext)+len(x.parts))
	seen := map[uint32]bool{}
	for pid := range x.startNext {
		seen[pid] = true
		pids = append(pids, pid)
	}
	for pid := range x.parts {
		if !seen[pid] {
			pids = append(pids, pid)
		}
	}
	slices.Sort(pids)
	var sparse int
	for _, pid := range pids {
		if p := x.parts[pid]; p != nil {
			sparse += len(p.offs)
		}
	}
	type idEntry struct{ h, off uint64 }
	ids := make([]idEntry, 0, x.ids.len())
	x.ids.each(func(h, off uint64) { ids = append(ids, idEntry{h, off}) })
	slices.SortFunc(ids, func(a, b idEntry) int {
		if a.h != b.h {
			if a.h < b.h {
				return -1
			}
			return 1
		}
		switch {
		case a.off < b.off:
			return -1
		case a.off > b.off:
			return 1
		}
		return 0
	})
	b := make([]byte, 0, idxHead+idxSummary+idxPartRec*len(pids)+12*sparse+16*len(ids)+idxFooter)
	b = append(b, idxSig...)
	b = binary.LittleEndian.AppendUint32(b, idxVersion)
	b = binary.LittleEndian.AppendUint64(b, x.records)
	b = binary.LittleEndian.AppendUint64(b, uint64(x.firstTime))
	b = binary.LittleEndian.AppendUint64(b, uint64(x.lastTime))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(pids)))
	b = binary.LittleEndian.AppendUint32(b, uint32(sparse))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(ids)))
	var start uint32
	for _, pid := range pids {
		var first, count uint64
		var n uint32
		if p := x.parts[pid]; p != nil {
			first, count, n = p.first, p.count, uint32(len(p.offs))
		} else {
			first = x.startNext[pid]
		}
		b = binary.LittleEndian.AppendUint32(b, pid)
		b = binary.LittleEndian.AppendUint64(b, first)
		b = binary.LittleEndian.AppendUint64(b, count)
		b = binary.LittleEndian.AppendUint32(b, start)
		b = binary.LittleEndian.AppendUint32(b, n)
		start += n
	}
	for _, pid := range pids {
		if p := x.parts[pid]; p != nil {
			for i := range p.offs {
				b = binary.LittleEndian.AppendUint64(b, p.offs[i])
				b = binary.LittleEndian.AppendUint32(b, p.poss[i])
			}
		}
	}
	for _, e := range ids {
		b = binary.LittleEndian.AppendUint64(b, e.h)
		b = binary.LittleEndian.AppendUint64(b, e.off)
	}
	b = binary.LittleEndian.AppendUint64(b, f.segment)
	b = binary.LittleEndian.AppendUint64(b, f.size)
	b = binary.LittleEndian.AppendUint64(b, f.lastPos)
	b = binary.LittleEndian.AppendUint32(b, f.lastCRC)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable)) // over everything before this field
	return append(b, idxEnd...)
}

// decodeSegIndex validates an idx file as a whole (signature, version, check, exact length for the
// declared counts, ascending partitions, contiguous and in-range sparse entries, sorted ID entries)
// and returns the index and what it says about its segment. It does not look at the segment.
func decodeSegIndex(b []byte) (*segIndex, idxFooterInfo, error) {
	var f idxFooterInfo
	bad := func(format string, a ...any) (*segIndex, idxFooterInfo, error) {
		return nil, f, fmt.Errorf("%w: %s", errIdxInvalid, fmt.Sprintf(format, a...))
	}
	if len(b) < idxHead+idxSummary+idxFooter {
		return bad("%d bytes", len(b))
	}
	if string(b[:8]) != idxSig || string(b[len(b)-8:]) != idxEnd {
		return bad("signature")
	}
	if v := binary.LittleEndian.Uint32(b[8:]); v != idxVersion {
		return bad("version %d", v)
	}
	if binary.LittleEndian.Uint32(b[len(b)-12:]) != crc32.Checksum(b[:len(b)-12], crcTable) {
		return bad("check failed")
	}
	x := newSegIndex(map[uint32]uint64{})
	x.records = binary.LittleEndian.Uint64(b[12:])
	x.firstTime = int64(binary.LittleEndian.Uint64(b[20:]))
	x.lastTime = int64(binary.LittleEndian.Uint64(b[28:]))
	parts := int(binary.LittleEndian.Uint32(b[36:]))
	sparse := int(binary.LittleEndian.Uint32(b[40:]))
	ids := int(binary.LittleEndian.Uint32(b[44:]))
	if parts < 0 || sparse < 0 || ids < 0 || idxHead+idxSummary+idxPartRec*parts+12*sparse+16*ids+idxFooter != len(b) {
		return bad("counts do not match the length")
	}
	foot := b[len(b)-idxFooter:]
	f = idxFooterInfo{binary.LittleEndian.Uint64(foot), binary.LittleEndian.Uint64(foot[8:]), binary.LittleEndian.Uint64(foot[16:]), binary.LittleEndian.Uint32(foot[24:])}
	at := idxHead + idxSummary
	sparseAt := at + idxPartRec*parts
	var next uint32
	var prev int64 = -1
	var totalCount uint64
	for i := 0; i < parts; i++ {
		r := b[at+idxPartRec*i:]
		pid, first, count := binary.LittleEndian.Uint32(r), binary.LittleEndian.Uint64(r[4:]), binary.LittleEndian.Uint64(r[12:])
		start, n := binary.LittleEndian.Uint32(r[20:]), binary.LittleEndian.Uint32(r[24:])
		if int64(pid) <= prev {
			return bad("partition ids are not ascending")
		}
		prev = int64(pid)
		if start != next || int(start)+int(n) > sparse || (count == 0) != (n == 0) {
			return bad("partition %d has inconsistent sparse entries", pid)
		}
		next += n
		totalCount += count
		x.startNext[pid] = first
		if count > 0 {
			p := &idxPart{first: first, count: count, offs: make([]uint64, n), poss: make([]uint32, n)}
			var last uint64
			for k := uint32(0); k < n; k++ {
				e := b[sparseAt+12*int(start+k):]
				off, pos := binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint32(e[8:])
				if off < first || off >= first+count || (k > 0 && off <= last) {
					return bad("partition %d has a sparse entry out of order or range", pid)
				}
				p.offs[k], p.poss[k], last = off, pos, off
			}
			x.parts[pid] = p
		}
	}
	if int(next) != sparse || totalCount != x.records {
		return bad("sparse entries or record counts do not add up")
	}
	idAt := sparseAt + 12*sparse
	var ph, po uint64
	for i := 0; i < ids; i++ {
		e := b[idAt+16*i:]
		h, off := binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint64(e[8:])
		if i > 0 && (h < ph || (h == ph && off < po)) {
			return bad("ID entries are not sorted")
		}
		ph, po = h, off
		x.ids.add(h, off)
	}
	return x, f, nil
}

// equal reports whether two indexes serialize identically (used by tests and by rebuild checks).
func (x *segIndex) sameAs(o *segIndex, f idxFooterInfo) bool {
	return bytes.Equal(encodeSegIndex(x, f), encodeSegIndex(o, f))
}
