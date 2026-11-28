package dostoevsky

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
)

// Immutable Bloom filters contain partition-qualified hashes. Only a negative answer is
// definitive. A positive always follows the exact index and full-record comparison.
type idFilter []byte

const filterProbes = 20
const maxFilterBytes = 4 << 20
const filterCacheBytes = 64 << 20

func filterName(id uint64) string { return fmt.Sprintf("seg-%016x.bloom", id) }
func mixFilterHash(h uint64) uint64 {
	// Avalanche the persistent FNV hash before double hashing.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}
func filterPositions(h uint64, bits uint32) (uint32, uint32) {
	pos := uint32((uint64(uint32(h)) * uint64(bits)) >> 32)
	step := uint32(((h>>32)*uint64(bits-1))>>32) + 1
	return pos, step
}

// contains takes an already mixed hash so one lookup can visit several segments.
func (f idFilter) contains(h uint64) bool {
	if len(f) == 0 {
		return false
	}
	bits := uint32(len(f) * 8)
	p, step := filterPositions(h, bits)
	for i := 0; i < filterProbes; i++ {
		if f[p>>3]&(1<<(p&7)) == 0 {
			return false
		}
		p += step
		if p >= bits {
			p -= bits
		}
	}
	return true
}
func makeIDFilter(t *idTable) idFilter {
	f := make(idFilter, min(maxFilterBytes, t.n*4))
	if len(f) == 0 {
		return f
	}
	bits := uint32(len(f) * 8)
	t.each(func(h, _ uint64) {
		p, step := filterPositions(mixFilterHash(h), bits)
		for i := 0; i < filterProbes; i++ {
			f[p>>3] |= 1 << (p & 7)
			p += step
			if p >= bits {
				p -= bits
			}
		}
	})
	return f
}

type idFilterCache struct {
	ordered []idFilter // populated when all sealed filters are resident, in log order
	entries map[uint64]idFilter
	order   []uint64
	bytes   int
}

func (c *idFilterCache) remove(id uint64) {
	if f, ok := c.entries[id]; ok {
		c.ordered = nil
		c.bytes -= len(f)
		delete(c.entries, id)
		c.order = slices.DeleteFunc(c.order, func(k uint64) bool { return k == id })
	}
}
func (c *idFilterCache) put(id uint64, f idFilter) {
	if _, ok := c.entries[id]; ok {
		return
	}
	c.ordered = nil
	for c.bytes+len(f) > filterCacheBytes && len(c.order) > 0 {
		k := c.order[0]
		c.order = c.order[1:]
		c.bytes -= len(c.entries[k])
		delete(c.entries, k)
	}
	c.entries[id] = f
	c.order = append(c.order, id)
	c.bytes += len(f)
}
func (l *segLog) rememberFilter(id uint64, x *segIndex) error {
	f := makeIDFilter(&x.ids)
	l.filters.put(id, f)
	info := x.footer
	b := append([]byte(nil), "DOSTBLM2"...)
	for _, v := range []uint64{id, info.size, info.lastPos} {
		b = binary.LittleEndian.AppendUint64(b, v)
	}
	b = binary.LittleEndian.AppendUint32(b, info.lastCRC)
	b = append(b, f...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
	// A missing sidecar costs an exact lookup on the next open.
	return l.writeHint(filterName(id), b)
}
func (l *segLog) readFilter(id uint64) (idFilter, error) {
	f, e := l.fs.OpenFile(filepath.Join(l.dir, filterName(id)), os.O_RDONLY, 0)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	size, e := f.Size()
	if e != nil {
		return nil, e
	}
	if size < 40 || size > maxFilterBytes+40 {
		return nil, errIdxInvalid
	}
	b := make([]byte, size)
	n, e := f.ReadAt(b, 0)
	if n != len(b) {
		return nil, io.ErrUnexpectedEOF
	}
	if e != nil && e != io.EOF {
		return nil, e
	}
	end := len(b) - 4
	if string(b[:8]) != "DOSTBLM2" || binary.LittleEndian.Uint32(b[end:]) != crc32.Checksum(b[:end], crcTable) {
		return nil, errIdxInvalid
	}
	info := idxFooterInfo{binary.LittleEndian.Uint64(b[8:]), binary.LittleEndian.Uint64(b[16:]), binary.LittleEndian.Uint64(b[24:]), binary.LittleEndian.Uint32(b[32:])}
	if e = l.checkIndexSegment(id, info); e != nil {
		return nil, e
	}
	return idFilter(b[36:end]), nil
}

func (c *idFilterCache) orderedFilters(ids []uint64) []idFilter {
	if c.ordered != nil {
		return c.ordered
	}
	if len(c.entries) != len(ids) {
		return nil
	}
	filters := make([]idFilter, len(ids))
	for i, id := range ids {
		f, ok := c.entries[id]
		if !ok {
			return nil
		}
		filters[i] = f
	}
	c.ordered = filters
	return filters
}

// PrepareIDFilters verifies existing sealed ID filters and builds any missing ones.
// Use it before admitting traffic to move first-use work off the publish path.
// It takes the writer lock for the operation. Cancellation takes effect between segments.
func (n *Node) PrepareIDFilters(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return ErrClosed
	}
	l := n.segments
	if l == nil {
		return ErrInvalid
	}
	if l.failed != nil {
		return l.failed
	}
	for _, id := range l.ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if f, err := l.readFilter(id); err == nil {
			l.filters.put(id, f)
			continue
		}
		x, err := l.sealedIndex(id)
		if err != nil {
			return err
		}
		if err = l.rememberFilter(id, x); err != nil {
			return err
		}
	}
	return ctx.Err()
}
