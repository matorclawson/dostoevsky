package dostoevsky

import "encoding/binary"

// Small streams keep their direct locations. Longer streams encode physical gaps
// and frame lengths in blocks. Each checkpoint bounds a lookup to 32 entries.
// The index is rebuilt from the unchanged journal; it has no on-disk format.
const (
	locationStride    = 32
	locationBlockSize = 256
)

type locationBlock struct {
	starts    [locationBlockSize / locationStride]int64
	positions [locationBlockSize / locationStride]uint16
	data      []byte
}
type locationIndex struct {
	blocks []*locationBlock
	count  int64
	next   int64 // payload address assuming the next journal frame belongs to this stream
}

func (idx *locationIndex) append(loc location) {
	slot := int(idx.count % locationBlockSize)
	if slot == 0 {
		idx.blocks = append(idx.blocks, &locationBlock{data: make([]byte, 0, locationBlockSize*2)})
	}
	block := idx.blocks[len(idx.blocks)-1]
	gap := loc.at - idx.next
	if slot%locationStride == 0 {
		check := slot / locationStride
		block.starts[check] = loc.at
		block.positions[check] = uint16(len(block.data))
		gap = 0
	}
	code := uint64(loc.size) << 1
	if gap != 0 {
		code |= 1
	}
	block.data = binary.AppendUvarint(block.data, code)
	if gap != 0 {
		block.data = binary.AppendUvarint(block.data, uint64(gap))
	}
	idx.next = loc.at + int64(loc.size) + 20
	idx.count++
}
func (idx *locationIndex) location(i int64) location {
	block := idx.blocks[i/locationBlockSize]
	slot := int(i % locationBlockSize)
	check := slot / locationStride
	at := block.starts[check]
	b := block.data[block.positions[check]:]
	for j := check * locationStride; ; j++ {
		code, n := binary.Uvarint(b)
		b = b[n:]
		if code&1 != 0 {
			gap, n := binary.Uvarint(b)
			at += int64(gap)
			b = b[n:]
		}
		size := int(code >> 1)
		if j == slot {
			return location{at: at, size: size}
		}
		at += int64(size) + 20
	}
}
