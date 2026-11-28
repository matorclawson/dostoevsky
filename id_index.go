package dostoevsky

import "hash/maphash"

// Fingerprints live only in the in-memory index. The journal holds the full ID.
// On a hit we compare the stored ID before returning a receipt, including when
// several IDs share a fingerprint. A miss costs one in-memory map lookup.
var producerIDSeed = maphash.MakeSeed()

type idIndex struct {
	latest map[uint64]int64
	// previous is allocated only if two records have the same fingerprint.
	previous map[int64]int64
}

func (idx *idIndex) add(hash uint64, offset int64) {
	if idx.latest == nil {
		idx.latest = make(map[uint64]int64)
	}
	if old, exists := idx.latest[hash]; exists {
		if idx.previous == nil {
			idx.previous = make(map[int64]int64)
		}
		idx.previous[offset] = old
	}
	idx.latest[hash] = offset
}

func (s *stream) storedID(j *journal, id string) (*Record, error) {
	if id == "" {
		return nil, nil
	}
	if id == s.firstID {
		event, err := j.read(s.first)
		if err != nil {
			return nil, err
		}
		return event.Record, nil
	}
	if s.tail == nil || s.tail.ids == nil {
		return nil, nil
	}
	idx := s.tail.ids
	for offset, ok := idx.latest[maphash.String(producerIDSeed, id)]; ok; offset, ok = idx.previous[offset] {
		event, err := j.read(s.location(offset - s.base))
		if err != nil {
			return nil, err
		}
		if event.Record.ID == id {
			return event.Record, nil
		}
	}
	return nil, nil
}
