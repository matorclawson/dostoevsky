package dostoevsky

// A small count-min sketch of sealed-index requests. Counters age in access
// time so a past burst cannot protect a cold index indefinitely.
type indexFrequency struct {
	rows     [4][256]uint8
	accesses uint64
}

func frequencyHash(id uint64) uint64 {
	id ^= id >> 30
	id *= 0xbf58476d1ce4e5b9
	id ^= id >> 27
	id *= 0x94d049bb133111eb
	return id ^ (id >> 31)
}

func (f *indexFrequency) observe(id uint64) {
	f.accesses++
	if f.accesses%512 == 0 {
		for i := range f.rows {
			for j := range f.rows[i] {
				f.rows[i][j] >>= 1
			}
		}
	}
	h := frequencyHash(id)
	for i := range f.rows {
		c := &f.rows[i][byte(h)]
		if *c < 255 {
			*c++
		}
		h >>= 8
	}
}

func (f *indexFrequency) score(id uint64) uint8 {
	h := frequencyHash(id)
	v := uint8(255)
	for i := range f.rows {
		v = min(v, f.rows[i][byte(h)])
		h >>= 8
	}
	return v
}

const frequencyCharge = 1152 // includes the sketch and allocator rounding

// EnableIndexAdmission enables frequency-based admission for the sealed index
// cache. Call after opening a segmented node. Returns false on legacy storage.
// Cache policy is process-local and has no effect on durable state.
func (n *Node) EnableIndexAdmission() bool {
	l := n.segments
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.frequency == nil {
		l.frequency = new(indexFrequency)
		for len(l.cacheOrder) > 0 && l.cacheIDBytes > (8<<20)-frequencyCharge {
			l.evictIndex(0)
		}
	}
	return true
}

func (l *segLog) evictIndex(i int) {
	id := l.cacheOrder[i]
	l.cacheIDBytes -= len(l.cache[id].ids.keys) * 16
	delete(l.cache, id)
	copy(l.cacheOrder[i:], l.cacheOrder[i+1:])
	l.cacheOrder = l.cacheOrder[:len(l.cacheOrder)-1]
}

func (l *segLog) admitIndex(id uint64, x *segIndex) {
	size := len(x.ids.keys) * 16
	const budget = (8 << 20) - frequencyCharge
	if size > budget {
		return
	}
	if _, ok := l.cache[id]; ok {
		return // Sealed indexes are immutable.
	}
	score := l.frequency.score(id)
	// A large candidate may need several victims. Check that it can fit
	// before displacing any resident.
	if shortage := l.cacheIDBytes + size - budget; shortage > 0 {
		for _, resident := range l.cacheOrder {
			if l.frequency.score(resident) < score {
				shortage -= len(l.cache[resident].ids.keys) * 16
				if shortage <= 0 {
					break
				}
			}
		}
		if shortage > 0 {
			return
		}
	}
	for len(l.cacheOrder) >= idxCacheSize || l.cacheIDBytes+size > budget {
		victim := 0
		least := l.frequency.score(l.cacheOrder[0])
		for i, k := range l.cacheOrder {
			v := l.frequency.score(k)
			if v < least {
				least = v
				victim = i
			}
		}
		if score <= least {
			return
		}
		l.evictIndex(victim)
	}
	l.cacheOrder = append(l.cacheOrder, id)
	l.cache[id] = x
	l.cacheIDBytes += size
}
