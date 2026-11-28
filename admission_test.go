package dostoevsky

import "testing"

func TestFrequencyAdmission(t *testing.T) {
	l := &segLog{cache: map[uint64]*segIndex{}, frequency: new(indexFrequency)}
	x := newSegIndex(nil)
	for i := uint64(0); i < idxCacheSize; i++ {
		l.cachePut(i, x)
	}
	for i := 0; i < 100; i++ {
		l.frequency.observe(0)
	}
	for i := uint64(32); i < 96; i++ {
		l.frequency.observe(i)
		l.cachePut(i, x)
	}
	if len(l.cache) != idxCacheSize || l.cache[0] == nil {
		t.Fatal("failed to protect reused index")
	}
	for i := 0; i < 8192; i++ {
		l.frequency.observe(96)
	}
	l.cachePut(96, x)
	if l.cache[96] == nil || l.frequency.score(0) >= l.frequency.score(96) {
		t.Fatal("failed to adapt")
	}
}

func TestAdmissionAccountsForSketch(t *testing.T) {
	l := &segLog{cache: map[uint64]*segIndex{}}
	n := &Node{segments: l}
	x := newSegIndex(nil)
	x.ids.keys = make([]uint64, (8<<20)/16)
	l.cachePut(1, x)
	if !n.EnableIndexAdmission() || len(l.cache) != 0 {
		t.Fatal("did not reserve the sketch budget")
	}
	l.cachePut(1, x)
	if len(l.cache) != 0 || l.cacheIDBytes != 0 {
		t.Fatal("oversize index retained")
	}
}

func TestRejectedAdmissionPreservesResidents(t *testing.T) {
	l := &segLog{cache: map[uint64]*segIndex{}, frequency: new(indexFrequency)}
	table := func(mib int) *segIndex { x := newSegIndex(nil); x.ids.keys = make([]uint64, (mib<<20)/16); return x }
	l.cachePut(1, table(1))
	l.cachePut(2, table(5))
	for i := 0; i < 10; i++ {
		l.frequency.observe(2)
	}
	l.frequency.observe(3)
	l.cachePut(3, table(4))
	if len(l.cache) != 2 || l.cache[1] == nil || l.cache[2] == nil {
		t.Fatal("partial eviction for rejected candidate")
	}
}
