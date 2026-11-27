package sketch

import (
	"bytes"
	"cmp"
	"container/heap"
	"errors"
	"hash/maphash"
	"math"
	"slices"
	"time"
)

// HotTopics tracks approximate event activity with a fixed number of four-way
// buckets. Use it in a stream consumer. Concurrent calls need external locking.
type HotTopics struct {
	seed     maphash.Seed
	halfLife float64
	width    int
	buckets  []topicBucket
	names    []byte
}

type activity struct {
	score float64
	at    int64
}
type topicSlot struct {
	activity
	length int
}
type topicBucket struct {
	slots [4]topicSlot
	// A bounded probation area remembers hashes only until a repeat earns admission.
	pending [4]struct {
		activity
		hash uint64
	}
	next uint8
}

// TopicScore is a topic and its approximate exponentially decayed event count.
type TopicScore struct {
	Topic string
	Score float64
}

// NewHotTopics reserves four resident names per bucket, each at most maxNameBytes.
// The name slab and bucket array are allocated once. Oversized names are skipped.
func NewHotTopics(buckets, maxNameBytes int, halfLife time.Duration) (*HotTopics, error) {
	if buckets < 1 || buckets > 1<<20 || maxNameBytes < 1 || maxNameBytes > 4096 || buckets > math.MaxInt/4/maxNameBytes || halfLife <= 0 {
		return nil, errors.New("invalid hot-topic dimensions or half-life")
	}
	return &HotTopics{seed: maphash.MakeSeed(), halfLife: float64(halfLife), width: maxNameBytes, buckets: make([]topicBucket, buckets), names: make([]byte, buckets*4*maxNameBytes)}, nil
}
func (h *HotTopics) value(a activity, now int64) float64 {
	if a.score == 0 || now <= a.at {
		return a.score
	}
	return a.score * math.Exp2(-float64(uint64(now)-uint64(a.at))/h.halfLife)
}

func (h *HotTopics) add(a activity, now int64) activity {
	if a.score != 0 && now < a.at {
		a.score += h.value(activity{1, now}, a.at)
		return a
	}
	return activity{h.value(a, now) + 1, now}
}

func (h *HotTopics) name(bucket, slot int) []byte {
	offset := (bucket*4 + slot) * h.width
	return h.names[offset : offset+h.width]
}

// Observe records one event. False means the name is empty or exceeds the
// configured byte limit. True includes events handled in the probation area.
func (h *HotTopics) Observe(topic string, at time.Time) bool {
	if len(topic) == 0 || len(topic) > h.width {
		return false
	}
	now := at.UnixNano()
	hash := maphash.String(h.seed, topic)
	index := int(hash % uint64(len(h.buckets)))
	b := &h.buckets[index]
	victim := -1
	for i := range b.slots {
		s := &b.slots[i]
		if s.length == 0 {
			victim = i
			continue
		}
		if s.length == len(topic) && bytes.Equal(h.name(index, i)[:s.length], []byte(topic)) {
			s.activity = h.add(s.activity, now)
			return true
		}
	}
	candidate := activity{1, now}
	if victim < 0 {
		pending := -1
		for i, p := range b.pending {
			if p.score != 0 && p.hash == hash {
				pending = i
				break
			}
		}
		if pending < 0 {
			p := &b.pending[b.next%4]
			b.next++
			p.hash = hash
			p.activity = candidate
			return true
		}
		p := &b.pending[pending]
		p.activity = h.add(p.activity, now)
		when := p.at
		for _, s := range b.slots {
			when = max(when, s.at)
		}
		victim = 0
		least := h.value(b.slots[0].activity, when)
		for i := 1; i < len(b.slots); i++ {
			score := h.value(b.slots[i].activity, when)
			if score < least {
				victim = i
				least = score
			}
		}
		if h.value(p.activity, when) <= least {
			return true
		}
		candidate = p.activity
		p.score = 0
	}
	b.slots[victim] = topicSlot{candidate, len(topic)}
	copy(h.name(index, victim), topic)
	return true
}

// Ranking holds views into the name slab only for the duration of a query.
type rankedTopic struct {
	name  []byte
	score float64
}

func rankOrder(a, b rankedTopic) int {
	if c := cmp.Compare(b.score, a.score); c != 0 {
		return c
	}
	return bytes.Compare(a.name, b.name)
}

type topicHeap []rankedTopic

func (q topicHeap) Len() int           { return len(q) }
func (q topicHeap) Less(i, j int) bool { return rankOrder(q[i], q[j]) > 0 }
func (q topicHeap) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *topicHeap) Push(x any)        { *q = append(*q, x.(rankedTopic)) }
func (q *topicHeap) Pop() any          { n := len(*q) - 1; v := (*q)[n]; *q = (*q)[:n]; return v }

// Top returns up to k resident topics ordered by score, then name. Query at
// or after the latest event for a common-time ranking. It uses O(k) scratch
// space and copies only the returned names.
func (h *HotTopics) Top(k int, at time.Time) []TopicScore {
	k = min(k, 4*len(h.buckets))
	if k <= 0 {
		return nil
	}
	q := make(topicHeap, 0, k)
	for b := range h.buckets {
		for i, s := range h.buckets[b].slots {
			if s.length == 0 {
				continue
			}
			entry := rankedTopic{h.name(b, i)[:s.length], h.value(s.activity, at.UnixNano())}
			if len(q) < k {
				q = append(q, entry)
				if len(q) == k {
					heap.Init(&q)
				}
			} else if rankOrder(entry, q[0]) < 0 {
				q[0] = entry
				heap.Fix(&q, 0)
			}
		}
	}
	slices.SortFunc(q, rankOrder)
	out := make([]TopicScore, len(q))
	for i, e := range q {
		out[i] = TopicScore{string(e.name), e.score}
	}
	return out
}
