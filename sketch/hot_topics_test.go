package sketch

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"
)

func TestHotTopics(t *testing.T) {
	h, err := NewHotTopics(1, 4, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(5, 0)
	for _, s := range []string{"a", "b", "c", "d", "e", "e"} {
		h.Observe(s, now)
	}
	h.Observe("e", now.Add(-time.Second))
	got := h.Top(1, now.Add(time.Second))
	if len(got) != 1 || got[0].Topic != "e" || math.Abs(got[0].Score-1.25) > 1e-9 {
		t.Fatal(got)
	}
	if len(h.Top(10, now)) != 4 || h.Observe("large", now) || h.Observe("", now) {
		t.Fatal("bounds")
	}
	if a := testing.AllocsPerRun(1000, func() { h.Observe("e", now) }); a != 0 {
		t.Fatal(a)
	}
}

func TestHotTopicRecall(t *testing.T) {
	h, _ := NewHotTopics(128, 24, 100*time.Millisecond)
	exact := map[string]activity{}
	now := time.Unix(0, 0)
	var hits int
	for phase := 0; phase < 2; phase++ {
		for i := 0; i < 50000; i++ {
			now = now.Add(10 * time.Microsecond)
			key := fmt.Sprintf("cold-%d-%d", phase, i)
			if i%2 == 0 {
				key = fmt.Sprintf("popular-%d-%d", phase, (i/2)%10)
			}
			h.Observe(key, now)
			exact[key] = h.add(exact[key], now.UnixNano())
		}
		var oracle []TopicScore
		for k, v := range exact {
			oracle = append(oracle, TopicScore{k, h.value(v, now.UnixNano())})
		}
		sort.Slice(oracle, func(i, j int) bool { return oracle[i].Score > oracle[j].Score })
		want := map[string]bool{}
		for _, v := range oracle[:10] {
			want[v.Topic] = true
		}
		for _, v := range h.Top(10, now) {
			if want[v.Topic] {
				hits++
			}
		}
	}
	t.Logf("top-10 across two phases: %d/20", hits)
}

func BenchmarkHotTopics(b *testing.B) {
	h, _ := NewHotTopics(128, 24, time.Second)
	keys := make([]string, 2048)
	for i := range keys {
		keys[i] = fmt.Sprint(i)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Observe(keys[i%len(keys)], time.Unix(0, int64(i)*1000))
	}
}
func BenchmarkHotTopicsConstruct(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := NewHotTopics(128, 24, time.Second)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestHotTopicsMillionNames(t *testing.T) {
	h, _ := NewHotTopics(128, 24, time.Second)
	now := time.Unix(0, 0)
	for i := 0; i < 1_000_000; i++ {
		now = now.Add(60 * time.Microsecond)
		h.Observe(fmt.Sprintf("cold-%d", i), now)
		if i%10 == 0 {
			h.Observe("regular", now)
		}
	}
	got := h.Top(1, now)
	if len(got) != 1 || got[0].Topic != "regular" {
		t.Fatal(got)
	}
	t.Logf("1,000,000 distinct cold names over 60 seconds of event time; top=%s score=%.2f", got[0].Topic, got[0].Score)
}
