package sketch

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func key(i int) []byte { return []byte(fmt.Sprintf("key-%d", i)) }

// Four standard errors of a precision-p sketch.
func hllBound(p uint8) float64 { return 4 * 1.04 / math.Sqrt(float64(uint(1)<<p)) }

func within(t *testing.T, label string, got float64, want int, p uint8) {
	t.Helper()
	if want == 0 {
		if got != 0 {
			t.Fatalf("%s: empty sketch estimates %v", label, got)
		}
		return
	}
	if rel := math.Abs(got-float64(want)) / float64(want); rel > hllBound(p) {
		t.Fatalf("%s: estimate %.1f for %d distinct, relative error %.4f exceeds %.4f", label, got, want, rel, hllBound(p))
	}
}

func TestHyperLogLogKnownCardinalities(t *testing.T) {
	const p = 14
	for _, n := range []int{0, 1, 10, 1000, 20000, 200000} {
		h, e := NewHyperLogLog(p)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < n; i++ {
			h.Add(key(i))
		}
		within(t, fmt.Sprintf("single n=%d", n), h.Estimate(), n, p)
	}
}

func TestHyperLogLogRepeatedAndSkewedKeys(t *testing.T) {
	const p = 14
	once, _ := NewHyperLogLog(p)
	skew, _ := NewHyperLogLog(p)
	const distinct = 5000
	for i := 0; i < distinct; i++ {
		once.Add(key(i))
	}
	rng := rand.New(rand.NewSource(7))
	// 90% of the traffic lands on ten hot keys; every key is still seen at least once.
	for i := 0; i < distinct; i++ {
		skew.Add(key(i))
	}
	for i := 0; i < 200000; i++ {
		if rng.Intn(10) < 9 {
			skew.Add(key(rng.Intn(10)))
		} else {
			skew.Add(key(rng.Intn(distinct)))
		}
	}
	a, _ := once.MarshalBinary()
	b, _ := skew.MarshalBinary()
	if !bytes.Equal(a, b) {
		t.Fatal("repeating keys changed the sketch")
	}
	within(t, "skewed", skew.Estimate(), distinct, p)
}

func TestHyperLogLogMergedWorkers(t *testing.T) {
	const p = 14
	const workers, per, stride = 4, 30000, 20000 // ranges overlap by a third
	merged, _ := NewHyperLogLog(p)
	whole, _ := NewHyperLogLog(p)
	seen := map[int]bool{}
	for w := 0; w < workers; w++ {
		h, _ := NewHyperLogLog(p)
		for i := w * stride; i < w*stride+per; i++ {
			h.Add(key(i))
			whole.Add(key(i))
			seen[i] = true
		}
		// Round trip each worker as it would be shipped.
		raw, _ := h.MarshalBinary()
		var back HyperLogLog
		if e := back.UnmarshalBinary(raw); e != nil {
			t.Fatal(e)
		}
		if e := merged.Merge(&back); e != nil {
			t.Fatal(e)
		}
	}
	a, _ := merged.MarshalBinary()
	b, _ := whole.MarshalBinary()
	if !bytes.Equal(a, b) {
		t.Fatal("merge of workers differs from one sketch fed everything")
	}
	within(t, "merged", merged.Estimate(), len(seen), p)
	other, _ := NewHyperLogLog(p - 1)
	if merged.Merge(other) == nil || merged.Merge(nil) == nil {
		t.Fatal("incompatible or nil merge accepted")
	}
}

func TestHyperLogLogLowPrecisionBound(t *testing.T) {
	const p = 8
	h, _ := NewHyperLogLog(p)
	for i := 0; i < 50000; i++ {
		h.Add(key(i))
	}
	within(t, "p=8", h.Estimate(), 50000, p)
}

// exactNearestRank is the p-th percentile by the nearest-rank method on sorted samples.
func exactNearestRank(sorted []time.Duration, p int) time.Duration {
	rank := (len(sorted)*p + 99) / 100
	return sorted[rank-1]
}

func checkAgainstExact(t *testing.T, label string, samples []time.Duration) {
	t.Helper()
	var h LatencyHistogram
	for _, s := range samples {
		if !h.Observe(s) {
			t.Fatalf("%s: sample %v rejected", label, s)
		}
	}
	if h.Count() != uint64(len(samples)) {
		t.Fatalf("%s: count %d want %d", label, h.Count(), len(samples))
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, p := range []int{1, 50, 90, 95, 99, 100} {
		want := exactNearestRank(sorted, p)
		got, ok := h.PercentileUpper(p)
		if !ok {
			t.Fatalf("%s p%d: no answer", label, p)
		}
		if got < want {
			t.Fatalf("%s p%d: upper bound %v is below exact %v", label, p, got, want)
		}
		if want < 32 {
			if got != want {
				t.Fatalf("%s p%d: %v is not exact for %v", label, p, got, want)
			}
		} else if uint64(got-want)*16 >= uint64(want) {
			t.Fatalf("%s p%d: %v exceeds %v by 6.25%% or more", label, p, got, want)
		}
	}
}

func TestLatencyHistogramZeroAndEmpty(t *testing.T) {
	var h LatencyHistogram
	if _, ok := h.PercentileUpper(95); ok {
		t.Fatal("empty histogram answered")
	}
	checkAgainstExact(t, "all zero", make([]time.Duration, 100))
	checkAgainstExact(t, "single", []time.Duration{time.Millisecond})
	for _, p := range []int{0, 101, -1} {
		h.Observe(1)
		if _, ok := h.PercentileUpper(p); ok {
			t.Fatalf("percentile %d accepted", p)
		}
	}
}

func TestLatencyHistogramBucketBoundaries(t *testing.T) {
	var samples []time.Duration
	for _, v := range []int64{0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65} {
		samples = append(samples, time.Duration(v))
	}
	for k := 5; k < 62; k++ {
		base := int64(1) << k
		samples = append(samples, time.Duration(base-1), time.Duration(base), time.Duration(base+1), time.Duration(base+base/16-1), time.Duration(base+base/16))
	}
	checkAgainstExact(t, "boundaries", samples)
	for _, s := range samples {
		checkAgainstExact(t, fmt.Sprintf("only %d", s), []time.Duration{s})
	}
	var h LatencyHistogram
	if !h.Observe(time.Duration(math.MaxInt64)) {
		t.Fatal("largest duration rejected")
	}
	if got, ok := h.PercentileUpper(100); !ok || got != time.Duration(math.MaxInt64) {
		t.Fatal(got, ok)
	}
	if h.Observe(-1) || h.Count() != 1 {
		t.Fatal("negative sample counted")
	}
}

func TestLatencyHistogramLongTail(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	var samples []time.Duration
	for i := 0; i < 989; i++ {
		samples = append(samples, time.Duration(800+rng.Intn(4200))*time.Microsecond)
	}
	for i := 0; i < 11; i++ { // 1.1% outliers: p99 must reach the tail, p95 must not
		samples = append(samples, time.Duration(1+rng.Intn(3600))*time.Second)
	}
	checkAgainstExact(t, "long tail", samples)
	var h LatencyHistogram
	for _, s := range samples {
		h.Observe(s)
	}
	p95, _ := h.PercentileUpper(95)
	p99, _ := h.PercentileUpper(99)
	if p95 > 6*time.Millisecond || p99 < time.Second {
		t.Fatal("tail not separated", p95, p99)
	}
}

func TestLatencyHistogramMergedCountsEveryBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	counts := []int{7, 11, 13, 0, 1000}
	merged := &LatencyHistogram{}
	var all []time.Duration
	total := 0
	for _, c := range counts {
		var w LatencyHistogram
		for i := 0; i < c; i++ {
			d := time.Duration(rng.Intn(50_000_000))
			w.Observe(d)
			all = append(all, d)
		}
		if !merged.Merge(&w) {
			t.Fatal("merge refused")
		}
		total += c
	}
	if merged.Count() != uint64(total) || total != len(all) {
		t.Fatalf("merged %d samples, %d acknowledged", merged.Count(), total)
	}
	if merged.Merge(nil) || merged.Count() != uint64(total) {
		t.Fatal("nil merge changed the histogram")
	}
	checkAgainstExact(t, "merged workers", all)
}
