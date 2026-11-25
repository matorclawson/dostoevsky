// Package hist is the latency histogram every benchmark in this repository reports its
// percentiles from: 1% wide bins from 1 microsecond up, 2400 of them, the last taking
// everything above. A quantile is the UPPER EDGE of the bin that holds it, in whole
// microseconds, so it is accurate to 1% and never below the true value. The Python and
// JavaScript workers implement the same bins and are checked against the same file,
// benchmarks/clients/hist_vector.json, so a number means the same thing in every language.
package hist

import (
	"math"
	"time"
)

// Bins is the number of bins.
const Bins = 2400

var logBase = math.Log(1.01)

// H is a histogram. The zero value is empty and ready to use.
type H struct{ n [Bins]uint64 }

// Index returns the bin a duration falls in.
func Index(d time.Duration) int {
	us := float64(d) / 1e3
	if us < 1 {
		return 0
	}
	return min(int(math.Log(us)/logBase), Bins-1)
}

// Add records one duration.
func (h *H) Add(d time.Duration) { h.n[Index(d)]++ }

// Total is the number of durations recorded.
func (h *H) Total() uint64 {
	var t uint64
	for _, c := range h.n {
		t += c
	}
	return t
}

// Quantile returns the upper edge of the bin holding the q-th quantile, or 0 when empty. The
// rule, spelled out so the other languages can copy it: rank = max(ceil(q*total), 1); walk the
// bins in order until the running count reaches the rank; the answer is
// trunc(1.01^(bin+1) * 1000 ns), which callers print in whole microseconds.
func (h *H) Quantile(q float64) time.Duration {
	total := h.Total()
	if total == 0 {
		return 0
	}
	rank := max(uint64(math.Ceil(q*float64(total))), 1)
	var seen uint64
	for i, c := range h.n {
		if seen += c; seen >= rank {
			return time.Duration(math.Pow(1.01, float64(i+1)) * 1e3)
		}
	}
	return 0
}
