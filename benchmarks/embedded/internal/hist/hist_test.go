package hist

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

// The vector is shared with the Python and JavaScript workers (benchmarks/clients/hist_vector.json):
// the same durations must give the same bins and the same percentiles in every language.
type vector struct {
	Bins        int              `json:"bins"`
	DurationsNs []int64          `json:"durations_ns"`
	Index       map[string]int   `json:"index"`
	QuantilesNs map[string]int64 `json:"quantiles_ns"`
}

func load(t *testing.T) vector {
	t.Helper()
	b, err := os.ReadFile("../../../clients/hist_vector.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vector
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBinsAndQuantilesMatchTheSharedVector(t *testing.T) {
	v := load(t)
	if v.Bins != Bins {
		t.Fatalf("vector has %d bins, Go has %d", v.Bins, Bins)
	}
	if len(v.Index) == 0 || len(v.QuantilesNs) == 0 || len(v.DurationsNs) == 0 {
		t.Fatal("the vector is empty: the test would prove nothing")
	}
	for ns, want := range v.Index {
		n, err := strconv.ParseInt(ns, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if got := Index(time.Duration(n)); got != want {
			t.Errorf("Index(%s ns) = %d, vector says %d", ns, got, want)
		}
	}
	var h H
	for _, ns := range v.DurationsNs {
		h.Add(time.Duration(ns))
	}
	for q, want := range v.QuantilesNs {
		f, err := strconv.ParseFloat(q, 64)
		if err != nil {
			t.Fatal(err)
		}
		if got := h.Quantile(f).Nanoseconds(); got != want {
			t.Errorf("Quantile(%s) = %d ns, vector says %d", q, got, want)
		}
	}
}

func TestEmptyAndSingleValueHistograms(t *testing.T) {
	var h H
	if h.Quantile(0.99) != 0 {
		t.Fatal("an empty histogram has a quantile")
	}
	h.Add(1500 * time.Microsecond)
	lo, hi := h.Quantile(0.01), h.Quantile(1)
	if lo != hi || hi < 1500*time.Microsecond || hi > time.Duration(float64(1500*time.Microsecond)*1.0101) {
		t.Fatalf("one value of 1.5 ms reported as %v and %v: must be its bin's upper edge, within 1%%", lo, hi)
	}
}

// A short sample keeps its sub-microsecond precision: 1.5 us must not be reported as "1 us".
func TestAShortSampleIsNotTruncatedToWholeMicroseconds(t *testing.T) {
	var h H
	h.Add(1900 * time.Nanosecond)
	got := h.Quantile(0.5)
	if got < 1900*time.Nanosecond || got > time.Duration(float64(1900*time.Nanosecond)*1.0101) {
		t.Fatalf("1900 ns reported as %v: must be its bin's upper edge, never below the sample, within 1%%", got)
	}
}
