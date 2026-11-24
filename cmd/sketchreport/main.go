// Command sketchreport measures the accuracy of the sketch package against
// exact values on fixed inputs and prints one receipt line per measurement.
// The output is deterministic (fixed seeds). Redirect it to
// docs/sketch-accuracy-results.txt; `make plots` charts it.
package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/rodriveracom/dostoevsky/sketch"
)

func main() {
	for _, p := range []uint8{10, 12, 14} {
		for _, n := range []int{1000, 100000} {
			h, err := sketch.NewHyperLogLog(p)
			if err != nil {
				panic(err)
			}
			for i := 0; i < n; i++ {
				h.Add([]byte(fmt.Sprintf("key-%d", i)))
			}
			enc, err := h.MarshalBinary()
			if err != nil {
				panic(err)
			}
			est := h.Estimate()
			fmt.Printf("kind=hll precision=%d n=%d exact=%d estimate=%.1f rel_err_pct=%.3f bytes=%d\n",
				p, n, n, est, 100*math.Abs(est-float64(n))/float64(n), len(enc))
		}
	}
	for _, shape := range []string{"lognormal", "bimodal"} {
		rng := rand.New(rand.NewSource(1))
		const count = 200000
		samples := make([]time.Duration, count)
		h := &sketch.LatencyHistogram{}
		for i := range samples {
			var us float64
			if shape == "lognormal" {
				us = math.Exp(rng.NormFloat64()*0.8 + 6)
			} else if rng.Intn(10) == 0 {
				us = 20000 + rng.Float64()*5000
			} else {
				us = 300 + rng.Float64()*200
			}
			samples[i] = time.Duration(us * float64(time.Microsecond))
			h.Observe(samples[i])
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		for _, pct := range []int{50, 99} {
			exact := samples[(pct*count+99)/100-1] // nearest rank
			upper, ok := h.PercentileUpper(pct)
			if !ok {
				panic("empty histogram")
			}
			fmt.Printf("kind=latency shape=%s percentile=%d n=%d exact_us=%.1f upper_us=%.1f rel_err_pct=%.3f bytes=7688\n",
				shape, pct, count, float64(exact)/1e3, float64(upper)/1e3, 100*(float64(upper)-float64(exact))/float64(exact))
		}
	}
}
