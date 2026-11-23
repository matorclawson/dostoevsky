// Command burst measures durable ingest and verifies each publication after reopening.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/sketch"
)

func main() {
	count := flag.Int("count", 100000, "number of records")
	width := flag.Int("batch", 128, "records per batch (1 uses Publish)")
	topics := flag.Int("topics", 1, "distinct topic names")
	workers := flag.Int("workers", 4, "producer goroutines")
	pace := flag.Duration("duration", 0, "spread scheduled batch arrivals over this duration")
	consumers := flag.Int("groups", 0, "independent live consumer groups (single-topic workload)")
	flag.Parse()
	if *consumers < 0 || *consumers > 16 || (*consumers > 0 && *topics != 1) || *pace < 0 || *count < 1 || *width < 1 || *width > 1024 || *topics < 1 || *topics > *count || *workers < 1 {
		log.Fatal("invalid workload")
	}
	dir, e := os.MkdirTemp("", "dost-burst")
	if e != nil {
		log.Fatal(e)
	}
	defer os.RemoveAll(dir)
	cfg := d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"*": 1}}
	n, e := d.Open(dir, cfg)
	if e != nil {
		log.Fatal(e)
	}
	ctx := context.Background()
	payload := make([]byte, 128)
	batchCount := 1 + (*count-1) / *width
	latency := make([]sketch.LatencyHistogram, min(*workers, batchCount))
	var next atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	var readers sync.WaitGroup
	for group := 0; group < *consumers; group++ {
		readers.Add(1)
		go func(g int) {
			defer readers.Done()
			p := d.Partition{Topic: "t0"}
			id := "g" + strconv.Itoa(g)
			var cursor int64
			for cursor < int64(*count) {
				page, e := n.ReadBatch(ctx, p, cursor, d.Limits{Records: 128, Bytes: 1 << 20}, true)
				if e != nil {
					log.Fatal(e)
				}
				if e = n.CommitBatch(ctx, p, id, cursor, page.Next, nil); e != nil {
					log.Fatal(e)
				}
				cursor = page.Next
			}
		}(group)
	}

	for w := range latency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				first := int(next.Add(int64(*width))) - *width
				if first >= *count {
					return
				}
				due := time.Now()
				if *pace > 0 {
					due = start.Add(time.Duration(float64(*pace) * float64(first) / float64(*count)))
					time.Sleep(time.Until(due))
				}
				batch := make([]d.Publication, 0, min(*width, *count-first))
				for i := first; i < min(first+*width, *count); i++ {
					id := strconv.Itoa(i)
					batch = append(batch, d.Publication{Topic: "t" + strconv.Itoa(i%*topics), Message: d.Message{ID: id, Key: id, Value: payload}})
				}
				if *width == 1 {
					_, e := n.Publish(ctx, batch[0].Topic, batch[0].Message)
					if e != nil {
						log.Fatal(e)
					}
				} else {
					if _, e := n.PublishBatch(ctx, batch); e != nil {
						log.Fatal(e)
					}
				}
				latency[w].Observe(time.Since(due))
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	readers.Wait()
	if *consumers > 0 {
		fmt.Printf("groups=%d all checkpoints complete after %s\n", *consumers, time.Since(start))
	}
	var total sketch.LatencyHistogram
	for i := range latency {
		total.Merge(&latency[i])
	}
	if total.Count() != uint64(batchCount) {
		log.Fatal("latency sample count mismatch")
	}
	p95, _ := total.PercentileUpper(95)
	p99, _ := total.PercentileUpper(99)
	fmt.Printf("batches=%d scheduled-to-ack p95<=%s p99<=%s\n", total.Count(), p95, p99)
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	info, e := os.Stat(dir + "/journal.v1")
	if e != nil {
		log.Fatal(e)
	}
	fmt.Printf("count=%d topics=%d batch=%d workers=%d ingest=%s records/s=%.0f liveHeapMiB=%.1f journalMiB=%.1f\n", *count, *topics, *width, *workers, elapsed, float64(*count)/elapsed.Seconds(), float64(stats.HeapAlloc)/(1<<20), float64(info.Size())/(1<<20))
	if e = n.Close(); e != nil {
		log.Fatal(e)
	}
	n = nil
	runtime.GC()
	start = time.Now()
	n, e = d.Open(dir, cfg)
	if e != nil {
		log.Fatal(e)
	}
	defer n.Close()
	fmt.Println("reopen", time.Since(start))
	for g := 0; g < *consumers; g++ {
		pos, e := n.Position(ctx, d.Partition{Topic: "t0"}, "g"+strconv.Itoa(g))
		if e != nil || pos.Next != int64(*count) {
			log.Fatal("checkpoint mismatch")
		}
	}
	seen := make([]bool, *count)
	for t := 0; t < *topics; t++ {
		p := d.Partition{Topic: "t" + strconv.Itoa(t)}
		for offset := 0; offset < 1 + (*count-1-t) / *topics; offset++ {
			r, e := n.Read(ctx, p, int64(offset), false)
			if e != nil {
				log.Fatal(e)
			}
			i, e := strconv.Atoi(r.ID)
			if e != nil || i < 0 || i >= *count || seen[i] || i%*topics != t || r.Offset != int64(offset) || r.Key != r.ID || !bytes.Equal(r.Value, payload) {
				log.Fatal("record mismatch")
			}
			seen[i] = true
		}
		if _, e := n.Read(ctx, p, int64(1 + (*count-1-t) / *topics), false); e != d.ErrEmpty {
			log.Fatal("unexpected tail", e)
		}
	}
	fmt.Println("verified", *count)
}
