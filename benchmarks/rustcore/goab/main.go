// A/B harness, Go side: the engine's public API under the same load, arguments and output as
// benchmarks/rustcore/dostoevsky-core/src/bin/rsab.rs.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rodriveracom/dostoevsky"
)

const topic = "events"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func statusKB(key string) uint64 {
	b, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, key) {
			f := strings.Fields(line)
			v, _ := strconv.ParseUint(f[1], 10, 64)
			return v
		}
	}
	return 0
}

const buckets = 64 + 58*32

func bucket(v uint64) int {
	if v < 64 {
		return int(v)
	}
	e := 63 - bits.LeadingZeros64(v)
	return 64 + (e-6)*32 + int((v>>(e-5))&31)
}

func bucketUpper(i int) uint64 {
	if i < 64 {
		return uint64(i)
	}
	e, sub := (i-64)/32+6, uint64((i-64)%32)
	return (32+sub+1)<<(e-5) - 1
}

func percentile(h []uint64, total uint64, q float64) uint64 {
	want := uint64(float64(total)*q + 0.999999)
	if want < 1 {
		want = 1
	}
	var seen uint64
	for i, c := range h {
		seen += c
		if seen >= want {
			return bucketUpper(i)
		}
	}
	return 0
}

func median(v []uint64) uint64 {
	if len(v) == 0 {
		return 0
	}
	v = slices.Clone(v)
	slices.Sort(v)
	return v[len(v)/2]
}

func value(buf []byte, seq uint64, payload int) []byte {
	if payload < 8 {
		payload = 8
	}
	buf = buf[:0]
	buf = binary.LittleEndian.AppendUint64(buf, seq)
	for len(buf) < payload {
		buf = append(buf, 'x')
	}
	return buf
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	born := time.Now()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: goab write|fill|reopen|read|digest --dir D [...]")
		os.Exit(2)
	}
	mode := os.Args[1]
	opt := map[string]string{}
	for i := 2; i+1 < len(os.Args); i += 2 {
		opt[strings.TrimLeft(os.Args[i], "-")] = os.Args[i+1]
	}
	num := func(k string, d uint64) uint64 {
		if v, ok := opt[k]; ok {
			f, err := strconv.ParseFloat(v, 64)
			must(err)
			return uint64(f)
		}
		return d
	}
	payload := int(num("payload", 100))
	ctx := context.Background()
	openStart := time.Now()
	node, err := dostoevsky.OpenSegmented(opt["dir"], dostoevsky.Config{Node: "n1", Nodes: []string{"n1"}, Topics: map[string]int{topic: 1}})
	must(err)
	openNS := time.Since(openStart).Nanoseconds()
	rssReady := statusKB("VmRSS:")
	part := dostoevsky.Partition{Topic: topic, Number: 0}
	switch mode {
	case "write":
		producers, seconds := int(num("producers", 1)), int(num("seconds", 10))
		var acked, lastAck atomic.Uint64
		var stop atomic.Bool
		type result struct {
			hist                   []uint64
			n, max, maxGap, gaps10 uint64
			over                   [3]uint64
		}
		results := make([]result, producers)
		start := time.Now()
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				r := result{hist: make([]uint64, buckets)}
				var buf []byte
				for !stop.Load() {
					buf = value(buf, uint64(p)<<40|r.n, payload)
					t0 := time.Now()
					_, err := node.Publish(ctx, topic, dostoevsky.Message{Value: buf})
					must(err)
					lat := uint64(time.Since(t0).Nanoseconds())
					now := uint64(time.Since(start).Nanoseconds())
					prev := lastAck.Swap(now)
					if now > prev {
						gap := now - prev
						r.maxGap = max(r.maxGap, gap)
						if gap > 10_000_000 {
							r.gaps10++
						}
					}
					r.hist[bucket(lat)]++
					r.max = max(r.max, lat)
					if lat > 10_000_000 {
						r.over[0]++
					}
					if lat > 50_000_000 {
						r.over[1]++
					}
					if lat > 100_000_000 {
						r.over[2]++
					}
					r.n++
					acked.Add(1)
				}
				results[p] = r
			}(p)
		}
		var perSecond, rss []uint64
		var prev uint64
		for s := 1; s <= seconds; s++ {
			time.Sleep(time.Until(start.Add(time.Duration(s) * time.Second)))
			a := acked.Load()
			perSecond = append(perSecond, a-prev)
			prev = a
			rss = append(rss, statusKB("VmRSS:"))
		}
		stop.Store(true)
		wg.Wait()
		elapsed := time.Since(start).Seconds()
		hist := make([]uint64, buckets)
		var total, maxLat, maxGap, gaps10 uint64
		var over [3]uint64
		for _, r := range results {
			for i, c := range r.hist {
				hist[i] += c
			}
			total += r.n
			maxLat, maxGap, gaps10 = max(maxLat, r.max), max(maxGap, r.maxGap), gaps10+r.gaps10
			for k := range over {
				over[k] += r.over[k]
			}
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		var maxPause uint64
		for _, p := range ms.PauseNs {
			maxPause = max(maxPause, p)
		}
		fmt.Printf(`{"engine":"go","mode":"write","producers":%d,"payload":%d,"seconds":%.3f,"acked":%d,"acked_per_s":%.1f,"p50_us":%.1f,"p99_us":%.1f,"p999_us":%.1f,"max_us":%.1f,"over_10ms":%d,"over_50ms":%d,"over_100ms":%d,"max_ack_gap_ms":%.2f,"ack_gaps_over_10ms":%d,"second_min":%d,"second_median":%d,"second_max":%d,"open_ms":%.3f,"rss_ready_kb":%d,"rss_steady_kb":%d,"rss_first_kb":%d,"rss_last_kb":%d,"rss_peak_kb":%d,"gc_cycles":%d,"gc_pause_total_ms":%.3f,"gc_pause_max_recent_us":%.1f,"heap_alloc_kb":%d,"journal_bytes":%d}`+"\n",
			producers, payload, elapsed, total, float64(total)/elapsed,
			float64(percentile(hist, total, 0.50))/1e3, float64(percentile(hist, total, 0.99))/1e3, float64(percentile(hist, total, 0.999))/1e3, float64(maxLat)/1e3,
			over[0], over[1], over[2], float64(maxGap)/1e6, gaps10,
			slices.Min(perSecond), median(perSecond), slices.Max(perSecond), float64(openNS)/1e6,
			rssReady, median(rss[len(rss)*2/3:]), rss[0], rss[len(rss)-1], statusKB("VmHWM:"),
			ms.NumGC, float64(ms.PauseTotalNs)/1e6, float64(maxPause)/1e3, ms.HeapAlloc/1024, node.JournalSize())
	case "fill":
		records, batch := num("records", 1_000_000), num("batch", 1000)
		start := time.Now()
		items := make([]dostoevsky.Publication, batch)
		bufs := make([][]byte, batch)
		for seq := uint64(0); seq < records; {
			k := min(batch, records-seq)
			for j := uint64(0); j < k; j++ {
				bufs[j] = value(bufs[j], seq+j, payload)
				items[j] = dostoevsky.Publication{Topic: topic, Message: dostoevsky.Message{Value: bufs[j]}}
			}
			_, err := node.PublishBatch(ctx, items[:k])
			must(err)
			seq += k
		}
		el := time.Since(start).Seconds()
		fmt.Printf(`{"engine":"go","mode":"fill","records":%d,"payload":%d,"batch":%d,"seconds":%.3f,"records_per_s":%.0f,"rss_peak_kb":%d}`+"\n", records, payload, batch, el, float64(records)/el, statusKB("VmHWM:"))
	case "reopen":
		offset := int64(num("offset", 0))
		r, err := node.Read(ctx, part, offset, false)
		must(err)
		fetchNS := time.Since(born).Nanoseconds()
		if r.Offset != offset {
			must(errors.New("wrong record"))
		}
		publishUS := -1.0
		if num("publish", 0) == 1 {
			t0 := time.Now()
			_, err := node.Publish(ctx, topic, dostoevsky.Message{Value: value(nil, 0, payload)})
			must(err)
			publishUS = float64(time.Since(t0).Nanoseconds()) / 1e3
		}
		fmt.Printf(`{"engine":"go","mode":"reopen","offset":%d,"open_ms":%.3f,"start_to_first_fetch_ms":%.3f,"first_publish_us":%.1f,"rss_kb":%d,"truncated_bytes":%d}`+"\n",
			offset, float64(openNS)/1e6, float64(fetchNS)/1e6, publishUS, statusKB("VmRSS:"), node.Truncated().Bytes)
	case "read", "digest":
		start := time.Now()
		next := int64(num("from", 0))
		var count, bytes uint64
		var digest uint32
		var scratch [8]byte
		for {
			page, err := node.ReadBatch(ctx, part, next, dostoevsky.Limits{Records: 1000, Bytes: 64 << 20}, false)
			if errors.Is(err, dostoevsky.ErrEmpty) {
				break
			}
			must(err)
			for _, r := range page.Records {
				count++
				bytes += uint64(len(r.Key) + len(r.Value))
				if mode == "digest" {
					binary.LittleEndian.PutUint64(scratch[:], uint64(r.Offset))
					digest = crc32.Update(digest, castagnoli, scratch[:])
					binary.LittleEndian.PutUint64(scratch[:], uint64(r.Time.UnixNano()))
					digest = crc32.Update(digest, castagnoli, scratch[:])
					binary.LittleEndian.PutUint32(scratch[:4], uint32(len(r.Key)))
					digest = crc32.Update(digest, castagnoli, scratch[:4])
					digest = crc32.Update(digest, castagnoli, []byte(r.Key))
					digest = crc32.Update(digest, castagnoli, r.Value)
				}
			}
			next = page.Next
		}
		el := time.Since(start).Seconds()
		fmt.Printf(`{"engine":"go","mode":"%s","records":%d,"bytes":%d,"digest":"%08x","seconds":%.3f,"records_per_s":%.0f,"mib_per_s":%.1f,"rss_peak_kb":%d,"truncated_bytes":%d}`+"\n",
			mode, count, bytes, digest, el, float64(count)/el, float64(bytes)/el/1048576, statusKB("VmHWM:"), node.Truncated().Bytes)
	default:
		os.Exit(2)
	}
	must(node.Close())
}
