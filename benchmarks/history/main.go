// Command history measures one Linux process opening or extending a durable journal.
// Run each open in a fresh process against the same fixture. OS caches are not dropped.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

var valueBytes = flag.Int("value-bytes", 512, "payload size from 8 to 512 bytes")

func value(i int) []byte {
	var v [512]byte
	binary.LittleEndian.PutUint64(v[:], uint64(i))
	if *valueBytes == 8 {
		return v[:8]
	}
	seed := sha256.Sum256(v[:8])
	for j := 8; j < len(v); j++ {
		v[j] = seed[j%len(seed)]
	}
	return v[:*valueBytes]
}
func rss(key string) uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, key+":") {
			n, err := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			if err != nil {
				panic(err)
			}
			return n
		}
	}
	panic(key)
}
func main() {
	dir := flag.String("dir", "", "disposable journal directory")
	mode := flag.String("mode", "open", "seed, open, pages, verify, or random")
	count := flag.Int("n", 16000, "total retained records")
	samples := flag.Int("samples", 100000, "number of deterministic random reads in random mode")
	start := flag.Int("start", 0, "first offset when extending a fixture")
	hexIDs := flag.Bool("hex-ids", false, "use unique 32-character producer IDs")
	ids := flag.Bool("ids", false, "write and check unique producer IDs")
	segmented := flag.Bool("segmented", false, "create a segmented store in a new directory")
	flag.Parse()
	if *valueBytes < 8 || *valueBytes > 512 || *dir == "" || *count < 1 || *start < 0 || *start >= *count {
		panic("invalid arguments")
	}
	t := time.Now()
	open := d.Open
	if *segmented {
		open = d.OpenSegmented
	}
	n, e := open(*dir, d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
	if e != nil {
		panic(e)
	}
	opened := time.Since(t)
	initialRSS := rss("VmRSS")
	ctx := context.Background()
	p := d.Partition{Topic: "events"}
	id := func(i int) string {
		if *hexIDs {
			var raw [8]byte
			binary.BigEndian.PutUint64(raw[:], uint64(i))
			digest := sha256.Sum256(raw[:])
			return hex.EncodeToString(digest[:16])
		}
		if *ids {
			return strconv.Itoa(i)
		}
		return ""
	}
	verify := func(i int) {
		r, err := n.Read(ctx, p, int64(i), false)
		if err != nil || r.Offset != int64(i) || r.ID != id(i) || !bytes.Equal(r.Value, value(i)) {
			panic(fmt.Sprintf("verification failed at %d: %v", i, err))
		}
	}
	var readTime time.Duration
	switch *mode {
	case "seed", "append-live":
		for i := *start; i < *count; {
			batch := make([]d.Publication, 0, min(256, *count-i))
			for len(batch) < cap(batch) {
				k := i + len(batch)
				batch = append(batch, d.Publication{Topic: "events", Message: d.Message{ID: id(k), Value: value(k)}})
			}
			rec, err := n.PublishBatch(ctx, batch)
			if err != nil {
				panic(err)
			}
			if len(rec) != len(batch) {
				panic("short ack")
			}
			for k, r := range rec {
				if r.Offset != int64(i+k) {
					panic("wrong ack")
				}
			}
			i += len(batch)
			if *mode == "append-live" {
				fmt.Printf("ack_next=%d\n", i)
			}
		}
	case "scrub":
		if err := n.VerifySegments(ctx); err != nil {
			panic(err)
		}
	case "pages", "acknowledged":
		for i := *start; i < *count; {
			page, err := n.ReadBatch(ctx, p, int64(i), d.Limits{Records: min(256, *count-i), Bytes: 1 << 20}, false)
			if err != nil || len(page.Records) == 0 {
				panic(fmt.Sprintf("page at %d: %v", i, err))
			}
			for _, r := range page.Records {
				if r.Offset != int64(i) || r.ID != id(i) || !bytes.Equal(r.Value, value(i)) {
					panic(fmt.Sprintf("wrong record at %d", i))
				}
				i++
			}
		}
	case "verify":
		for i := 0; i < *count; i++ {
			verify(i)
		}
	case "random":
		if *samples < 1 {
			panic("samples must be positive")
		}
		rng := rand.New(rand.NewSource(1))
		begin := time.Now()
		for i := 0; i < *samples; i++ {
			verify(rng.Intn(*count))
		}
		readTime = time.Since(begin)
	case "open":
	default:
		panic("unknown mode")
	}
	if _, err := n.Read(ctx, p, int64(*count-1), false); err != nil {
		panic(err)
	}
	if _, err := n.Read(ctx, p, int64(*count), false); *mode != "acknowledged" && err != d.ErrEmpty {
		panic("unexpected tail")
	}
	elapsed := time.Since(t)
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("mode=%s count=%d value_bytes=%d ids=%t hex_ids=%t open_ms=%.3f elapsed_s=%.3f openRSS_KiB=%d currentRSS_KiB=%d peakRSS_KiB=%d liveHeap_KiB=%d\n", *mode, *count, *valueBytes, *ids || *hexIDs, *hexIDs, float64(opened)/1e6, elapsed.Seconds(), initialRSS, rss("VmRSS"), rss("VmHWM"), mem.HeapAlloc/1024)
	if *mode == "random" {
		fmt.Printf("random_reads=%d read_elapsed_s=%.3f verified_reads_per_s=%.0f\n", *samples, readTime.Seconds(), float64(*samples)/readTime.Seconds())
	}
	if err := n.Close(); err != nil {
		panic(err)
	}
}
