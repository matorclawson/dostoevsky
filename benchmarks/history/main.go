// Command history measures one Linux process opening or extending a durable journal.
// Run each open in a fresh process against the same fixture. OS caches are not dropped.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

func value(i int) []byte {
	var v [512]byte
	binary.LittleEndian.PutUint64(v[:], uint64(i))
	seed := sha256.Sum256(v[:8])
	for j := 8; j < len(v); j++ {
		v[j] = seed[j%len(seed)]
	}
	return v[:]
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
	mode := flag.String("mode", "open", "seed, open, or verify")
	count := flag.Int("n", 16000, "total retained records")
	start := flag.Int("start", 0, "first offset when extending a fixture")
	ids := flag.Bool("ids", false, "write and check unique producer IDs")
	flag.Parse()
	if *dir == "" || *count < 1 || *start < 0 || *start >= *count {
		panic("invalid arguments")
	}
	t := time.Now()
	n, e := d.Open(*dir, d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
	if e != nil {
		panic(e)
	}
	opened := time.Since(t)
	initialRSS := rss("VmRSS")
	ctx := context.Background()
	p := d.Partition{Topic: "events"}
	id := func(i int) string {
		if *ids {
			return strconv.Itoa(i)
		}
		return ""
	}
	switch *mode {
	case "seed":
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
		}
	case "verify":
		for i := 0; i < *count; i++ {
			r, err := n.Read(ctx, p, int64(i), false)
			if err != nil || r.Offset != int64(i) || r.ID != id(i) || !bytes.Equal(r.Value, value(i)) {
				panic(fmt.Sprintf("verification failed at %d: %v", i, err))
			}
		}
	case "open":
	default:
		panic("unknown mode")
	}
	if _, err := n.Read(ctx, p, int64(*count-1), false); err != nil {
		panic(err)
	}
	if _, err := n.Read(ctx, p, int64(*count), false); err != d.ErrEmpty {
		panic("unexpected tail")
	}
	elapsed := time.Since(t)
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("mode=%s count=%d ids=%t open_ms=%.3f elapsed_s=%.3f openRSS_KiB=%d currentRSS_KiB=%d peakRSS_KiB=%d liveHeap_KiB=%d\n", *mode, *count, *ids, float64(opened)/1e6, elapsed.Seconds(), initialRSS, rss("VmRSS"), rss("VmHWM"), mem.HeapAlloc/1024)
	if err := n.Close(); err != nil {
		panic(err)
	}
}
