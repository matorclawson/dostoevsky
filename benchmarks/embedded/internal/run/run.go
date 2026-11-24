// Package run drives identical Linux workloads in separate engine binaries.
package run

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Store interface {
	Publish([]byte) (uint64, error)
	Read(uint64) ([]byte, error)
	Close()
}

// Measured is implemented by a Store backed by a separate server process. The
// harness then reports that process tree's sampled peak memory next to its own.
type Measured interface {
	// ServerPeakKiB is the sum of the highest sampled VmHWM of every process in the server tree.
	ServerPeakKiB() uint64
}

func Main(engine string, open func(string, bool) (Store, error)) {
	count := flag.Int("count", 2000, "records per run")
	workers := flag.Int("workers", 1, "concurrent synchronous producers")
	flag.Parse()
	if *count < 1 || *workers < 1 || *workers > *count {
		log.Fatal("invalid workload")
	}
	dir, err := os.MkdirTemp("", "embedded-bench-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s, err := open(dir, true)
	if err != nil {
		log.Fatal(err)
	}
	delays := make([]time.Duration, *count)
	seq := make([]uint64, *count)
	var next atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 512)
			for {
				i := int(next.Add(1) - 1)
				if i >= *count {
					return
				}
				binary.BigEndian.PutUint64(data, uint64(i))
				begin := time.Now()
				offset, e := s.Publish(data)
				delays[i] = time.Since(begin)
				if e != nil {
					log.Fatal(e)
				}
				seq[i] = offset
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var usage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		log.Fatal(err)
	}
	var server uint64
	measured := false
	if m, ok := s.(Measured); ok {
		server, measured = m.ServerPeakKiB(), true
	}
	s.Close()
	start = time.Now()
	s, err = open(dir, false)
	if err != nil {
		log.Fatal(err)
	}
	reopen := time.Since(start)
	defer s.Close()
	want := make([]byte, 512)
	readStart := time.Now()
	for i, offset := range seq {
		binary.BigEndian.PutUint64(want, uint64(i))
		got, e := s.Read(offset)
		if e != nil || !bytes.Equal(got, want) {
			log.Fatalf("record %d at %d: %v", i, offset, e)
		}
	}
	readElapsed := time.Since(readStart)
	sort.Slice(delays, func(i, j int) bool { return delays[i] < delays[j] })
	line := fmt.Sprintf("engine=%s workers=%d verified=%d records/s=%.0f p95_us=%d p99_us=%d liveHeap_KiB=%d peakRSS_KiB=%d reopen_ms=%.2f verified_reads_per_s=%.0f", engine, *workers, *count, float64(*count)/elapsed.Seconds(), delays[(95*len(delays)-1)/100].Microseconds(), delays[(99*len(delays)-1)/100].Microseconds(), mem.HeapAlloc/1024, usage.Maxrss, float64(reopen)/1e6, float64(*count)/readElapsed.Seconds())
	if measured {
		// peakRSS_KiB stays the harness process alone; total_rss_KiB adds the server tree (sampled peaks).
		line += fmt.Sprintf(" server_rss_KiB=%d total_rss_KiB=%d arch=server", server, uint64(usage.Maxrss)+server)
	}
	fmt.Println(line)
}
