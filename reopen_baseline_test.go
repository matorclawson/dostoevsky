package dostoevsky

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Baseline of the current engine at scale (evidence for the reopen redesign, not a gate).
// Two steps in two processes, so the measurement process holds nothing from the build:
//
//	DOST_REOPEN_BUILD=<empty dir on ext4> DOST_REOPEN_RECORDS=1000000 go test -run TestReopenBuild -v -count=1 -timeout 0 .
//	DOST_REOPEN_MEASURE=<the same dir>   DOST_REOPEN_RECORDS=1000000 go test -run TestReopenMeasure -v -count=1 -timeout 0 .
//
// The journal is the publish workload's (512-byte values, one topic, no IDs), written with one
// sync per 1,000 frames: it is a builder, and its speed is not a publish rate. The measurement
// prints bytes per record, the journal's size, three Open+Close times, the heap in use and the
// process's VmRSS and VmHWM after Open (read from /proc/self/status; Linux only).

func TestReopenBuild(t *testing.T) {
	dir := os.Getenv("DOST_REOPEN_BUILD")
	if dir == "" {
		t.Skip("set DOST_REOPEN_BUILD to an empty directory")
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		t.Fatalf("%s is not empty", dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	records := reopenRecords(t)
	start := time.Now()
	buildReopenJournalBatched(t, dir, records, 1000)
	fi, err := os.Stat(filepath.Join(dir, "journal.v1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reopen_build records=%d journal_bytes=%d bytes_per_record=%.1f build_seconds=%.1f", records, fi.Size(), float64(fi.Size())/float64(records), time.Since(start).Seconds())
}

func procStatusKiB(t testing.TB, key string) uint64 {
	t.Helper()
	f, err := os.Open("/proc/self/status")
	if err != nil {
		t.Fatalf("this measurement needs /proc: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), key+":"); ok {
			fields := strings.Fields(rest)
			if len(fields) > 0 {
				v, err := strconv.ParseUint(fields[0], 10, 64)
				if err == nil {
					return v
				}
			}
		}
	}
	t.Fatalf("%s not found in /proc/self/status", key)
	return 0
}

func TestReopenMeasure(t *testing.T) {
	dir := os.Getenv("DOST_REOPEN_MEASURE")
	if dir == "" {
		t.Skip("set DOST_REOPEN_MEASURE to the directory TestReopenBuild filled")
	}
	records := reopenRecords(t)
	fi, err := os.Stat(filepath.Join(dir, "journal.v1"))
	if err != nil {
		t.Fatal(err)
	}
	const rounds = 3
	var times []float64
	var heap, rss, hwm uint64
	for i := 0; i < rounds; i++ {
		start := time.Now()
		n, err := Open(dir, reopenConfig())
		open := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heap, rss, hwm = ms.HeapAlloc/1024, procStatusKiB(t, "VmRSS"), procStatusKiB(t, "VmHWM")
		}
		if err = n.Close(); err != nil {
			t.Fatal(err)
		}
		times = append(times, float64(open.Microseconds())/1000)
	}
	t.Logf("reopen_baseline records=%d journal_bytes=%d bytes_per_record=%.1f open_ms=%.1f,%.1f,%.1f heap_alloc_KiB_after_first_open=%d vmrss_KiB_after_first_open=%d vmhwm_KiB_after_first_open=%d",
		records, fi.Size(), float64(fi.Size())/float64(records), times[0], times[1], times[2], heap, rss, hwm)
}
