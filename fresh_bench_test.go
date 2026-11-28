package dostoevsky

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Each cell opens a copy of the same retained-ID fixture. All timings exclude
// generation of the input, preparation of membership hints and verification.
func TestFreshPublishCell(t *testing.T) {
	dir := os.Getenv("DOST_FRESH_DIR")
	if dir == "" {
		t.Skip("opt-in durable publishing experiment")
	}
	env := func(k string) int {
		v, e := strconv.Atoi(os.Getenv(k))
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	workers, size := env("DOST_WORKERS"), env("DOST_PAYLOAD")
	const count = 4096
	ctx := context.Background()
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	n, e := OpenSegmented(dir, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	if e = n.PrepareIDFilters(ctx); e != nil {
		t.Fatal(e)
	}
	items := make([]Message, count)
	for i := range items {
		items[i] = Message{ID: "fresh-" + strconv.Itoa(i), Value: bytes.Repeat([]byte{byte(i)}, size)}
	}
	lat := make([]time.Duration, count)
	offsets := make([]int64, count)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := w; i < count; i += workers {
				at := time.Now()
				r, err := n.Publish(ctx, "events", items[i])
				lat[i] = time.Since(at)
				if err != nil || r.ID != items[i].ID || !bytes.Equal(r.Value, items[i].Value) {
					t.Errorf("invalid receipt %d: %v", i, err)
					return
				}
				offsets[i] = r.Offset
			}
		}(w)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	begin := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begin)
	runtime.ReadMemStats(&after)
	if t.Failed() {
		return
	}
	hwm := 0
	data, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			hwm, _ = strconv.Atoi(strings.Fields(line)[1])
			break
		}
	}
	sorted := slices.Clone(offsets)
	slices.Sort(sorted)
	for i, v := range sorted {
		if v != 97*64+int64(i) {
			t.Fatal("unexpected fresh offset", i, v)
		}
	}
	slices.Sort(lat)
	fmt.Printf("CELL %d %d %.2f %.1f %.1f %.2f %.1f %d %d\n", workers, size, count/elapsed.Seconds(), float64(lat[(count*95+99)/100-1])/1000, float64(lat[(count*99+99)/100-1])/1000, float64(after.Mallocs-before.Mallocs)/count, float64(after.TotalAlloc-before.TotalAlloc)/count, hwm, n.publishes.groupAttempts.Load())
}

// Measure the storage calls independently of the throughput trials. This
// wrapper forwards every operation to the real active segment on the test disk.
type freshTimedFile struct {
	fsFile
	write, syncTime time.Duration
	syncs           int
}

func (f *freshTimedFile) WriteAt(p []byte, off int64) (int, error) {
	at := time.Now()
	n, e := f.fsFile.WriteAt(p, off)
	f.write += time.Since(at)
	return n, e
}
func (f *freshTimedFile) SyncData() error {
	at := time.Now()
	e := f.fsFile.SyncData()
	f.syncTime += time.Since(at)
	f.syncs++
	return e
}

func TestFreshDurableStages(t *testing.T) {
	dir := os.Getenv("DOST_FRESH_PROFILE_DIR")
	if dir == "" {
		t.Skip("opt-in storage timing")
	}
	n, e := OpenSegmented(dir, Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	if e := n.PrepareIDFilters(context.Background()); e != nil {
		t.Fatal(e)
	}
	f := &freshTimedFile{fsFile: n.segments.active.f}
	n.segments.active.f = f
	const count = 2048
	value := make([]byte, 1024)
	now := time.Now()
	for i := 0; i < count; i++ {
		_, err := n.Publish(context.Background(), "events", Message{ID: "profile-" + strconv.Itoa(i), Value: value})
		if err != nil {
			t.Fatal(err)
		}
	}
	total := time.Since(now)
	if f.syncs != count {
		t.Fatalf("expected %d syncs, observed %d", count, f.syncs)
	}
	t.Logf("records=%d write_us/record=%.1f fdatasync_us/record=%.1f elapsed_us/record=%.1f sync_share_pct=%.1f", count, float64(f.write.Microseconds())/count, float64(f.syncTime.Microseconds())/count, float64(total.Microseconds())/count, 100*float64(f.syncTime)/float64(total))
}
