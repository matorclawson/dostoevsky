package dostoevsky

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The driver runs every measured cell in a fresh process over a fixture copy.
func TestAdmissionWorkload(t *testing.T) {
	dir := os.Getenv("DOST_ADMISSION_DIR")
	if dir == "" {
		t.Skip("opt-in benchmark")
	}
	ctx := context.Background()
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	n, err := OpenSegmented(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	const segments, per = 96, 64
	mode := os.Getenv("DOST_MODE")
	if mode == "seed" {
		n.rollBytes = 1
		for s := 0; s <= segments; s++ {
			items := make([]Publication, per)
			for i := range items {
				items[i] = Publication{"events", Message{ID: fmt.Sprint(s*per + i)}}
			}
			if _, err := n.PublishBatch(ctx, items); err != nil {
				t.Fatal(err)
			}
		}
		if err := n.PrepareIDFilters(ctx); err != nil {
			t.Fatal(err)
		}
		return
	}
	if os.Getenv("DOST_POLICY") == "frequency" && !any(n).(interface{ EnableIndexAdmission() bool }).EnableIndexAdmission() {
		t.Fatal("not segmented")
	}
	workers, err := strconv.Atoi(os.Getenv("DOST_WORKERS"))
	if err != nil {
		t.Fatal(err)
	}
	const count = 4096
	rng := rand.New(rand.NewSource(42))
	ids := make([]int, count)
	for i := range ids {
		s := rng.Intn(segments)
		switch mode {
		case "mixed", "shift":
			hot := 0
			if mode == "shift" && i >= count/2 {
				hot = 8
			}
			if i%128 < 80 {
				s = hot + rng.Intn(8)
			} else {
				s = 16 + (i/128*48+i%128-80)%80
			}
		case "uniform":
		case "append":
		default:
			t.Fatal(mode)
		}
		ids[i] = s*per + rng.Intn(per)
	}
	// Load membership hints before timing; exact indexes remain cold on reopen.
	if err := n.PrepareIDFilters(ctx); err != nil {
		t.Fatal(err)
	}
	loads := n.segments.indexLoads
	lat := make([]time.Duration, count)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := w; i < count; i += workers {
				key := strconv.Itoa(ids[i])
				if mode == "append" {
					key = "new-" + strconv.Itoa(i)
				}
				at := time.Now()
				r, e := n.Publish(ctx, "events", Message{ID: key})
				lat[i] = time.Since(at)
				if e != nil || r.ID != key || (mode != "append" && r.Offset != int64(ids[i])) {
					t.Errorf("receipt %d: %v %v", i, r, e)
					return
				}
			}
		}(w)
	}
	begin := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begin)
	peak := 0
	status, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			peak, _ = strconv.Atoi(strings.Fields(line)[1])
		}
	}
	slices.Sort(lat)
	fmt.Printf("CELL %s %s %d %.2f %.2f %.2f %d\n", os.Getenv("DOST_POLICY"), mode, workers, count/elapsed.Seconds(), float64(lat[(count*99+99)/100-1])/1000, float64(n.segments.indexLoads-loads)*1000/count, peak)
}
