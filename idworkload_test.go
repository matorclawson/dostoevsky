package dostoevsky

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Run in a fresh process for each cell. Only use a disposable idpublish fixture.
func TestIdentifiedPublishWorkload(t *testing.T) {
	dir := os.Getenv("DOST_ID_DIR")
	if dir == "" {
		t.Skip("opt-in fixture")
	}
	integer := func(name string) int {
		i, e := strconv.Atoi(os.Getenv(name))
		if e != nil || i < 1 {
			t.Fatal(name)
		}
		return i
	}
	history, workers, count := integer("DOST_ID_HISTORY"), integer("DOST_ID_WORKERS"), integer("DOST_ID_COUNT")
	mode := os.Getenv("DOST_ID_MODE")
	if mode != "new" && mode != "recent" && mode != "oldest" && mode != "roll" {
		t.Fatal(mode)
	}
	ctx := context.Background()
	p := Partition{Topic: "events"}
	n, e := Open(dir, Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	makeMsg := func(i int) Message {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(i))
		h := sha256.Sum256(b[:])
		return Message{ID: hex.EncodeToString(h[:16]), Value: b[:]}
	}
	seed := os.Getenv("DOST_ID_RUN")
	prepare := func(i int) (Message, int64) {
		switch mode {
		case "oldest":
			return makeMsg(0), 0
		case "recent":
			return makeMsg(history - 1), int64(history - 1)
		default:
			m := makeMsg(i)
			m.ID = seed + "-" + m.ID
			if mode == "roll" {
				m.Value = bytes.Repeat(m.Value, 64)
			}
			return m, -1
		}
	}
	lat := make([]time.Duration, count)
	offsets := make([]int64, count)
	var wg sync.WaitGroup
	start := make(chan struct{})
	var before uint64
	if n.segments != nil {
		before = n.segments.active.id
	}
	coldStart := time.Now()
	first, want := prepare(0)
	r, e := n.Publish(ctx, p.Topic, first)
	firstUS := time.Since(coldStart).Microseconds()
	if e != nil || want >= 0 && r.Offset != want {
		t.Fatal(r, e)
	}
	base := r.Offset
	var probes, loads uint64
	if n.segments != nil {
		probes = n.segments.idProbes
		loads = n.segments.indexLoads
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := w; i < count; i += workers {
				m, want := prepare(i + 1)
				begin := time.Now()
				r, e := n.Publish(ctx, p.Topic, m)
				lat[i] = time.Since(begin)
				if e != nil || want >= 0 && r.Offset != want {
					t.Errorf("publish %d offset %d error %v", i, r.Offset, e)
					return
				}
				offsets[i] = r.Offset
			}
		}(w)
	}
	begin := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(begin)
	if t.Failed() {
		return
	}
	var boundaryUS []int64
	if l := n.segments; l != nil && l.active.id > before {
		starts := map[int64]bool{}
		for id := before + 1; id <= l.active.id; id++ {
			x := l.idx
			if id != l.active.id {
				var err error
				x, err = l.sealedIndex(id)
				if err != nil {
					t.Fatal(err)
				}
			}
			starts[int64(x.first(l.state.byPart[p]))] = true
		}
		for i, off := range offsets {
			if starts[off] {
				boundaryUS = append(boundaryUS, lat[i].Microseconds())
			}
		}
	}
	if len(boundaryUS) > 0 {
		fmt.Printf("rollover_first_record_us=%v\n", boundaryUS)
	}
	if want < 0 {
		slices.Sort(offsets)
		for i, off := range offsets {
			if off != base+1+int64(i) {
				t.Fatal("non-contiguous receipts", i, off)
			}
		}
	}
	slices.Sort(lat)
	percent := func(p int) float64 { return float64(lat[(len(lat)*p+99)/100-1]) / 1000 }
	var rolls uint64
	var pr, ld float64
	if l := n.segments; l != nil {
		rolls = l.active.id - before
		pr = float64(l.idProbes-probes) / float64(count)
		ld = float64(l.indexLoads-loads) / float64(count)
	}
	status, _ := os.ReadFile("/proc/self/status")
	rss := func(k string) string {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, k+":") {
				return strings.Fields(line)[1]
			}
		}
		return "0"
	}
	fmt.Printf("history=%d workers=%d mode=%s samples=%d first_us=%d first_probes=%d first_loads=%d rate=%.1f p50_us=%.1f p95_us=%.1f p99_us=%.1f probes=%.5f loads=%.5f rolls=%d rss_KiB=%s peak_KiB=%s\n", history, workers, mode, count, firstUS, probes, loads, float64(count)/elapsed.Seconds(), percent(50), percent(95), percent(99), pr, ld, rolls, rss("VmRSS"), rss("VmHWM"))
}
