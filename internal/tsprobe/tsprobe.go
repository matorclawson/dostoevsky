// Package tsprobe records where an acknowledged publish spends its time. MEASUREMENT BRANCH ONLY: never merged.
// Everything is a fixed-size atomic histogram; one line per process (or per node) is appended to the file named by
// TIMESPLIT_OUT when a node that published is closed. Nothing here changes what is acknowledged or when it is synced.
package tsprobe

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const bins = 400 // 5% wide, from 1 microsecond to about 5 minutes

// Hist is a duration histogram with an exact count and sum.
type Hist struct {
	n     [bins]atomic.Uint64
	count atomic.Uint64
	sum   atomic.Int64
}

func (h *Hist) Add(d time.Duration) {
	i := 0
	if us := float64(d) / 1e3; us > 1 {
		i = min(int(math.Log(us)/math.Log(1.05)), bins-1)
	}
	h.n[i].Add(1)
	h.count.Add(1)
	h.sum.Add(int64(d))
}

func (h *Hist) quantile(q float64) float64 {
	rank := max(uint64(math.Ceil(q*float64(h.count.Load()))), 1)
	var seen uint64
	for i := range h.n {
		if seen += h.n[i].Load(); seen >= rank {
			return math.Pow(1.05, float64(i+1))
		}
	}
	return 0
}

// take prints name=count/mean/p50/p90/p99 in microseconds and resets.
func (h *Hist) take(name string) string {
	c := h.count.Load()
	s := name + "=none"
	if c > 0 {
		s = fmt.Sprintf("%s_n=%d %s_mean_us=%.1f %s_p50_us=%.0f %s_p90_us=%.0f %s_p99_us=%.0f", name, c,
			name, float64(h.sum.Load())/float64(c)/1e3, name, h.quantile(0.5), name, h.quantile(0.9), name, h.quantile(0.99))
	}
	for i := range h.n {
		h.n[i].Store(0)
	}
	h.count.Store(0)
	h.sum.Store(0)
	return s
}

// How a publish write came about.
const (
	Inline   = iota // an idle node: the caller wrote alone
	Leader          // the caller that started a drain
	Overflow        // the queue was full
	Queued          // a group written by the drainer
	kinds
)

const maxSize = 65 // group sizes 1..64 exactly, then "more"

var (
	LockWait, Write, Sync, QueueWait, Gather, Handler, Turnaround Hist

	groups     [kinds]atomic.Uint64
	sizes      [maxSize + 1]atomic.Uint64
	records    atomic.Uint64
	gatherFull atomic.Uint64
	gatherOver atomic.Uint64
	firstSync  atomic.Int64
	lastSync   atomic.Int64
)

// gatherEnv overrides the gather wait of this process, in microseconds.
var gatherEnv = os.Getenv("TIMESPLIT_GATHER_US")

// idle holds, per connection, when its last response was written (net.Conn -> time.Time).
var idle sync.Map

// GatherLimit is the gather wait of this process: TIMESPLIT_GATHER_US, or the default passed in.

func GatherLimit(def time.Duration) time.Duration {
	if us, err := strconv.Atoi(gatherEnv); err == nil && us >= 0 {
		return time.Duration(us) * time.Microsecond
	}
	return def
}

// Group records one publish write attempt of size records.
func Group(kind, size int) {
	groups[kind].Add(1)
	records.Add(uint64(size))
	sizes[min(size, maxSize)].Add(1)
}

// Gathered records one gather: how long it waited and whether it ended with the released callers back.
func Gathered(d time.Duration, full bool) {
	Gather.Add(d)
	if full {
		gatherFull.Add(1)
	} else {
		gatherOver.Add(1)
	}
}

// Synced records one append: the write and the sync that followed.
func Synced(start, written, synced time.Time) {
	Write.Add(written.Sub(start))
	Sync.Add(synced.Sub(written))
	firstSync.CompareAndSwap(0, synced.UnixNano())
	lastSync.Store(synced.UnixNano())
}

// ConnState measures the client turnaround: from the response written (the connection goes idle) to the first byte of the
// next request on the same connection, on one clock.
func ConnState(c net.Conn, s http.ConnState) {
	switch s {
	case http.StateIdle:
		idle.Store(c, time.Now())
	case http.StateActive:
		if t, ok := idle.LoadAndDelete(c); ok {
			Turnaround.Add(time.Since(t.(time.Time)))
		}
	case http.StateClosed, http.StateHijacked:
		idle.Delete(c)
	}
}

// Timed measures each request from handler entry to handler return.
func Timed(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.ServeHTTP(w, r)
		Handler.Add(time.Since(start))
	})
}

// Dump appends one line to TIMESPLIT_OUT and resets. A node that published nothing prints nothing.
func Dump(gatherDefault time.Duration) {
	out := os.Getenv("TIMESPLIT_OUT")
	recs := records.Swap(0)
	if recs == 0 || out == "" {
		return
	}
	var hist []string
	for i := range sizes {
		if n := sizes[i].Swap(0); n > 0 {
			label := strconv.Itoa(i)
			if i == maxSize {
				label = "more"
			}
			hist = append(hist, fmt.Sprintf("%s:%d", label, n))
		}
	}
	var g [kinds]uint64
	var total uint64
	for i := range groups {
		g[i] = groups[i].Swap(0)
		total += g[i]
	}
	var ru syscall.Rusage
	cpu := 0.0
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		cpu = float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9
	}
	span := float64(lastSync.Swap(0)-firstSync.Swap(0)) / 1e9
	syncs := Sync.count.Load()
	perSec := 0.0
	if span > 0 {
		perSec = float64(syncs) / span
	}
	line := fmt.Sprintf("timesplit pid=%d gomaxprocs=%d gather_limit_us=%d records=%d groups=%d inline=%d leader=%d overflow=%d queued=%d size_mean=%.2f sizes=%s gather_full=%d gather_over=%d syncs_per_s=%.1f process_cpu_s=%.2f %s %s %s %s %s %s %s\n",
		os.Getpid(), runtime.GOMAXPROCS(0), GatherLimit(gatherDefault).Microseconds(), recs, total, g[Inline], g[Leader], g[Overflow], g[Queued],
		float64(recs)/float64(max(total, 1)), strings.Join(hist, ","), gatherFull.Swap(0), gatherOver.Swap(0), perSec, cpu,
		LockWait.take("lock_wait"), Write.take("write"), Sync.take("sync"), QueueWait.take("queue_wait"), Gather.take("gather"), Handler.take("handler"), Turnaround.take("turnaround"))
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "timesplit:", err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(line); err != nil {
		fmt.Fprintln(os.Stderr, "timesplit:", err)
	}
}

// Profile starts a CPU profile and an execution trace into TIMESPLIT_PROFILE.cpu / .trace and returns the stop. A
// process that is restarted (the reopen) would overwrite them, so an existing file is left alone.
func Profile() func() {
	prefix := os.Getenv("TIMESPLIT_PROFILE")
	if prefix == "" {
		return func() {}
	}
	var stops []func()
	create := func(name string, start func(*os.File) error, stop func()) {
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		if err := start(f); err != nil {
			fmt.Fprintln(os.Stderr, "timesplit:", err)
			f.Close()
			return
		}
		stops = append(stops, func() { stop(); f.Close() })
	}
	create(prefix+".cpu", func(f *os.File) error { return pprof.StartCPUProfile(f) }, pprof.StopCPUProfile)
	create(prefix+".trace", func(f *os.File) error { return trace.Start(f) }, trace.Stop)
	return func() {
		for _, s := range stops {
			s()
		}
	}
}
