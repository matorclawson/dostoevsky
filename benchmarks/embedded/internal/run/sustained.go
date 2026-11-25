package run

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/hist"
)

// Sustained mode runs one engine under a fixed window with a target, the way the 100M
// tests do: N synchronous producers publish 512-byte records until the first of
//
//	reached   the target number of acknowledged messages,
//	window    the fixed window ends,
//	failed    the engine returns an error (never retried),
//	memory    the host's available memory fell below the guard (see minFreeKiB),
//	disk      the data disk's free space fell below Sustained.MinFreeDiskKiB,
//	verify    a sampled record read back wrong.
//
// Counting contract: acked is the number of messages the engine acknowledged, all of them.
// The first failure (or other stop) tells every producer to stop at its next loop; a
// publish already in flight, or one started before the stop was seen, still completes, and
// if the engine acknowledged it it is counted, because it is on disk. The reason text names
// the count at the moment the stop was recorded ("failed at N acknowledged"), which can be a
// few below the final acked. The single publish that returned the error is never counted
// and nothing is retried. All counters are atomic, so none of this races.
//
// Whatever the reason, the run prints one final "sustained" line saying so in plain words
// together with the count reached; a failure is a result, not an error exit. Per-bucket
// lines (messages per second, p99, memory, disk) and per-reopen lines come before it.

// Retainer is implemented by a Store that can drop old records so disk stays bounded.
// Retain(keepFrom) allows every record with an offset below keepFrom to be dropped.
type Retainer interface {
	Retain(keepFrom uint64) error
}

// Sustained configures a run.
type Sustained struct {
	Window         time.Duration // the fixed window
	Target         int64         // stop at this many acknowledged messages
	Workers        int           // synchronous producers
	Bucket         time.Duration // length of a reporting bucket
	ReopenEvery    int64         // close, reopen and verify every this many acknowledged messages (0: only at the end)
	KeepLast       int64         // with a Retainer, keep about this many recent records (0: no retention)
	RetainEvery    time.Duration // how often retention runs
	MinFreeDiskKiB uint64        // stop, as a result, when the data disk has less free space than this
	Sample         int           // records checked after a reopen (default 2000)
}

// Stop reasons, as printed in stop=.
const (
	StopReached = "reached"
	StopWindow  = "window"
	StopFailed  = "failed"
	StopMemory  = "memory-guard"
	StopDisk    = "disk"
	StopVerify  = "verification-failed"
)

// abandonAfter is how long a stopped run waits for producers still inside a publish before
// it stops waiting (a hung server): the publishes that did not return are named.
var abandonAfter = 60 * time.Second

// tickEvery is how often the run checks memory and disk and decides on buckets, retention
// and reopens (a variable so tests need not wait).
var tickEvery = 200 * time.Millisecond

// hooks, so tests can simulate a full disk and measure without a real data disk
var (
	diskFreeKiB = func(dir string) (uint64, error) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(dir, &st); err != nil {
			return 0, err
		}
		return uint64(st.Bavail) * uint64(st.Bsize) / 1024, nil
	}
	diskUsedKiB = func(dir string) (uint64, error) {
		var total int64
		err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil // a file removed during the walk
				}
				return err
			}
			if !d.IsDir() {
				if info, e := d.Info(); e == nil {
					total += info.Size()
				}
			}
			return nil
		})
		return uint64(total) / 1024, err
	}
)

// ----- the latency histogram of a sustained run -----

// latHist is internal/hist's bins (shared with every other harness and language) with atomic counters, because the
// producers add to it concurrently. The bin of a duration is hist.Index, so a number means the same thing everywhere.
type latHist struct{ n [hist.Bins]atomic.Uint64 }

func (h *latHist) add(d time.Duration) { h.n[hist.Index(d)].Add(1) }

// quantile returns the upper edge of the bin holding the q-th quantile, so it is accurate to 1%.
func (h *latHist) quantile(q float64) time.Duration {
	var total uint64
	for i := range h.n {
		total += h.n[i].Load()
	}
	if total == 0 {
		return 0
	}
	rank := max(uint64(math.Ceil(q*float64(total))), 1)
	var seen uint64
	for i := range h.n {
		if seen += h.n[i].Load(); seen >= rank {
			return time.Duration(math.Pow(1.01, float64(i+1)) * 1e3)
		}
	}
	return 0
}

// ----- the run -----

type stopInfo struct {
	kind, text string
	at         time.Time
}

type pair struct {
	i   int64
	off uint64
}

func maxRSSKiB() uint64 {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return uint64(ru.Maxrss) / 1024 // macOS reports bytes
	}
	return uint64(ru.Maxrss)
}

func availMemText() string {
	kib, err := memAvailable()
	if err != nil {
		return "unavailable"
	}
	return strconv.FormatUint(kib, 10)
}

// lockedWriter serializes the lines of the coordinator, retention and reopen, which can print at the same time.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

func executeSustained(engine string, open func(string, bool) (Store, error), o Sustained, out io.Writer) (err error) {
	out = &lockedWriter{w: out}
	if o.Workers < 1 || o.Target < 1 || o.Window <= 0 || o.Bucket <= 0 || o.KeepLast < 0 || (o.KeepLast > 0 && o.RetainEvery <= 0) {
		return errors.New("invalid sustained workload")
	}
	if o.Sample <= 0 {
		o.Sample = 2000
	}
	if err := checkMemory(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	setContext(ctx)
	defer func() { cancel(); setContext(context.Background()) }()

	dir, err := os.MkdirTemp("", "sustained-bench-")
	if err != nil {
		return err
	}
	defer func() {
		if rerr := removeAll(dir); rerr != nil && err == nil {
			err = rerr
		}
	}()

	var (
		s      Store
		closed = true
		gate   sync.RWMutex // producers and retention hold it for reading; a reopen holds it for writing
	)
	bind := func() {
		if b, ok := s.(Cancelable); ok {
			b.BindContext(ctx)
		}
	}
	noClose := false // set when a retention is still running at the end: closing under it would be a bug
	closeStore := func() (cerr error) {
		if s == nil || closed || noClose {
			return nil
		}
		closed = true
		defer func() {
			if r := recover(); r != nil {
				cerr = fmt.Errorf("close: %v", r)
			}
		}()
		s.Close()
		return nil
	}
	defer func() { closeStore() }()
	if s, err = open(dir, true); err != nil {
		return fmt.Errorf("open: %w", err)
	}
	closed = false
	bind()

	var (
		next, acked   atomic.Int64
		maxOffset     atomic.Uint64
		floor         atomic.Uint64 // the offset below which retention may have dropped records
		stop          atomic.Pointer[stopInfo]
		failure       atomic.Pointer[stopInfo] // the first engine failure, kept apart from whichever stop was admitted first
		cur           atomic.Pointer[latHist]
		total         = new(latHist)
		reopens       int
		finalReopenMS = "none"
		sampMu        sync.Mutex
		strided       []pair
		ring          [1000]pair
		ringN         int
	)
	cur.Store(new(latHist))
	setStop := func(kind, text string) { stop.CompareAndSwap(nil, &stopInfo{kind, text, time.Now()}) }
	// An engine failure always counts as the result, even when the window (or the target) was admitted first: an
	// in-flight publish that fails after the stop is still a failed engine, never a normal window.
	setFailure := func(text string) {
		failure.CompareAndSwap(nil, &stopInfo{StopFailed, text, time.Now()})
		setStop(StopFailed, text)
	}
	stride := max(o.Target/2000, 1)

	start := time.Now()
	deadline := start.Add(o.Window) // monotonic: producers check it themselves, so slow telemetry cannot extend the window
	var wg sync.WaitGroup
	for range o.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 512)
			for {
				if stop.Load() != nil {
					return
				}
				if !time.Now().Before(deadline) {
					setStop(StopWindow, fmt.Sprintf("window of %v ended at %d acknowledged", o.Window, acked.Load()))
					return
				}
				i := next.Add(1) - 1
				if i >= o.Target {
					return
				}
				binary.BigEndian.PutUint64(data, uint64(i))
				gate.RLock()
				begin := time.Now()
				off, e := s.Publish(data)
				d := time.Since(begin)
				gate.RUnlock()
				if e != nil {
					// Never retried: an ambiguous publish is the engine's failure.
					setFailure(fmt.Sprintf("engine failed at %d acknowledged: publish %d: %v", acked.Load(), i, e))
					return
				}
				acked.Add(1)
				for {
					m := maxOffset.Load()
					if off <= m || maxOffset.CompareAndSwap(m, off) {
						break
					}
				}
				cur.Load().add(d)
				total.add(d)
				if i%stride == 0 || i%16 == 0 {
					sampMu.Lock()
					if i%stride == 0 {
						strided = append(strided, pair{i, off})
					}
					if i%16 == 0 {
						ring[ringN%len(ring)] = pair{i, off}
						ringN++
					}
					sampMu.Unlock()
				}
			}
		}()
	}

	serverPeak := func() string {
		if m, ok := s.(Measured); ok && !closed {
			if kib, e := m.ServerPeakKiB(); e == nil {
				return strconv.FormatUint(kib, 10)
			}
			return "unmeasured"
		}
		return "none"
	}
	diskText := func() (used, free string) {
		used, free = "unmeasured", "unmeasured"
		if u, e := diskUsedKiB(dir); e == nil {
			used = strconv.FormatUint(u, 10)
		}
		if f, e := diskFreeKiB(dir); e == nil {
			free = strconv.FormatUint(f, 10)
		}
		return
	}

	// verify reads back a sample of acknowledged records that retention has not dropped:
	// the strided sample across the whole run and the most recent ones.
	verify := func() (int, error) {
		sampMu.Lock()
		pairs := slices.Clone(strided)
		n := min(ringN, len(ring))
		pairs = append(pairs, ring[:n]...)
		sampMu.Unlock()
		// The strided and the recent samples can hold the same record: each is read and counted once.
		seen := make(map[uint64]bool, len(pairs))
		pairs = slices.DeleteFunc(pairs, func(p pair) bool {
			if seen[p.off] {
				return true
			}
			seen[p.off] = true
			return false
		})
		fl := floor.Load()
		pairs = slices.DeleteFunc(pairs, func(p pair) bool { return p.off < fl })
		if len(pairs) > o.Sample {
			step := float64(len(pairs)) / float64(o.Sample)
			picked := make([]pair, 0, o.Sample)
			for k := 0; k < o.Sample; k++ {
				picked = append(picked, pairs[int(float64(k)*step)])
			}
			pairs = picked
		}
		want := make([]byte, 512) // the whole record: the index in the first 8 bytes, zeros after, as published
		for _, p := range pairs {
			got, e := s.Read(p.off)
			binary.BigEndian.PutUint64(want, uint64(p.i))
			if e != nil || !bytes.Equal(got, want) {
				return 0, fmt.Errorf("record %d at offset %d read back wrong: %v", p.i, p.off, e)
			}
		}
		return len(pairs), nil
	}

	// reopen closes the store, opens it again (timed), and verifies a sample. A problem comes
	// back as the reason to stop with; nil means all is well.
	reopen := func(final bool) *stopInfo {
		gate.Lock()
		defer gate.Unlock()
		fail := func(kind, format string, a ...any) *stopInfo {
			return &stopInfo{kind, fmt.Sprintf(format, a...), time.Now()}
		}
		if e := closeStore(); e != nil {
			return fail(StopFailed, "engine failed at %d acknowledged: close: %v", acked.Load(), e)
		}
		begin := time.Now()
		ns, e := open(dir, false)
		ms := float64(time.Since(begin)) / 1e6
		if e != nil {
			return fail(StopFailed, "engine failed at %d acknowledged: reopen: %v", acked.Load(), e)
		}
		s, closed = ns, false
		bind()
		n, e := verify()
		if e != nil {
			return fail(StopVerify, "after reopen at %d acknowledged: %v", acked.Load(), e)
		}
		used, _ := diskText()
		fmt.Fprintf(out, "reopen engine=%s at_acked=%d ms=%.2f verified=%d harness_peak_rss_KiB=%d server_peak_rss_KiB=%s disk_KiB=%s\n", engine, acked.Load(), ms, n, maxRSSKiB(), serverPeak(), used)
		if final {
			finalReopenMS = fmt.Sprintf("%.2f", ms)
		} else {
			reopens++
		}
		return nil
	}

	var retaining atomic.Bool
	var retentionWG sync.WaitGroup
	var fenced atomic.Bool // set once the run is over: no retention starts after it
	retain := func() {
		defer retentionWG.Done()
		defer retaining.Store(false)
		gate.RLock() // s is replaced only under the write lock, so it is read under the read lock
		defer gate.RUnlock()
		r, ok := s.(Retainer)
		if !ok {
			return
		}
		top := maxOffset.Load()
		if top <= uint64(o.KeepLast) {
			return
		}
		keep := top - uint64(o.KeepLast)
		begin := time.Now()
		if e := r.Retain(keep); e != nil {
			setFailure(fmt.Sprintf("engine failed at %d acknowledged: retention: %v", acked.Load(), e))
			return
		}
		floor.Store(keep)
		fmt.Fprintf(out, "retain engine=%s floor=%d ms=%.1f at_acked=%d\n", engine, keep, float64(time.Since(begin))/1e6, acked.Load())
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	tick := time.NewTicker(tickEvery)
	defer tick.Stop()
	window := time.NewTimer(o.Window)
	defer window.Stop()
	bucketEnd, retainAt := start.Add(o.Bucket), start.Add(o.RetainEvery)
	nextReopen := o.ReopenEvery
	bucketN, lastAcked, lastBucket := 0, int64(0), start
	abandoned := false
	for running := true; running; {
		select {
		case <-done:
			running = false
		case <-window.C:
			setStop(StopWindow, fmt.Sprintf("window of %v ended at %d acknowledged", o.Window, acked.Load()))
		case now := <-tick.C:
			if e := checkMemory(); e != nil {
				setStop(StopMemory, fmt.Sprintf("stopped by the host memory guard at %d acknowledged (%v); the engine had not finished", acked.Load(), e))
				cancel() // a store waiting on a server stops waiting
			} else if kib, e := diskFreeKiB(dir); e == nil && o.MinFreeDiskKiB > 0 && kib < o.MinFreeDiskKiB {
				setStop(StopDisk, fmt.Sprintf("disk nearly full at %d acknowledged: %d KiB free, below the %d KiB floor", acked.Load(), kib, o.MinFreeDiskKiB))
			}
			if st := stop.Load(); st != nil && now.Sub(st.at) > abandonAfter {
				abandoned, running = true, false
				break
			}
			if stop.Load() != nil {
				continue
			}
			if !now.Before(bucketEnd) {
				h := cur.Swap(new(latHist))
				time.Sleep(time.Millisecond) // writers that loaded the old histogram finish adding
				a := acked.Load()
				used, free := diskText()
				bucketN++
				fmt.Fprintf(out, "bucket engine=%s n=%d t_s=%.0f acked=%d msgs_per_s=%.0f p99_us=%d harness_peak_rss_KiB=%d server_peak_rss_KiB=%s disk_KiB=%s avail_mem_KiB=%s avail_disk_KiB=%s\n",
					engine, bucketN, now.Sub(start).Seconds(), a, float64(a-lastAcked)/now.Sub(lastBucket).Seconds(), h.quantile(0.99).Microseconds(), maxRSSKiB(), serverPeak(), used, availMemText(), free)
				lastAcked, lastBucket = a, now
				for !bucketEnd.After(now) {
					bucketEnd = bucketEnd.Add(o.Bucket)
				}
			}
			if o.KeepLast > 0 && !now.Before(retainAt) && !fenced.Load() && retaining.CompareAndSwap(false, true) {
				retainAt = now.Add(o.RetainEvery)
				retentionWG.Add(1)
				go retain()
			}
			if o.ReopenEvery > 0 && acked.Load() >= nextReopen {
				if bad := reopen(false); bad != nil {
					setStop(bad.kind, bad.text)
				} else {
					nextReopen += o.ReopenEvery
				}
			}
		}
	}
	elapsed := time.Since(start)
	// Retention is fenced and joined BEFORE anything closes the store: a Close that overlaps an active Retain is a bug of
	// the harness. A retention still blocked after abandonAfter is reported and the store is then left unclosed.
	fenced.Store(true)
	retentionDone := make(chan struct{})
	go func() { retentionWG.Wait(); close(retentionDone) }()
	retentionStuck := false
	select {
	case <-retentionDone:
	case <-time.After(abandonAfter):
		retentionStuck = true
	}
	st := stop.Load()
	if f := failure.Load(); f != nil && (st == nil || st.kind == StopWindow || st.kind == StopReached) {
		st = f // a failed publish after the window or the target was admitted still makes this a failed run
	}
	if st == nil && acked.Load() >= o.Target {
		st = &stopInfo{StopReached, fmt.Sprintf("reached %d acknowledged messages", acked.Load()), time.Now()}
	}
	if st == nil {
		st = &stopInfo{StopFailed, "ended without a recorded reason", time.Now()}
	}
	if abandoned {
		st = &stopInfo{st.kind, st.text + "; producers still inside a publish after " + abandonAfter.String() + " were abandoned", st.at}
	}
	if retentionStuck {
		noClose = true
		st = &stopInfo{StopFailed, st.text + "; retention still running after " + abandonAfter.String() + ", the store was not closed", st.at}
	}
	if (st.kind == StopReached || st.kind == StopWindow) && !abandoned && !retentionStuck {
		// A finished run closes, reopens and verifies once more; a problem there is the result.
		if bad := reopen(true); bad != nil {
			st = bad
		}
	}
	harness, server := maxRSSKiB(), serverPeak()
	used, free := diskText()
	if e := closeStore(); e != nil && st.kind != StopFailed {
		st = &stopInfo{StopFailed, st.text + "; close: " + e.Error(), st.at}
	}
	fmt.Fprintf(out, "sustained engine=%s workers=%d target=%d window_s=%.0f stop=%s acked=%d seconds=%.1f msgs_per_s=%.0f p99_us=%d reopens=%d final_reopen_ms=%s harness_peak_rss_KiB=%d server_peak_rss_KiB=%s disk_KiB=%s avail_mem_KiB=%s avail_disk_KiB=%s reason=%q\n",
		engine, o.Workers, o.Target, o.Window.Seconds(), st.kind, acked.Load(), elapsed.Seconds(), float64(acked.Load())/elapsed.Seconds(), total.quantile(0.99).Microseconds(), reopens, finalReopenMS, harness, server, used, availMemText(), free, st.text)
	return nil
}
