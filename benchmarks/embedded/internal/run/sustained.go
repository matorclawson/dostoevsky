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

// telemetryBound is how long the final line waits for the disk figures: a slow disk walk must not hold the receipt back.
var telemetryBound = time.Second

// us3 prints a duration as microseconds with three decimals (the upper bin edge, nothing rounded away).
func us3(d time.Duration) string { return strconv.FormatFloat(float64(d)/1000, 'f', 3, 64) }

// ----- the run -----

// memoryCut is a verification cut short by the host memory guard.
type memoryCut struct{ error }

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
	mu   sync.Mutex
	w    io.Writer
	shut bool
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.shut { // after the final result line nothing else may appear (an abandoned retention that wakes up later)
		return len(b), nil
	}
	return l.w.Write(b)
}

func (l *lockedWriter) shutOff() { l.mu.Lock(); l.shut = true; l.mu.Unlock() }

// finish writes the final receipt line and shuts the writer in one step.
func (l *lockedWriter) finish(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.shut {
		io.WriteString(l.w, line)
	}
	l.shut = true
}

func executeSustained(engine string, open func(string, bool) (Store, error), o Sustained, out io.Writer) (err error) {
	lw := &lockedWriter{w: out}
	out = lw
	defer lw.shutOff()
	if o.Workers < 1 || o.Target < 1 || o.Window <= 0 || o.Bucket <= 0 || o.KeepLast < 0 || (o.KeepLast > 0 && o.RetainEvery <= 0) {
		return errors.New("invalid sustained workload")
	}
	if o.Sample <= 0 {
		o.Sample = 2000
	}
	if err := checkMemory(); err != nil {
		return err
	}
	// The package-level hooks are read ONCE here: work abandoned at the end can outlive this call, and must not read a
	// package variable that a test (or anything else) restores afterwards.
	usedFn, freeFn, memFn, minFree, bound := diskUsedKiB, diskFreeKiB, memAvailable, minFreeKiB, telemetryBound
	availMem := func() string {
		kib, err := memFn()
		if err != nil {
			return "unavailable"
		}
		return strconv.FormatUint(kib, 10)
	}
	// checkMem returns the reading it judged, so a trip is reported with the value that tripped it (one reading, not two).
	checkMem := func() (uint64, error) {
		kib, err := memFn()
		if errors.Is(err, errNoMeminfo) {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("cannot read available memory: %w", err)
		}
		if kib < minFree {
			return kib, fmt.Errorf("low-memory abort: %d KiB available, below %d KiB", kib, minFree)
		}
		return kib, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	setContext(ctx)
	defer func() { cancel(); setContext(context.Background()) }()

	dir, err := os.MkdirTemp("", "sustained-bench-")
	if err != nil {
		return err
	}
	// keepDir: the data directory is left in place when abandoned work may still own the store (it is named in the reason).
	keepDir := false
	defer func() {
		if keepDir {
			return
		}
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
	// over: the loop has ended, so a background reopen still waiting for the lock does nothing (the final reopen is exempt).
	var over atomic.Bool
	// noClose: a retention is still running at the end, and closing under it would be a bug.
	var noClose atomic.Bool
	closeStore := func() (cerr error) {
		if noClose.Load() { // ownership first: s and closed belong to abandoned work once this is set
			return nil
		}
		if s == nil || closed {
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

	// floor: the offset below which retention may have dropped records. failure: the first engine failure, kept apart from
	// whichever stop was admitted first. cur: the bucket's histogram (histMu makes a rotation wait for writers of the old
	// one). reopens and finalReopenMS are written by a reopen under the gate and read by the final line as atomic snapshots.
	var (
		next, acked   atomic.Int64
		maxOffset     atomic.Uint64
		floor         atomic.Uint64
		stop          atomic.Pointer[stopInfo]
		failure       atomic.Pointer[stopInfo]
		cur           = new(latHist)
		histMu        sync.RWMutex
		total         = new(latHist)
		reopens       atomic.Int64
		finalReopenMS atomic.Pointer[string]
		sampMu        sync.Mutex
		strided       []pair
		ring          [1000]pair
		ringN         int
	)
	setStop := func(kind, text string) { stop.CompareAndSwap(nil, &stopInfo{kind, text, time.Now()}) }
	// An engine failure always counts as the result, even when the window (or the target) was admitted first: an
	// in-flight publish that fails after the stop is still a failed engine, never a normal window.
	setFailure := func(text string) {
		failure.CompareAndSwap(nil, &stopInfo{StopFailed, text, time.Now()})
		setStop(StopFailed, text)
	}
	stride := max(o.Target/2000, 1)
	// guarded is the host memory guard that also records the available memory at the cut-off, for the final line.
	// The FIRST trip is latched (memTrip and cutoffKiB): memory that recovers before the run returns cannot erase it.
	var cutoffKiB atomic.Uint64
	var memTrip atomic.Pointer[stopInfo]
	guarded := func() error {
		k, e := checkMem()
		if e != nil {
			cutoffKiB.CompareAndSwap(0, max(k, 1)) // the reading that tripped it
			memTrip.CompareAndSwap(nil, &stopInfo{StopMemory, fmt.Sprintf("stopped by the host memory guard at %d acknowledged (%v)", acked.Load(), e), time.Now()})
		}
		return e
	}

	start := time.Now()
	deadline := start.Add(o.Window) // monotonic: producers check it themselves, so slow telemetry cannot extend the window
	var wg sync.WaitGroup
	for range o.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 512)
			for {
				// The stop, the deadline and the target are checked UNDER the read lock: a publish that queued behind a reopen
				// must not start once the window has ended (or the run was stopped) while it waited.
				gate.RLock()
				if stop.Load() != nil {
					gate.RUnlock()
					return
				}
				if !time.Now().Before(deadline) {
					setStop(StopWindow, fmt.Sprintf("window of %v ended at %d acknowledged", o.Window, acked.Load()))
					gate.RUnlock()
					return
				}
				i := next.Add(1) - 1
				if i >= o.Target {
					gate.RUnlock()
					return
				}
				binary.BigEndian.PutUint64(data, uint64(i))
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
				histMu.RLock()
				cur.add(d)
				histMu.RUnlock()
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
		if u, e := usedFn(dir); e == nil {
			used = strconv.FormatUint(u, 10)
		}
		if f, e := freeFn(dir); e == nil {
			free = strconv.FormatUint(f, 10)
		}
		return
	}
	// diskTextBounded is diskText with a deadline, for the final line: a slow disk walk does not hold the receipt back.
	diskTextBounded := func() (used, free string) {
		ch := make(chan [2]string, 1)
		go func() { u, f := diskText(); ch <- [2]string{u, f} }()
		select {
		case r := <-ch:
			return r[0], r[1]
		case <-time.After(bound):
			return "unmeasured", "unmeasured"
		}
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
		// Two DIFFERENT records acknowledged at one offset is the engine's failure, not a duplicate.
		seen := make(map[uint64]int64, len(pairs))
		var conflict *pair
		pairs = slices.DeleteFunc(pairs, func(p pair) bool {
			if i, ok := seen[p.off]; ok {
				if i != p.i && conflict == nil {
					c := p
					conflict = &c
				}
				return true
			}
			seen[p.off] = p.i
			return false
		})
		if conflict != nil {
			return 0, fmt.Errorf("records %d and %d were both acknowledged at offset %d (one offset for two different records)", seen[conflict.off], conflict.i, conflict.off)
		}
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
		guardEvery := 0
		want := make([]byte, 512) // the whole record: the index in the first 8 bytes, zeros after, as published
		for _, p := range pairs {
			if guardEvery++; guardEvery%64 == 1 { // the host memory guard also covers a long verification
				if me := guarded(); me != nil {
					return 0, memoryCut{me}
				}
			}
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
		if over.Load() && !final { // the run ended while a background reopen waited for the lock: it must not touch the store now
			return nil
		}
		if me := guarded(); me != nil {
			return fail(StopMemory, "stopped by the host memory guard before the reopen at %d acknowledged (%v)", acked.Load(), me)
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
		var cut memoryCut
		if errors.As(e, &cut) {
			return fail(StopMemory, "stopped by the host memory guard during the verification after the reopen at %d acknowledged (%v)", acked.Load(), cut.error)
		}
		if e != nil {
			return fail(StopVerify, "after reopen at %d acknowledged: %v", acked.Load(), e)
		}
		used, _ := diskText()
		fmt.Fprintf(out, "reopen engine=%s at_acked=%d ms=%.2f verified=%d harness_peak_rss_KiB=%d server_peak_rss_KiB=%s disk_KiB=%s\n", engine, acked.Load(), ms, n, maxRSSKiB(), serverPeak(), used)
		if final {
			txt := fmt.Sprintf("%.2f", ms)
			finalReopenMS.Store(&txt)
		} else {
			reopens.Add(1)
		}
		return nil
	}

	var retaining atomic.Bool
	// retentionWG joins all background work (retention, interval reopens, bucket telemetry); fenced: no new one starts once the run is over.
	var retentionWG sync.WaitGroup
	var fenced atomic.Bool
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
	// Telemetry and interval reopens run off the coordinator's critical path: a slow disk walk, or a reopen waiting for a
	// hung publish, must never keep the window, the guards or the abandon limit from being served.
	reopenDone := make(chan *stopInfo, 1)
	reopening, telemetryBusy := false, atomic.Bool{}
	for running := true; running; {
		select {
		case <-done:
			running = false
		case <-window.C:
			setStop(StopWindow, fmt.Sprintf("window of %v ended at %d acknowledged", o.Window, acked.Load()))
		case bad := <-reopenDone:
			reopening = false
			if bad != nil {
				setStop(bad.kind, bad.text)
			} else {
				nextReopen += o.ReopenEvery
			}
		case now := <-tick.C:
			if e := guarded(); e != nil {
				setStop(StopMemory, fmt.Sprintf("stopped by the host memory guard at %d acknowledged (%v); the engine had not finished", acked.Load(), e))
				cancel() // a store waiting on a server stops waiting
			} else if kib, e := freeFn(dir); e == nil && o.MinFreeDiskKiB > 0 && kib < o.MinFreeDiskKiB {
				setStop(StopDisk, fmt.Sprintf("disk nearly full at %d acknowledged: %d KiB free, below the %d KiB floor", acked.Load(), kib, o.MinFreeDiskKiB))
			}
			if st := stop.Load(); st != nil && now.Sub(st.at) > abandonAfter {
				abandoned, running = true, false
				break
			}
			if stop.Load() != nil {
				continue
			}
			if !now.Before(bucketEnd) && telemetryBusy.CompareAndSwap(false, true) {
				histMu.Lock()
				h := cur
				cur = new(latHist) // no writer is inside the old one once the lock is held
				histMu.Unlock()
				a, n, prevAcked, prevAt := acked.Load(), bucketN+1, lastAcked, lastBucket
				bucketN, lastAcked, lastBucket = n, a, now
				for !bucketEnd.After(now) {
					bucketEnd = bucketEnd.Add(o.Bucket)
				}
				retentionWG.Add(1)
				go func() {
					defer retentionWG.Done()
					defer telemetryBusy.Store(false)
					used, free := diskText()
					gate.RLock() // the store is replaced only under the write lock
					sp := serverPeak()
					gate.RUnlock()
					fmt.Fprintf(out, "bucket engine=%s n=%d t_s=%.0f acked=%d msgs_per_s=%.0f p99_us=%s quantiles=bin-upper-1pct harness_peak_rss_KiB=%d server_peak_rss_KiB=%s disk_KiB=%s avail_mem_KiB=%s avail_disk_KiB=%s\n",
						engine, n, now.Sub(start).Seconds(), a, float64(a-prevAcked)/now.Sub(prevAt).Seconds(), us3(h.quantile(0.99)), maxRSSKiB(), sp, used, availMem(), free)
				}()
			}
			if o.KeepLast > 0 && !now.Before(retainAt) && !fenced.Load() && retaining.CompareAndSwap(false, true) {
				retainAt = now.Add(o.RetainEvery)
				retentionWG.Add(1)
				go retain()
			}
			if o.ReopenEvery > 0 && !reopening && !fenced.Load() && acked.Load() >= nextReopen {
				reopening = true
				retentionWG.Add(1)
				go func() { defer retentionWG.Done(); reopenDone <- reopen(false) }()
			}
		}
	}
	elapsed := time.Since(start)
	// Retention is fenced and joined BEFORE anything closes the store: a Close that overlaps an active Retain is a bug of
	// the harness. A retention still blocked after abandonAfter is reported and the store is then left unclosed.
	// A guard that can cancel across the final phases (reopen, verification, close), not just between calls.
	guardStop := make(chan struct{})
	guardDone := make(chan struct{})
	go func() {
		defer close(guardDone)
		t := time.NewTicker(tickEvery)
		defer t.Stop()
		for {
			select {
			case <-guardStop:
				return
			case <-t.C:
				if e := guarded(); e != nil {
					cancel() // a store waiting on a server stops waiting
					return
				}
			}
		}
	}()
	fenced.Store(true)
	over.Store(true)
	retentionDone := make(chan struct{})
	go func() { retentionWG.Wait(); close(retentionDone) }()
	retentionStuck := false
	select {
	case <-retentionDone:
	case <-time.After(abandonAfter):
		retentionStuck = true
	}
	var late *stopInfo // the result of an interval reopen that finished after the loop ended
	select {
	case late = <-reopenDone:
	default:
	}
	st := stop.Load()
	if late != nil && (st == nil || st.kind == StopWindow || st.kind == StopReached) {
		st = late
	}
	if f := failure.Load(); f != nil && (st == nil || st.kind == StopWindow || st.kind == StopReached) {
		st = f // a failed publish after the window or the target was admitted still makes this a failed run
	}
	if st == nil && acked.Load() >= o.Target {
		st = &stopInfo{StopReached, fmt.Sprintf("reached %d acknowledged messages", acked.Load()), time.Now()}
	}
	if st == nil {
		st = &stopInfo{StopFailed, "ended without a recorded reason", time.Now()}
	}
	if st.kind == StopWindow {
		// Publishes in flight when the window closed finished and were counted: the reason carries the FINAL number.
		st = &stopInfo{st.kind, fmt.Sprintf("window of %v ended at %d acknowledged", o.Window, acked.Load()), st.at}
	}
	if abandoned {
		// A publish that never settled may or may not have been stored: not a normal stop. The store may still be owned by
		// that publish, so nothing is closed, the data directory is left in place, and no live store field is read.
		noClose.Store(true)
		keepDir = true
		st = &stopInfo{StopFailed, st.text + "; ambiguous: producers still inside a publish after " + abandonAfter.String() + " were abandoned; the data directory " + dir + " was left in place", st.at}
	}
	if retentionStuck {
		noClose.Store(true)
		keepDir = true
		st = &stopInfo{StopFailed, st.text + "; retention or a reopen still running after " + abandonAfter.String() + ", the store was not closed; the data directory " + dir + " was left in place", st.at}
	}
	owned := !abandoned && !retentionStuck // only then is the store ours to reopen, read and close
	if (st.kind == StopReached || st.kind == StopWindow) && owned {
		// A finished run closes, reopens and verifies once more; a problem there is the result.
		if bad := reopen(true); bad != nil {
			st = bad
		}
	}
	harness, server := maxRSSKiB(), "unmeasured"
	if owned {
		server = serverPeak()
	}
	used, free := diskTextBounded()
	if e := closeStore(); e != nil && st.kind != StopFailed {
		st = &stopInfo{StopFailed, st.text + "; close: " + e.Error(), st.at}
	}
	close(guardStop)
	<-guardDone
	// Checked once more before a normal completion is reported: memory that fell during the final close is still a cut-off.
	guarded()
	if t := memTrip.Load(); t != nil && (st.kind == StopReached || st.kind == StopWindow) {
		st = t // a trip at ANY point of the run counts, even if memory has recovered since
	}
	// mem_cutoff_KiB is the configured threshold; the reading that tripped it is a separate key.
	cut, atTrip := "none", "none"
	if k := cutoffKiB.Load(); k > 0 && st.kind == StopMemory {
		cut, atTrip = strconv.FormatUint(minFree, 10), strconv.FormatUint(k, 10)
	}
	// The data directory is removed BEFORE the receipt: a receipt for a normal run means the run is clean, files and all.
	if !keepDir {
		if rerr := removeAll(dir); rerr != nil {
			st = &stopInfo{StopFailed, st.text + "; removing the data directory: " + rerr.Error(), st.at}
		}
		keepDir = true // already done; the deferred removal has nothing left to do
	}
	// The final line and the shut-off of the writer are ONE step: nothing can print after the receipt.
	finalMS := "none"
	if p := finalReopenMS.Load(); p != nil {
		finalMS = *p
	}
	lw.finish(fmt.Sprintf("sustained engine=%s workers=%d target=%d window_s=%.0f stop=%s acked=%d seconds=%.1f msgs_per_s=%.0f p99_us=%s quantiles=bin-upper-1pct reopens=%d final_reopen_ms=%s harness_peak_rss_KiB=%d server_peak_rss_KiB=%s disk_KiB=%s avail_mem_KiB=%s avail_disk_KiB=%s mem_cutoff_KiB=%s mem_avail_at_trip_KiB=%s reason=%q\n",
		engine, o.Workers, o.Target, o.Window.Seconds(), st.kind, acked.Load(), elapsed.Seconds(), float64(acked.Load())/elapsed.Seconds(), us3(total.quantile(0.99)), reopens.Load(), finalMS, harness, server, used, availMem(), free, cut, atTrip, st.text))
	return nil
}
