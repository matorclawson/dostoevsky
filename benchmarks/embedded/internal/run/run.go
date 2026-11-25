// Package run drives identical Linux workloads in separate engine binaries.
package run

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
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
	// ServerPeakKiB is the sum of the highest sampled VmHWM of every process in
	// the server tree. It returns an error when memory could not be measured; the
	// trial then fails instead of reporting zero.
	ServerPeakKiB() (uint64, error)
}

// Cancelable is implemented by a Store whose verification can wait on a server.
// The harness gives it a context that is cancelled when the memory guard trips
// (and when the trial ends), and the store must stop waiting when it is done.
type Cancelable interface {
	BindContext(context.Context)
}

// trialCtx is the current trial's context: cancelled when the memory guard trips
// and when the trial ends. A process runs one trial, so one package variable is
// enough; commands use it for every blocking call to a server, including open.
var (
	trialMu  sync.Mutex
	trialCtx = context.Background()
)

// Context returns the current trial's context.
func Context() context.Context {
	trialMu.Lock()
	defer trialMu.Unlock()
	return trialCtx
}

func setContext(ctx context.Context) {
	trialMu.Lock()
	trialCtx = ctx
	trialMu.Unlock()
}

// Within runs f, which has no context form, in a goroutine and waits for it for at
// most d and never past ctx. When the wait ends first the goroutine is left to
// finish on its own (it is released when the server's process is killed); the
// caller gets an error and must not rely on f having run to completion.
func Within(ctx context.Context, d time.Duration, f func() error) error {
	done := make(chan error, 1)
	go func() { done <- f() }()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("stopped: %w", ctx.Err())
	case <-timer.C:
		return fmt.Errorf("no answer within %v", d)
	}
}

// minFreeKiB is the approved low-memory abort: a trial stops, as a failure, when
// the host's available memory falls below 300 MiB before or during the trial.
var minFreeKiB uint64 = 300 << 10

// memAvailable is a hook so tests can simulate memory pressure.
var memAvailable = readMemAvailableKiB

// removeAll is a hook so tests can simulate a trial directory that cannot be removed.
var removeAll = os.RemoveAll

var errNoMeminfo = errors.New("available memory is not measured on this platform")

func readMemAvailableKiB() (uint64, error) {
	if runtime.GOOS != "linux" {
		return 0, errNoMeminfo
	}
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "MemAvailable:"); ok {
			f := strings.Fields(rest)
			if len(f) >= 1 {
				return strconv.ParseUint(f[0], 10, 64)
			}
		}
	}
	return 0, errors.New("MemAvailable not found in /proc/meminfo")
}

func checkMemory() error {
	kib, err := memAvailable()
	if errors.Is(err, errNoMeminfo) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read available memory: %w", err)
	}
	if kib < minFreeKiB {
		return fmt.Errorf("low-memory abort: %d KiB available, below %d KiB", kib, minFreeKiB)
	}
	return nil
}

// cutoffStatus is recorded on every receipt line: the abort threshold in KiB, or
// "unavailable" where the platform cannot report available memory (the cutoff
// then did not apply to the trial).
func cutoffStatus() string {
	if _, err := memAvailable(); errors.Is(err, errNoMeminfo) {
		return "unavailable"
	}
	return strconv.FormatUint(minFreeKiB, 10)
}

func Main(engine string, open func(string, bool) (Store, error)) {
	count := flag.Int("count", 2000, "records per run")
	workers := flag.Int("workers", 1, "concurrent synchronous producers")
	flag.Parse()
	if err := execute(engine, open, *count, *workers, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "FAILED:", err)
		os.Exit(1)
	}
}

// execute runs one trial. Every failure path returns an error instead of
// exiting, so the store is closed (stopping any server), the workers are joined
// and the trial's data directory is removed before the process ends.
func execute(engine string, open func(string, bool) (Store, error), count, workers int, out io.Writer) (err error) {
	if count < 1 || workers < 1 || workers > count {
		return errors.New("invalid workload")
	}
	if err = checkMemory(); err != nil {
		return err
	}
	cutoff := cutoffStatus()
	// The memory guard is the first thing that runs and the last thing that is
	// joined, so every phase (open, publish, reopen, verify, close) is inside one
	// monitored span. tripped() is checked after each phase.
	ctx, cancel := context.WithCancel(context.Background())
	setContext(ctx)
	defer func() { cancel(); setContext(context.Background()) }()
	var guardErr atomic.Pointer[error]
	stopGuard := make(chan struct{})
	guardDone := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() { close(stopGuard) })
		<-guardDone
	}
	defer stop()
	go func() {
		defer close(guardDone)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopGuard:
				return
			case <-t.C:
				if e := checkMemory(); e != nil {
					guardErr.Store(&e)
					cancel() // a store waiting on a server stops waiting
					return
				}
			}
		}
	}()

	tripped := func() error {
		if ge := guardErr.Load(); ge != nil {
			return *ge
		}
		return nil
	}
	dir, err := os.MkdirTemp("", "embedded-bench-")
	if err != nil {
		return err
	}
	defer func() {
		if rerr := removeAll(dir); rerr != nil && err == nil {
			err = rerr
		}
	}()
	var s Store
	bind := func() {
		if b, ok := s.(Cancelable); ok {
			b.BindContext(ctx)
		}
	}
	closed := true
	closeStore := func() (cerr error) {
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
	defer func() {
		if cerr := closeStore(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	if s, err = open(dir, true); err != nil {
		return err
	}
	closed = false
	bind()
	if err = tripped(); err != nil {
		return err
	}

	delays := make([]time.Duration, count)
	seq := make([]uint64, count)
	var next atomic.Int64
	var firstErr atomic.Pointer[error]
	fail := func(e error) { firstErr.CompareAndSwap(nil, &e) }
	var wg sync.WaitGroup
	start := time.Now()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 512)
			for {
				if firstErr.Load() != nil {
					return
				}
				if ge := guardErr.Load(); ge != nil {
					fail(*ge)
					return
				}
				i := int(next.Add(1) - 1)
				if i >= count {
					return
				}
				binary.BigEndian.PutUint64(data, uint64(i))
				begin := time.Now()
				offset, e := s.Publish(data)
				delays[i] = time.Since(begin)
				if e != nil {
					// Never retried: an ambiguous publish is a failed trial.
					fail(fmt.Errorf("publish %d: %w", i, e))
					return
				}
				seq[i] = offset
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if e := firstErr.Load(); e != nil {
		return *e
	}
	if ge := guardErr.Load(); ge != nil {
		return *ge
	}
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var usage syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return err
	}
	var server uint64
	measured := false
	if m, ok := s.(Measured); ok {
		kib, e := m.ServerPeakKiB()
		if e != nil {
			return fmt.Errorf("server memory: %w", e)
		}
		server, measured = kib, true
	}
	if err = closeStore(); err != nil {
		return err
	}
	if err = tripped(); err != nil {
		return err
	}
	start = time.Now()
	if s, err = open(dir, false); err != nil {
		return err
	}
	closed = false
	bind()
	reopen := time.Since(start)
	if err = tripped(); err != nil {
		return err
	}
	want := make([]byte, 512)
	readStart := time.Now()
	for i, offset := range seq {
		binary.BigEndian.PutUint64(want, uint64(i))
		got, e := s.Read(offset)
		if e != nil || !bytes.Equal(got, want) {
			return fmt.Errorf("record %d at %d: %v", i, offset, e)
		}
		if err = tripped(); err != nil {
			return err
		}
	}
	readElapsed := time.Since(readStart)
	sort.Slice(delays, func(i, j int) bool { return delays[i] < delays[j] })
	line := fmt.Sprintf("engine=%s workers=%d verified=%d records/s=%.0f p95_us=%d p99_us=%d liveHeap_KiB=%d peakRSS_KiB=%d reopen_ms=%.2f verified_reads_per_s=%.0f", engine, workers, count, float64(count)/elapsed.Seconds(), delays[(95*len(delays)-1)/100].Microseconds(), delays[(99*len(delays)-1)/100].Microseconds(), mem.HeapAlloc/1024, usage.Maxrss, float64(reopen)/1e6, float64(count)/readElapsed.Seconds())
	if measured {
		// peakRSS_KiB stays the harness process alone. total_rss_KiB adds the server
		// tree: a sum of sampled per-process peaks that can occur at different times
		// and count shared pages once per process.
		line += fmt.Sprintf(" server_rss_KiB=%d total_rss_KiB=%d arch=server", server, uint64(usage.Maxrss)+server)
	}
	// The guard keeps running through the final Close, so server shutdown is
	// monitored too. Success is printed only after the Close succeeded, the guard
	// goroutine has exited (a sample still in flight cannot arrive after the last
	// check) and it reported clean. Every worker was joined by wg.Wait above.
	if err = closeStore(); err != nil {
		return err
	}
	// The trial directory goes before the guard is joined and before any receipt,
	// so a removal failure is a failed trial, not a success followed by an error.
	if err = removeAll(dir); err != nil {
		return err
	}
	stop()
	if err = tripped(); err != nil {
		return err
	}
	fmt.Fprintln(out, line+" mem_cutoff_KiB="+cutoff)
	return nil
}
