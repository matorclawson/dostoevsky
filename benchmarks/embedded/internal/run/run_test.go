package run

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type shared struct {
	mu   sync.Mutex
	data map[uint64][]byte
	n    uint64
}

type fake struct {
	s          *shared
	failAt     uint64 // publish with this sequence number fails (0 = never)
	slow       time.Duration
	serverErr  error
	closeCount *int32
	onRead     func()        // called at the start of every Read
	panicAt    int32         // Close number that panics (0 = never)
	onClose    func(n int32) // called at the start of every Close with its number
	mu         sync.Mutex
	bound      context.Context
}

func (f *fake) Publish(b []byte) (uint64, error) {
	if f.slow > 0 {
		time.Sleep(f.slow)
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	f.s.n++
	if f.failAt != 0 && f.s.n == f.failAt {
		return 0, errors.New("injected publish failure")
	}
	f.s.data[f.s.n] = append([]byte(nil), b...)
	return f.s.n, nil
}

func (f *fake) Read(o uint64) ([]byte, error) {
	if f.onRead != nil {
		f.onRead()
	}
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.data[o], nil
}

func (f *fake) BindContext(ctx context.Context) {
	f.mu.Lock()
	f.bound = ctx
	f.mu.Unlock()
}

func (f *fake) boundContext() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bound
}

func (f *fake) Close() {
	n := atomic.AddInt32(f.closeCount, 1)
	if f.onClose != nil {
		f.onClose(n)
	}
	if f.panicAt != 0 && n == f.panicAt {
		panic("injected close failure")
	}
}

type measuredFake struct{ *fake }

func (m measuredFake) ServerPeakKiB() (uint64, error) { return 0, m.serverErr }

func opener(f *fake, measured bool) func(string, bool) (Store, error) {
	return func(string, bool) (Store, error) {
		if measured {
			return measuredFake{f}, nil
		}
		return f, nil
	}
}

func newFake() *fake {
	return &fake{s: &shared{data: map[uint64][]byte{}}, closeCount: new(int32)}
}

// withTemp points TMPDIR at an empty directory and reports whether it is still empty.
func withTemp(t *testing.T) func() int {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	return func() int {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
}

func setMemory(t *testing.T, f func() (uint64, error)) {
	t.Helper()
	old := memAvailable
	memAvailable = f
	t.Cleanup(func() { memAvailable = old })
}

func TestSuccessfulTrialPrintsAReceiptLine(t *testing.T) {
	left := withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	f := newFake()
	var out bytes.Buffer
	if err := execute("fake", opener(f, false), 20, 4, &out); err != nil {
		t.Fatal(err)
	}
	line := out.String()
	for _, want := range []string{"engine=fake ", "workers=4 ", "verified=20 ", "records/s=", "peakRSS_KiB=", "reopen_ms="} {
		if !strings.Contains(line, want) {
			t.Errorf("receipt line %q lacks %q", line, want)
		}
	}
	if strings.Contains(line, "server_rss_KiB") {
		t.Error("an in-process engine reported server memory")
	}
	if left() != 0 {
		t.Error("trial data was not removed")
	}
	if got := atomic.LoadInt32(f.closeCount); got != 2 {
		t.Errorf("store closed %d times, want 2 (before and after the reopen)", got)
	}
}

// A failed publish must stop the other workers, close the store (which stops a
// server) and remove the trial's data: no deferred cleanup may be skipped.
func TestPublishFailureCleansUp(t *testing.T) {
	for _, workers := range []int{1, 4} {
		left := withTemp(t)
		setMemory(t, func() (uint64, error) { return 1 << 30, nil })
		f := newFake()
		f.failAt = 5
		var out bytes.Buffer
		const count = 100000
		err := execute("fake", opener(f, false), count, workers, &out)
		if err == nil || !strings.Contains(err.Error(), "injected publish failure") {
			t.Fatalf("%d workers: error %v", workers, err)
		}
		if out.Len() != 0 {
			t.Errorf("%d workers: a failed trial printed a receipt line: %q", workers, out.String())
		}
		if left() != 0 {
			t.Errorf("%d workers: trial data was not removed after the failure", workers)
		}
		if got := atomic.LoadInt32(f.closeCount); got != 1 {
			t.Errorf("%d workers: store closed %d times, want exactly 1", workers, got)
		}
		// The failing publish is not retried and the workers stop. One worker stops
		// exactly at the failure; several can finish publishes already in flight, or
		// started before the failure is recorded, but never run on to the end.
		if workers == 1 && f.s.n != 5 {
			t.Errorf("one worker ran %d publishes, want exactly 5", f.s.n)
		}
		if f.s.n >= count/10 {
			t.Errorf("%d workers: %d of %d publishes ran; workers did not stop after the failure", workers, f.s.n, count)
		}
	}
}

func TestLowMemoryBeforeTheTrialAbortsWithoutStartingIt(t *testing.T) {
	left := withTemp(t)
	setMemory(t, func() (uint64, error) { return 100 << 10, nil })
	opened := false
	err := execute("fake", func(string, bool) (Store, error) { opened = true; return newFake(), nil }, 10, 1, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("error %v", err)
	}
	if opened || left() != 0 {
		t.Error("a trial was started or left data behind")
	}
}

func TestLowMemoryDuringTheTrialAbortsAndCleansUp(t *testing.T) {
	left := withTemp(t)
	var calls atomic.Int32
	setMemory(t, func() (uint64, error) {
		if calls.Add(1) == 1 {
			return 1 << 30, nil // the pre-trial check passes
		}
		return 100 << 10, nil // pressure appears during the trial
	})
	f := newFake()
	f.slow = 20 * time.Millisecond
	err := execute("fake", opener(f, false), 500, 1, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("error %v", err)
	}
	if left() != 0 || atomic.LoadInt32(f.closeCount) != 1 {
		t.Errorf("cleanup incomplete: %d entries left, %d closes", left(), atomic.LoadInt32(f.closeCount))
	}
}

func TestUnreadableMemoryOnLinuxFailsTheTrial(t *testing.T) {
	withTemp(t)
	setMemory(t, func() (uint64, error) { return 0, errors.New("procfs gone") })
	if err := execute("fake", opener(newFake(), false), 10, 1, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "cannot read available memory") {
		t.Fatalf("error %v", err)
	}
}

// A server whose memory could not be measured fails the trial, and the store is still closed.
func TestUnmeasuredServerFailsTheTrial(t *testing.T) {
	left := withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	f := newFake()
	f.serverErr = errors.New("server memory unavailable")
	var out bytes.Buffer
	err := execute("fake", opener(f, true), 10, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "server memory unavailable") {
		t.Fatalf("error %v", err)
	}
	if out.Len() != 0 || left() != 0 || atomic.LoadInt32(f.closeCount) != 1 {
		t.Errorf("output %q, %d entries left, %d closes", out.String(), left(), atomic.LoadInt32(f.closeCount))
	}
}

func TestMeasuredServerReportsTotals(t *testing.T) {
	withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	var out bytes.Buffer
	if err := execute("fake", opener(newFake(), true), 10, 1, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "server_rss_KiB=0 total_rss_KiB=") || !strings.Contains(out.String(), "arch=server") {
		t.Fatalf("line %q", out.String())
	}
}

// A low-memory event while the reopened store is being verified fails the
// trial even though every record verifies.
func TestLowMemoryDuringVerificationFailsTheTrial(t *testing.T) {
	left := withTemp(t)
	var low atomic.Bool
	setMemory(t, func() (uint64, error) {
		if low.Load() {
			return 100 << 10, nil
		}
		return 1 << 30, nil
	})
	f := newFake()
	var once sync.Once
	f.onRead = func() {
		once.Do(func() {
			low.Store(true)
			time.Sleep(400 * time.Millisecond) // the guard samples every 100 ms
		})
	}
	var out bytes.Buffer
	err := execute("fake", opener(f, false), 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("error %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("a failed trial printed a receipt line: %q", out.String())
	}
	if left() != 0 || atomic.LoadInt32(f.closeCount) != 2 {
		t.Errorf("cleanup incomplete: %d entries left, %d closes", left(), atomic.LoadInt32(f.closeCount))
	}
}

// A failure of the final Close must not follow a success line.
func TestCloseFailureAfterVerificationPrintsNoReceipt(t *testing.T) {
	left := withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	f := newFake()
	f.panicAt = 2 // the Close after verification
	var out bytes.Buffer
	err := execute("fake", opener(f, false), 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "injected close failure") {
		t.Fatalf("error %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("a receipt line was printed before the final Close failed: %q", out.String())
	}
	if left() != 0 {
		t.Error("trial data was not removed")
	}
}

// The guard's last sample can return after verification has finished. Success
// must wait for the guard to exit and then see its result.
func TestLateGuardSampleStillFailsTheTrial(t *testing.T) {
	left := withTemp(t)
	var low atomic.Bool
	release := make(chan struct{})
	setMemory(t, func() (uint64, error) {
		if low.Load() {
			<-release // this sample is in flight when verification returns
			return 100 << 10, nil
		}
		return 1 << 30, nil
	})
	f := newFake()
	var once sync.Once
	f.onRead = func() {
		once.Do(func() {
			low.Store(true)
			time.Sleep(150 * time.Millisecond)                              // the guard starts a sample and blocks in it
			time.AfterFunc(300*time.Millisecond, func() { close(release) }) // delivered only after verification
		})
	}
	var out bytes.Buffer
	err := execute("fake", opener(f, false), 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("a late low-memory sample was not reported: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("a success line was printed before the guard was joined: %q", out.String())
	}
	if left() != 0 || atomic.LoadInt32(f.closeCount) != 2 {
		t.Errorf("cleanup incomplete: %d entries left, %d closes", left(), atomic.LoadInt32(f.closeCount))
	}
}

func TestReceiptStatesTheMemoryCutoff(t *testing.T) {
	withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	var out bytes.Buffer
	if err := execute("fake", opener(newFake(), false), 5, 1, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), " mem_cutoff_KiB=307200") {
		t.Errorf("line %q lacks the cutoff", out.String())
	}
	// Where available memory is not measured the cutoff did not apply, and the line says so.
	setMemory(t, func() (uint64, error) { return 0, errNoMeminfo })
	out.Reset()
	if err := execute("fake", opener(newFake(), false), 5, 1, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), " mem_cutoff_KiB=unavailable") {
		t.Errorf("line %q does not say the cutoff was unavailable", out.String())
	}
}

// Server shutdown is part of the trial: a low-memory event while the final Close
// runs fails it, and no receipt is printed.
func TestLowMemoryDuringTheFinalCloseFailsTheTrial(t *testing.T) {
	left := withTemp(t)
	var low atomic.Bool
	setMemory(t, func() (uint64, error) {
		if low.Load() {
			return 100 << 10, nil
		}
		return 1 << 30, nil
	})
	f := newFake()
	f.onClose = func(n int32) {
		if n == 2 { // the Close after verification
			low.Store(true)
			time.Sleep(350 * time.Millisecond) // the guard samples every 100 ms
		}
	}
	var out bytes.Buffer
	err := execute("fake", opener(f, false), 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("a low-memory event during Close was not reported: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("a receipt was printed although shutdown ran under memory pressure: %q", out.String())
	}
	if left() != 0 {
		t.Error("trial data was not removed")
	}
}

// The first open is inside the monitored span: pressure while a store opens
// fails the trial even if the store then works.
func TestLowMemoryDuringTheFirstOpenFailsTheTrial(t *testing.T) {
	left := withTemp(t)
	var low atomic.Bool
	setMemory(t, func() (uint64, error) {
		if low.Load() {
			return 100 << 10, nil
		}
		return 1 << 30, nil
	})
	f := newFake()
	opens := 0
	open := func(string, bool) (Store, error) {
		opens++
		if opens == 1 {
			low.Store(true)
			time.Sleep(350 * time.Millisecond) // the guard samples every 100 ms
		}
		return f, nil
	}
	var out bytes.Buffer
	err := execute("fake", open, 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("a low-memory event during the first open was not reported: %v", err)
	}
	if out.Len() != 0 || opens != 1 {
		t.Errorf("output %q after %d opens; the trial must stop right after the first open", out.String(), opens)
	}
	if left() != 0 || atomic.LoadInt32(f.closeCount) != 1 {
		t.Errorf("cleanup incomplete: %d entries left, %d closes", left(), atomic.LoadInt32(f.closeCount))
	}
}

// A trial directory that cannot be removed is a failed trial: no receipt line.
func TestRemovalFailureIsAFailedTrial(t *testing.T) {
	withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	old := removeAll
	var calls atomic.Int32
	removeAll = func(path string) error {
		calls.Add(1)
		return errors.New("injected removal failure")
	}
	t.Cleanup(func() { removeAll = old })
	var out bytes.Buffer
	err := execute("fake", opener(newFake(), false), 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "injected removal failure") {
		t.Fatalf("error %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("a receipt was printed although the trial directory could not be removed: %q", out.String())
	}
	if calls.Load() < 1 {
		t.Error("removal was never attempted")
	}
}

// A store that waits on a server is handed a context that the memory guard
// cancels, so a stalled verification read ends at once instead of holding the
// trial open.
func TestGuardCancelsAStalledVerificationRead(t *testing.T) {
	left := withTemp(t)
	var low atomic.Bool
	setMemory(t, func() (uint64, error) {
		if low.Load() {
			return 100 << 10, nil
		}
		return 1 << 30, nil
	})
	f := newFake()
	var once sync.Once
	f.onRead = func() {
		once.Do(func() {
			low.Store(true)
			select {
			case <-f.boundContext().Done(): // the guard trips and cancels the context
			case <-time.After(10 * time.Second):
				t.Error("the bound context was never cancelled")
			}
		})
	}
	start := time.Now()
	var out bytes.Buffer
	err := execute("fake", opener(f, false), 5, 1, &out)
	if err == nil || !strings.Contains(err.Error(), "low-memory abort") {
		t.Fatalf("error %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the stalled read held the trial open")
	}
	if out.Len() != 0 || left() != 0 {
		t.Errorf("output %q, %d entries left", out.String(), left())
	}
}
