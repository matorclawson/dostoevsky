package run

import (
	"bytes"
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
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.data[o], nil
}

func (f *fake) Close() { atomic.AddInt32(f.closeCount, 1) }

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
	left := withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	f := newFake()
	f.failAt = 5
	var out bytes.Buffer
	err := execute("fake", opener(f, false), 50, 4, &out)
	if err == nil || !strings.Contains(err.Error(), "injected publish failure") {
		t.Fatalf("error %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("a failed trial printed a receipt line: %q", out.String())
	}
	if left() != 0 {
		t.Error("trial data was not removed after the failure")
	}
	if got := atomic.LoadInt32(f.closeCount); got != 1 {
		t.Errorf("store closed %d times, want exactly 1", got)
	}
	// The failure is not retried: the failing sequence number was used once.
	if f.s.n < 5 || f.s.n > 5+4 {
		t.Errorf("%d publishes ran; workers did not stop after the failure", f.s.n)
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
