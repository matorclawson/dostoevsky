package run

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fastTick(t *testing.T) {
	t.Helper()
	old := tickEvery
	tickEvery = 10 * time.Millisecond
	t.Cleanup(func() { tickEvery = old })
}

func sustainedRun(t *testing.T, store func() Store, o Sustained) (lines []string, final map[string]string) {
	t.Helper()
	withTemp(t)
	fastTick(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	var out bytes.Buffer
	if err := executeSustained("fake", func(string, bool) (Store, error) { return store(), nil }, o, &out); err != nil {
		t.Fatalf("executeSustained: %v", err)
	}
	lines = strings.Split(strings.TrimSpace(out.String()), "\n")
	last := lines[len(lines)-1]
	if !strings.HasPrefix(last, "sustained ") {
		t.Fatalf("the last line is not the result line: %q", last)
	}
	return lines, fields(last)
}

// fields parses key=value pairs; a quoted reason="..." runs to the closing quote.
func fields(line string) map[string]string {
	m := map[string]string{}
	if i := strings.Index(line, ` reason="`); i >= 0 {
		m["reason"] = strings.TrimSuffix(strings.TrimPrefix(line[i:], ` reason="`), `"`)
		line = line[:i]
	}
	for _, f := range strings.Fields(line) {
		if k, v, ok := strings.Cut(f, "="); ok {
			m[k] = v
		}
	}
	return m
}

func countPrefix(lines []string, p string) (n int) {
	for _, l := range lines {
		if strings.HasPrefix(l, p) {
			n++
		}
	}
	return
}

func TestSustainedReachesTheTarget(t *testing.T) {
	f := newFake()
	lines, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 600, Workers: 4, Bucket: time.Hour})
	if r["stop"] != StopReached || r["acked"] != "600" || !strings.Contains(r["reason"], "reached 600") {
		t.Fatalf("%v", r)
	}
	if r["final_reopen_ms"] == "none" {
		t.Fatalf("a finished run reopens and verifies at the end: %v", r)
	}
	if countPrefix(lines, "reopen ") != 1 {
		t.Fatalf("lines: %v", lines)
	}
	if n, _ := strconv.Atoi(strings.TrimPrefix(lines[0], "reopen engine=fake at_acked=")[:3]); n != 600 {
		t.Fatalf("the final reopen is at 600 acknowledged: %q", lines[0])
	}
}

func TestSustainedStopsAtTheEndOfTheWindow(t *testing.T) {
	f := newFake()
	f.slow = time.Millisecond
	_, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 300 * time.Millisecond, Target: 1_000_000_000, Workers: 4, Bucket: time.Hour})
	acked, _ := strconv.Atoi(r["acked"])
	if r["stop"] != StopWindow || acked < 50 || acked >= 1_000_000 || !strings.Contains(r["reason"], "window of") {
		t.Fatalf("%v", r)
	}
	if r["final_reopen_ms"] == "none" {
		t.Fatalf("a window that ended normally reopens and verifies: %v", r)
	}
}

func TestSustainedAnEngineFailureIsAResultAndIsNeverRetried(t *testing.T) {
	f := newFake()
	f.failAt = 50
	lines, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 100_000, Workers: 3, Bucket: time.Hour})
	if r["stop"] != StopFailed || !strings.Contains(r["reason"], "injected publish failure") || !strings.Contains(r["reason"], "engine failed at") {
		t.Fatalf("%v", r)
	}
	if r["final_reopen_ms"] != "none" || countPrefix(lines, "reopen ") != 0 {
		t.Fatalf("a failed engine is not reopened: %v", lines)
	}
	// Exactly one publish failed and it is not counted: every other attempt was acknowledged and
	// counted. (How many more publishes other producers squeezed in before they saw the stop is a
	// scheduling matter, so it is not asserted.)
	acked, _ := strconv.Atoi(r["acked"])
	f.s.mu.Lock()
	attempts := int(f.s.n)
	f.s.mu.Unlock()
	if acked < 49 || acked >= 100_000 || attempts != acked+1 {
		t.Fatalf("acked %d of %d attempts: the failing publish must be the only one not counted, and nothing may run to the target", acked, attempts)
	}
}

func TestSustainedHostMemoryGuardIsReportedAsSuch(t *testing.T) {
	withTemp(t)
	fastTick(t)
	var low atomic.Bool
	time.AfterFunc(250*time.Millisecond, func() { low.Store(true) })
	setMemory(t, func() (uint64, error) {
		if low.Load() {
			return 100, nil
		}
		return 1 << 30, nil
	})
	f := newFake()
	f.slow = time.Millisecond
	var out bytes.Buffer
	if err := executeSustained("fake", func(string, bool) (Store, error) { return f, nil }, Sustained{Window: 30 * time.Second, Target: 1_000_000_000, Workers: 2, Bucket: time.Hour}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	r := fields(lines[len(lines)-1])
	if r["stop"] != StopMemory || !strings.Contains(r["reason"], "host memory guard") || !strings.Contains(r["reason"], "the engine had not finished") {
		t.Fatalf("%v", r)
	}
	if r["final_reopen_ms"] != "none" {
		t.Fatalf("a run stopped by the guard is not reopened: %v", r)
	}
}

func TestSustainedDiskFloor(t *testing.T) {
	old := diskFreeKiB
	diskFreeKiB = func(string) (uint64, error) { return 1000, nil }
	t.Cleanup(func() { diskFreeKiB = old })
	f := newFake()
	f.slow = time.Millisecond
	_, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 1_000_000_000, Workers: 2, Bucket: time.Hour, MinFreeDiskKiB: 5000})
	if r["stop"] != StopDisk || !strings.Contains(r["reason"], "disk nearly full") || !strings.Contains(r["reason"], "1000 KiB free") {
		t.Fatalf("%v", r)
	}
}

func TestSustainedReopensAtIntervalsAndVerifies(t *testing.T) {
	f := newFake()
	f.slow = time.Millisecond
	lines, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 3000, Workers: 4, Bucket: time.Hour, ReopenEvery: 1000})
	if r["stop"] != StopReached {
		t.Fatalf("%v", r)
	}
	// at least one interval reopen before the end, and the final one
	if got := countPrefix(lines, "reopen "); got < 2 {
		t.Fatalf("%d reopen lines: %v", got, lines)
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "reopen ") {
			if v, _ := strconv.Atoi(fields(l)["verified"]); v < 1 {
				t.Fatalf("a reopen verified nothing: %q", l)
			}
		}
	}
}

type badRead struct{ *fake }

func (b badRead) Read(o uint64) ([]byte, error) {
	got, err := b.fake.Read(o)
	if err != nil || len(got) == 0 {
		return got, err
	}
	bad := append([]byte(nil), got...)
	bad[3] ^= 0xFF // the sequence number no longer matches
	return bad, nil
}

func TestSustainedWrongDataAfterReopenStopsAsVerificationFailure(t *testing.T) {
	f := newFake()
	_, r := sustainedRun(t, func() Store { return badRead{f} }, Sustained{Window: 30 * time.Second, Target: 500, Workers: 2, Bucket: time.Hour})
	if r["stop"] != StopVerify || !strings.Contains(r["reason"], "read back wrong") {
		t.Fatalf("%v", r)
	}
}

type retaining struct {
	*fake
	mu    sync.Mutex
	calls int
	keep  uint64
}

func (r *retaining) Retain(keepFrom uint64) error {
	r.mu.Lock()
	r.calls++
	r.keep = keepFrom
	r.mu.Unlock()
	r.fake.s.mu.Lock()
	defer r.fake.s.mu.Unlock()
	for off := range r.fake.s.data {
		if off < keepFrom {
			delete(r.fake.s.data, off)
		}
	}
	return nil
}

func TestSustainedRetentionRunsAndVerificationSkipsDroppedRecords(t *testing.T) {
	r := &retaining{fake: newFake()}
	r.fake.slow = 200 * time.Microsecond
	lines, res := sustainedRun(t, func() Store { return r }, Sustained{Window: 30 * time.Second, Target: 4000, Workers: 4, Bucket: time.Hour, KeepLast: 500, RetainEvery: 20 * time.Millisecond})
	r.mu.Lock()
	calls := r.calls
	r.mu.Unlock()
	if calls < 1 || countPrefix(lines, "retain ") < 1 {
		t.Fatalf("retention never ran: %d calls, lines %v", calls, lines)
	}
	if res["stop"] != StopReached {
		t.Fatalf("records below the floor were verified, or the run failed: %v", res)
	}
}

type failingRetain struct{ *fake }

func (failingRetain) Retain(uint64) error { return errors.New("disk said no") }

func TestSustainedRetentionFailureIsTheEnginesFailure(t *testing.T) {
	f := failingRetain{newFake()}
	f.fake.slow = 200 * time.Microsecond
	_, res := sustainedRun(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 1_000_000_000, Workers: 2, Bucket: time.Hour, KeepLast: 10, RetainEvery: 20 * time.Millisecond})
	if res["stop"] != StopFailed || !strings.Contains(res["reason"], "retention: disk said no") {
		t.Fatalf("%v", res)
	}
}

func TestSustainedBucketsReportRateAndP99(t *testing.T) {
	f := newFake()
	f.slow = time.Millisecond
	lines, _ := sustainedRun(t, func() Store { return f }, Sustained{Window: 500 * time.Millisecond, Target: 1_000_000_000, Workers: 4, Bucket: 200 * time.Millisecond})
	if countPrefix(lines, "bucket ") < 2 {
		t.Fatalf("buckets: %v", lines)
	}
	first := fields(lines[0])
	rate, _ := strconv.ParseFloat(first["msgs_per_s"], 64)
	p99, _ := strconv.Atoi(first["p99_us"])
	if first["n"] != "1" || rate <= 0 || p99 < 900 {
		t.Fatalf("bucket line: %q (a 1 ms publish cannot have p99 under 0.9 ms)", lines[0])
	}
}

// a publish that never returns (a hung server) does not hang the run forever
type hangs struct {
	*fake
	release chan struct{}
}

func (h hangs) Publish(b []byte) (uint64, error) {
	<-h.release
	return 0, errors.New("released")
}

func TestSustainedAbandonsProducersStuckInAHungPublish(t *testing.T) {
	old := abandonAfter
	abandonAfter = 400 * time.Millisecond
	t.Cleanup(func() { abandonAfter = old })
	h := hangs{newFake(), make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	_, r := sustainedRun(t, func() Store { return h }, Sustained{Window: 200 * time.Millisecond, Target: 1_000_000_000, Workers: 2, Bucket: time.Hour})
	if r["stop"] != StopWindow || !strings.Contains(r["reason"], "abandoned") || r["final_reopen_ms"] != "none" {
		t.Fatalf("%v", r)
	}
}

func TestSustainedRefusesAnInvalidWorkloadAndALowStart(t *testing.T) {
	withTemp(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	f := newFake()
	for name, o := range map[string]Sustained{
		"no workers":        {Window: time.Second, Target: 10, Workers: 0, Bucket: time.Second},
		"no window":         {Target: 10, Workers: 1, Bucket: time.Second},
		"no target":         {Window: time.Second, Workers: 1, Bucket: time.Second},
		"no bucket":         {Window: time.Second, Target: 10, Workers: 1},
		"keep without tick": {Window: time.Second, Target: 10, Workers: 1, Bucket: time.Second, KeepLast: 5},
	} {
		if err := executeSustained("fake", opener(f, false), o, &bytes.Buffer{}); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	setMemory(t, func() (uint64, error) { return 100, nil })
	if err := executeSustained("fake", opener(f, false), Sustained{Window: time.Second, Target: 10, Workers: 1, Bucket: time.Second}, &bytes.Buffer{}); err == nil {
		t.Error("a run that starts below the memory guard was accepted")
	}
}

func TestSustainedHistogramQuantilesAreWithinOnePercent(t *testing.T) {
	h := new(hist)
	for i := 0; i < 990; i++ {
		h.add(time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		h.add(100 * time.Millisecond)
	}
	p50, p99, p100 := h.quantile(0.5), h.quantile(0.99), h.quantile(1)
	within := func(got, want time.Duration) bool {
		r := float64(got) / float64(want)
		return r >= 0.99 && r <= 1.02
	}
	if !within(p50, time.Millisecond) || !within(p99, time.Millisecond) || !within(p100, 100*time.Millisecond) {
		t.Fatalf("p50 %v p99 %v max %v", p50, p99, p100)
	}
	if new(hist).quantile(0.99) != 0 {
		t.Fatal("an empty histogram has no quantile")
	}
	var huge hist
	huge.add(1000 * time.Hour) // clamps to the last bin instead of indexing out of range
	if huge.quantile(1) <= 0 {
		t.Fatal("a huge latency vanished")
	}
	huge.add(0)
	huge.add(-time.Second)
}
