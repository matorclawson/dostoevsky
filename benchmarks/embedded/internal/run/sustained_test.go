package run

import (
	"bytes"
	"encoding/binary"
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
	return sustainedRunMem(t, store, o, func() (uint64, error) { return 1 << 30, nil })
}

func sustainedRunMem(t *testing.T, store func() Store, o Sustained, mem func() (uint64, error)) (lines []string, final map[string]string) {
	t.Helper()
	withTemp(t)
	fastTick(t)
	setMemory(t, mem)
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
	p99, _ := strconv.ParseFloat(first["p99_us"], 64)
	if dot := strings.IndexByte(first["p99_us"], '.'); dot < 0 || len(first["p99_us"])-dot-1 != 3 {
		t.Fatalf("p99_us must carry three decimals: %q", first["p99_us"])
	}
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
	if r["stop"] != StopFailed || !strings.Contains(r["reason"], "abandoned") || !strings.Contains(r["reason"], "ambiguous") || r["final_reopen_ms"] != "none" {
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
	h := new(latHist)
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
	if new(latHist).quantile(0.99) != 0 {
		t.Fatal("an empty histogram has no quantile")
	}
	var huge latHist
	huge.add(1000 * time.Hour) // clamps to the last bin instead of indexing out of range
	if huge.quantile(1) <= 0 {
		t.Fatal("a huge latency vanished")
	}
	huge.add(0)
	huge.add(-time.Second)
}

// ----- paired review of PR 31 (1f9b19b): one test per finding -----

// M1: a slow bucket telemetry must not let fresh publishes start after the window.
func TestSustainedNoPublishStartsAfterTheWindowEvenWhenTelemetryIsSlow(t *testing.T) {
	old := diskUsedKiB
	diskUsedKiB = func(string) (uint64, error) { time.Sleep(300 * time.Millisecond); return 1, nil } // a slow disk walk
	t.Cleanup(func() { diskUsedKiB = old })
	f := newFake()
	f.slow = time.Millisecond
	window := 100 * time.Millisecond
	lc := &lateCounter{fake: f, window: window + 30*time.Millisecond}
	_, r := sustainedRun(t, func() Store { return lc }, Sustained{Window: window, Target: 1_000_000_000, Workers: 4, Bucket: 20 * time.Millisecond})
	if lc.late.Load() != 0 || r["stop"] != StopWindow {
		t.Fatalf("%d publishes started more than 30 ms after the window; %v", lc.late.Load(), r)
	}
}

// lateCounter counts publishes that START later than `window` after the first one (the run starts just before it).
type lateCounter struct {
	*fake
	window time.Duration
	first  atomic.Int64
	late   atomic.Int64
}

func (l *lateCounter) Publish(b []byte) (uint64, error) {
	now := time.Now().UnixNano()
	l.first.CompareAndSwap(0, now)
	if now-l.first.Load() > int64(l.window) {
		l.late.Add(1)
	}
	return l.fake.Publish(b)
}

// M2: a publish still in flight when the window ends and then failing is a failed run, with the other acknowledgments kept.
type failsLate struct {
	*fake
	after time.Duration
}

func (f failsLate) Publish(b []byte) (uint64, error) {
	if binary.BigEndian.Uint64(b) == 1 {
		time.Sleep(f.after)
		return 0, errors.New("late publish failure")
	}
	return f.fake.Publish(b)
}

func TestSustainedAPublishThatFailsAfterTheWindowStillMakesAFailedRun(t *testing.T) {
	f := newFake()
	f.slow = time.Millisecond
	_, r := sustainedRun(t, func() Store { return failsLate{f, 200 * time.Millisecond} }, Sustained{Window: 40 * time.Millisecond, Target: 1_000_000_000, Workers: 2, Bucket: time.Hour})
	acked, _ := strconv.Atoi(r["acked"])
	if r["stop"] != StopFailed || !strings.Contains(r["reason"], "late publish failure") || acked < 1 || r["final_reopen_ms"] != "none" {
		t.Fatalf("%v", r)
	}
}

// M3: Close never overlaps an active Retain, also on a failure path.
type blockedRetain struct {
	*fake
	release   chan struct{}
	started   atomic.Bool
	active    atomic.Bool
	overlap   atomic.Bool
	failAfter int64
	n         atomic.Int64
}

func (b *blockedRetain) Publish(d []byte) (uint64, error) {
	if b.n.Add(1) > b.failAfter {
		return 0, errors.New("producer failure")
	}
	return b.fake.Publish(d)
}

func (b *blockedRetain) Retain(uint64) error {
	b.started.Store(true)
	b.active.Store(true)
	<-b.release
	b.active.Store(false)
	return nil
}

func (b *blockedRetain) Close() {
	if b.active.Load() {
		b.overlap.Store(true)
	}
	b.fake.Close()
}

func TestSustainedCloseWaitsForAnActiveRetention(t *testing.T) {
	b := &blockedRetain{fake: newFake(), release: make(chan struct{}), failAfter: 400}
	b.fake.slow = time.Millisecond // producers outlast the first retention tick, so a retention is running when one fails
	go func() { time.Sleep(300 * time.Millisecond); close(b.release) }()
	_, r := sustainedRun(t, func() Store { return b }, Sustained{Window: 30 * time.Second, Target: 1_000_000, Workers: 2, Bucket: time.Hour, KeepLast: 10, RetainEvery: 20 * time.Millisecond})
	if !b.started.Load() {
		t.Fatal("the retention never started, so this test proves nothing")
	}
	if b.overlap.Load() {
		t.Fatal("Close ran while Retain was active")
	}
	if r["stop"] != StopFailed {
		t.Fatalf("%v", r)
	}
}

func TestSustainedARetentionThatNeverReturnsIsReportedAndTheStoreIsNotClosed(t *testing.T) {
	old := abandonAfter
	abandonAfter = 200 * time.Millisecond
	t.Cleanup(func() { abandonAfter = old })
	b := &blockedRetain{fake: newFake(), release: make(chan struct{}), failAfter: 400}
	b.fake.slow = time.Millisecond
	t.Cleanup(func() { close(b.release) })
	_, r := sustainedRun(t, func() Store { return b }, Sustained{Window: 30 * time.Second, Target: 1_000_000, Workers: 2, Bucket: time.Hour, KeepLast: 10, RetainEvery: 20 * time.Millisecond})
	if !b.started.Load() || b.overlap.Load() || !strings.Contains(r["reason"], "still running") {
		t.Fatalf("overlap=%v %v", b.overlap.Load(), r)
	}
}

// M4: a corrupted last byte of a record is a verification failure.
type lastByteBad struct{ *fake }

func (b lastByteBad) Read(o uint64) ([]byte, error) {
	got, err := b.fake.Read(o)
	if err != nil || len(got) < 512 {
		return got, err
	}
	bad := append([]byte(nil), got...)
	bad[511] ^= 0xFF
	return bad, nil
}

func TestSustainedACorruptedLastByteFailsVerification(t *testing.T) {
	f := newFake()
	_, r := sustainedRun(t, func() Store { return lastByteBad{f} }, Sustained{Window: 30 * time.Second, Target: 500, Workers: 2, Bucket: time.Hour})
	if r["stop"] != StopVerify || !strings.Contains(r["reason"], "read back wrong") {
		t.Fatalf("%v", r)
	}
}

// L1: a record in both samples is read and counted once.
func TestSustainedOverlappingSamplesCountEachRecordOnce(t *testing.T) {
	f := newFake() // ONE store: a reopen must find the records again
	lines, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 30, Workers: 1, Bucket: time.Hour})
	if r["stop"] != StopReached || r["acked"] != "30" {
		t.Fatalf("%v", r)
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "reopen ") {
			if n, _ := strconv.Atoi(fields(l)["verified"]); n != 30 {
				t.Fatalf("verified %d of 30 acknowledged: %q", n, l)
			}
		}
	}
}

// The number in the reason is the final count, the same as the acked field (publishes in flight at the window's end finish and count).
func TestSustainedWindowReasonAndAckedAgree(t *testing.T) {
	f := newFake()
	f.slow = time.Millisecond
	_, r := sustainedRun(t, func() Store { return f }, Sustained{Window: 100 * time.Millisecond, Target: 1_000_000_000, Workers: 8, Bucket: time.Hour})
	if r["stop"] != StopWindow || !strings.Contains(r["reason"], "ended at "+r["acked"]+" acknowledged") {
		t.Fatalf("acked=%s but reason=%q", r["acked"], r["reason"])
	}
}

// ----- paired review round 2 of PR 31 (54e3a9d) -----

// M1: a publish queued behind a slow interval reopen must not start after the window.
func TestSustainedAPublishQueuedBehindASlowReopenDoesNotStartAfterTheWindow(t *testing.T) {
	f := newFake()
	f.slow = time.Millisecond
	window := 60 * time.Millisecond
	lc := &lateCounter{fake: f, window: window + 30*time.Millisecond}
	var opens atomic.Int32
	store := func() Store {
		if opens.Add(1) > 1 {
			time.Sleep(150 * time.Millisecond) // a slow reopen holding the write lock past the window
		}
		return lc
	}
	_, r := sustainedRun(t, store, Sustained{Window: window, Target: 1_000_000_000, Workers: 4, Bucket: time.Hour, ReopenEvery: 5})
	if lc.late.Load() != 0 {
		t.Fatalf("%d publishes started after the window while queued behind a reopen; %v", lc.late.Load(), r)
	}
}

// M2: a reopen waiting for a hung publish does not stop the coordinator from serving the window and the abandon limit.
type hangsAfter struct {
	*fake
	n       atomic.Int64
	after   int64
	release chan struct{}
}

func (h *hangsAfter) Publish(b []byte) (uint64, error) {
	if h.n.Add(1) > h.after {
		<-h.release
		return 0, errors.New("released")
	}
	return h.fake.Publish(b)
}

func TestSustainedACoordinatorWaitingOnAReopenStillAbandonsAHungPublish(t *testing.T) {
	old := abandonAfter
	abandonAfter = 300 * time.Millisecond
	t.Cleanup(func() { abandonAfter = old })
	h := &hangsAfter{fake: newFake(), after: 3, release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	type res struct {
		lines []string
		r     map[string]string
	}
	got := make(chan res, 1)
	go func() {
		l, r := sustainedRun(t, func() Store { return h }, Sustained{Window: 100 * time.Millisecond, Target: 1_000_000_000, Workers: 2, Bucket: time.Hour, ReopenEvery: 2})
		got <- res{l, r}
	}()
	select {
	case g := <-got:
		if g.r["stop"] != StopFailed || !strings.Contains(g.r["reason"], "abandoned") {
			t.Fatalf("%v", g.r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the coordinator was blocked behind the reopen: the window and the abandon limit were never served")
	}
}

// M4: two different records acknowledged at one offset fail verification.
type oneOffset struct{ *fake }

func (o oneOffset) Publish(b []byte) (uint64, error) {
	if _, err := o.fake.Publish(b); err != nil {
		return 0, err
	}
	return 1, nil
}

func TestSustainedTwoRecordsAtOneOffsetFailVerification(t *testing.T) {
	_, r := sustainedRun(t, func() Store { return oneOffset{newFake()} }, Sustained{Window: 30 * time.Second, Target: 5, Workers: 1, Bucket: time.Hour})
	if r["stop"] != StopVerify || !strings.Contains(r["reason"], "one offset for two different records") {
		t.Fatalf("%v", r)
	}
}

// M5: the host memory guard also covers the final verification.
func TestSustainedTheMemoryGuardCoversTheFinalVerification(t *testing.T) {
	var low atomic.Bool
	f := newFake()
	f.onRead = func() { low.Store(true) } // memory falls as the verification starts reading
	_, r := sustainedRunMem(t, func() Store { return f }, Sustained{Window: 30 * time.Second, Target: 300, Workers: 1, Bucket: time.Hour},
		func() (uint64, error) {
			if low.Load() {
				return 100, nil
			}
			return 1 << 30, nil
		})
	if r["stop"] != StopMemory || !strings.Contains(r["reason"], "memory guard") {
		t.Fatalf("%v", r)
	}
}

// Low: nothing is printed after the final result line, also when an abandoned retention wakes up later.
func TestSustainedNothingIsPrintedAfterTheFinalLine(t *testing.T) {
	old := abandonAfter
	abandonAfter = 200 * time.Millisecond
	t.Cleanup(func() { abandonAfter = old })
	withTemp(t)
	fastTick(t)
	setMemory(t, func() (uint64, error) { return 1 << 30, nil })
	b := &blockedRetain{fake: newFake(), release: make(chan struct{}), failAfter: 400}
	b.fake.slow = time.Millisecond
	var out bytes.Buffer
	if err := executeSustained("fake", func(string, bool) (Store, error) { return b, nil }, Sustained{Window: 30 * time.Second, Target: 1_000_000, Workers: 2, Bucket: time.Hour, KeepLast: 10, RetainEvery: 20 * time.Millisecond}, &out); err != nil {
		t.Fatal(err)
	}
	before := out.String()
	close(b.release) // the abandoned retention returns now and would print a retain line
	time.Sleep(150 * time.Millisecond)
	if out.String() != before || !strings.HasPrefix(before[strings.LastIndex(strings.TrimSpace(before), "\n")+1:], "sustained ") {
		t.Fatalf("output after the final line: %q", out.String())
	}
}
