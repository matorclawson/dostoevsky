package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const receipt = `a23f  dost
# gate
trial=1 engine=dostoevsky workers=1 records/s=4000 p99_us=600 peakRSS_KiB=10240 reopen_ms=100
trial=2 engine=dostoevsky workers=1 records/s=4200 p99_us=700 peakRSS_KiB=10240 reopen_ms=110
trial=3 engine=dostoevsky workers=1 records/s=4100 p99_us=650 peakRSS_KiB=10240 reopen_ms=105
trial=1 engine=jetstream workers=1 records/s=3000 p99_us=900 peakRSS_KiB=45056 reopen_ms=10
trial=2 engine=jetstream workers=1 records/s=3200 p99_us=950 peakRSS_KiB=45056 reopen_ms=11
trial=3 engine=jetstream workers=1 records/s=3100 p99_us=920 peakRSS_KiB=45056 reopen_ms=12
trial=1 engine=dostoevsky workers=8 records/s=13000 p99_us=2000 peakRSS_KiB=11264 reopen_ms=50
trial=1 engine=jetstream workers=8 records/s=4000 p99_us=5000 peakRSS_KiB=45056 reopen_ms=13

# history
trial=9 engine=dostoevsky workers=1 records/s=999999 p99_us=1 peakRSS_KiB=1 reopen_ms=1
engine=dostoevsky workers=1 records/s=999999
`

const meta = "machine=test box\nos=Linux\ngo=1.0\ngomaxprocs=2\ncgo=0\npayload_bytes=512\nrecords=16000\nsync=sync\njetstream=2.15.3\ncommand=make x\nsketch_command=make y\n"

const sketchReceipt = `kind=hll precision=12 n=100000 exact=100000 estimate=100500.0 rel_err_pct=0.500 bytes=4100
kind=latency shape=lognormal percentile=99 n=200000 exact_us=1000.0 upper_us=1040.0 rel_err_pct=4.000 bytes=7688
`

func write(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMedian(t *testing.T) {
	if m := median([]float64{5, 1, 3}); m != 3 {
		t.Fatal(m)
	}
	if m := median([]float64{4, 1, 3, 2}); m != 2.5 {
		t.Fatal(m)
	}
	if m := median(nil); m != 0 {
		t.Fatal(m)
	}
}

func TestNiceMax(t *testing.T) {
	for in, want := range map[float64]float64{0: 1, 3: 5, 4200: 5000, 13000: 20000, 48645: 50000, 0.7: 1, 1.1: 2} {
		if got := niceMax(in); got != want {
			t.Errorf("niceMax(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestCommas(t *testing.T) {
	for in, want := range map[string]string{"7": "7", "999": "999", "1000": "1,000", "48645": "48,645", "1234567": "1,234,567"} {
		if got := commas(in); got != want {
			t.Errorf("commas(%s) = %s", in, got)
		}
	}
}

func TestRunIsDeterministicAndShowsMedians(t *testing.T) {
	dir := t.TempDir()
	r := write(t, dir, "r.txt", receipt)
	m := write(t, dir, "m.txt", meta)
	s := write(t, dir, "s.txt", sketchReceipt)
	var first map[string]string
	for round := 0; round < 2; round++ {
		out := filepath.Join(dir, "out")
		if err := run(r, m, s, out, 0); err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, name := range []string{"throughput.svg", "memory.svg", "p99-latency.svg", "reopen.svg", "sketch-accuracy.svg", "architecture.svg"} {
			b, err := os.ReadFile(filepath.Join(out, name))
			if err != nil {
				t.Fatal(err)
			}
			got[name] = string(b)
		}
		if first == nil {
			first = got
			continue
		}
		for name, body := range got {
			if body != first[name] {
				t.Errorf("%s differs between runs", name)
			}
		}
	}
	th := first["throughput.svg"]
	for _, want := range []string{"4,100", "3,100", "13,000", "<title id=\"t\">Synchronous publish throughput", "Dostoevsky", "Embedded JetStream", "test box", "make x", "prefers-color-scheme:dark"} {
		if !strings.Contains(th, want) {
			t.Errorf("throughput.svg missing %q", want)
		}
	}
	if !strings.Contains(first["memory.svg"], "10.0") || !strings.Contains(first["memory.svg"], "44.0") {
		t.Error("memory.svg should show medians 10.0 and 44.0 MiB")
	}
	if !strings.Contains(first["architecture.svg"], "ack only after sync") || !strings.Contains(first["architecture.svg"], "<desc id=\"d\">") {
		t.Error("architecture.svg missing durability text or description")
	}
	if !strings.Contains(first["reopen.svg"], "JetStream wins at 1 and 8 producers.") || !strings.Contains(first["reopen.svg"], "Dostoevsky scans its journal on open.") {
		t.Error("reopen.svg must derive and state where JetStream wins")
	}
	if strings.Contains(first["reopen.svg"], "Reopen and verify") || !strings.Contains(first["reopen.svg"], "Reopen time") {
		t.Error("reopen.svg title must say it measures reopen time only")
	}
	if !strings.Contains(first["throughput.svg"], "Dostoevsky wins at 1 and 8 producers.") || strings.Contains(first["throughput.svg"], "scans its journal") {
		t.Error("throughput.svg verdict wrong")
	}
	if !strings.Contains(first["sketch-accuracy.svg"], "4,100 B serialized") || !strings.Contains(first["sketch-accuracy.svg"], "7,688 B in memory") {
		t.Error("sketch sizes must say serialized or in memory")
	}
	if !strings.Contains(first["sketch-accuracy.svg"], "4.000%") || !strings.Contains(first["sketch-accuracy.svg"], "7,688 B") {
		t.Error("sketch-accuracy.svg missing error and memory")
	}
}

func TestRunWithoutSketchReceiptSkipsThatChart(t *testing.T) {
	dir := t.TempDir()
	r := write(t, dir, "r.txt", receipt)
	m := write(t, dir, "m.txt", meta)
	out := filepath.Join(dir, "out")
	if err := run(r, m, filepath.Join(dir, "missing.txt"), out, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "sketch-accuracy.svg")); err == nil {
		t.Fatal("sketch chart written without a receipt")
	}
}

func TestRunRejectsReceiptWithoutTrials(t *testing.T) {
	dir := t.TempDir()
	r := write(t, dir, "r.txt", "nothing here\n")
	m := write(t, dir, "m.txt", meta)
	if err := run(r, m, filepath.Join(dir, "none"), filepath.Join(dir, "out"), 0); err == nil {
		t.Fatal("empty receipt accepted")
	}
}

func TestOnlyTheGateSectionIsCharted(t *testing.T) {
	dir := t.TempDir()
	r := write(t, dir, "r.txt", receipt)
	trials, err := parseTrials(r, "gate")
	if err != nil {
		t.Fatal(err)
	}
	if len(trials) != 8 {
		t.Fatalf("parsed %d gate trials, want 8", len(trials))
	}
}

func TestRunRequiresTheExpectedTrialCount(t *testing.T) {
	dir := t.TempDir()
	r := write(t, dir, "r.txt", receipt)
	m := write(t, dir, "m.txt", meta)
	if err := run(r, m, filepath.Join(dir, "none"), filepath.Join(dir, "out"), 5); err == nil || !strings.Contains(err.Error(), "trials") {
		t.Fatalf("receipt with 3 trials accepted for 5: %v", err)
	}
}

// The medians the charts show must equal the summary the receipt itself records
// and docs/publisher-gate.md publishes.
func TestRealReceiptMediansMatchPublishedTable(t *testing.T) {
	trials, err := parseTrials("../../docs/publisher-gate-results.txt", "gate")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[int]float64{
		"dostoevsky": {1: 4005, 8: 13252, 32: 36422},
		"jetstream":  {1: 3185, 8: 3384, 32: 4020},
	}
	for _, s := range collect(trials, "records/s", 1) {
		for w, vs := range s.values {
			if len(vs) != 5 {
				t.Errorf("%s at %d producers: %d trials, want 5", s.engine, w, len(vs))
			}
			if got := median(vs); got != want[s.engine][w] {
				t.Errorf("%s at %d producers: median %v, published %v", s.engine, w, got, want[s.engine][w])
			}
		}
	}
	if len(trials) != 30 {
		t.Errorf("gate section has %d trials, want 30", len(trials))
	}
}

func TestTicksAreRound(t *testing.T) {
	for in, want := range map[float64][2]float64{4.0: {1, 4}, 4.8: {1, 5}, 0.3: {0.05, 6}, 48645: {10000, 5}} {
		step, n := ticks(in)
		if step != want[0] || float64(n) != want[1] {
			t.Errorf("ticks(%v) = %v, %d; want %v, %v", in, step, n, want[0], want[1])
		}
	}
}

func TestWrapKeepsLinesShort(t *testing.T) {
	for _, l := range wrap([]string{strings.Repeat("word ", 60)}, 40) {
		if len(l) > 40 {
			t.Fatalf("line of %d characters", len(l))
		}
	}
}

func TestCheckGroupsFailsLoudly(t *testing.T) {
	line := func(engine string, w, trial int) string {
		return "trial=" + strconv.Itoa(trial) + " engine=" + engine + " workers=" + strconv.Itoa(w) + " records/s=1\n"
	}
	build := func(skip string, dup bool) []trial {
		var b strings.Builder
		b.WriteString("# gate\n")
		for _, e := range expectedEngines {
			for _, w := range expectedProducers {
				if e+strconv.Itoa(w) == skip {
					continue
				}
				for i := 1; i <= 2; i++ {
					b.WriteString(line(e, w, i))
				}
			}
		}
		if dup {
			b.WriteString(line("dostoevsky", 1, 1))
		}
		p := write(t, t.TempDir(), "r.txt", b.String())
		trials, err := parseTrials(p, "gate")
		if err != nil {
			t.Fatal(err)
		}
		return trials
	}
	if err := checkGroups(build("", false), 2); err != nil {
		t.Fatal(err)
	}
	if err := checkGroups(build("jetstream32", false), 2); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing group accepted: %v", err)
	}
	if err := checkGroups(build("", true), 2); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate trial accepted: %v", err)
	}
	if err := checkGroups(build("", false), 3); err == nil || !strings.Contains(err.Error(), "trials") {
		t.Fatalf("wrong count accepted: %v", err)
	}
}

func TestWinnersAreDerivedFromTheData(t *testing.T) {
	data := []series{
		{engine: "dostoevsky", values: map[int][]float64{1: {5}, 8: {1}, 32: {1}}},
		{engine: "jetstream", values: map[int][]float64{1: {1}, 8: {5}, 32: {5}}},
	}
	if got := winners(data, true); got != "Dostoevsky wins at 8 and 32 producers. JetStream wins at 1 producer." {
		t.Fatal(got)
	}
	if got := winners(data, false); got != "Dostoevsky wins at 1 producer. JetStream wins at 8 and 32 producers." {
		t.Fatal(got)
	}
	if !losing(data, true) {
		t.Fatal("JetStream wins at 1 producer, so Dostoevsky is losing somewhere")
	}
	tie := []series{
		{engine: "dostoevsky", values: map[int][]float64{1: {3}}},
		{engine: "jetstream", values: map[int][]float64{1: {3}}},
	}
	if got := winners(tie, true); got != "" {
		t.Fatalf("a tie produced a winner: %q", got)
	}
}
