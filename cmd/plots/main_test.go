package main

import (
	"os"
	"path/filepath"
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
		if err := run(r, m, s, out); err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, name := range []string{"throughput.svg", "memory.svg", "p99-latency.svg", "reopen.svg", "sketch-accuracy.svg"} {
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
	if !strings.Contains(first["reopen.svg"], "JetStream is faster here") {
		t.Error("reopen.svg must say where JetStream wins")
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
	if err := run(r, m, filepath.Join(dir, "missing.txt"), out); err != nil {
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
	if err := run(r, m, filepath.Join(dir, "none"), filepath.Join(dir, "out")); err == nil {
		t.Fatal("empty receipt accepted")
	}
}
