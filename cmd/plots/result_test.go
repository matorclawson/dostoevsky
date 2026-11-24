package main

import (
	"os/exec"
	"strings"
	"testing"
)

// benchmarks/comparison/result.sh prints medians per engine and producer count:
// the middle value of an odd count and the mean of the middle two otherwise, with
// total_rss_KiB used when a line has it and peakRSS_KiB otherwise.
func TestResultScriptMedians(t *testing.T) {
	if _, err := exec.LookPath("awk"); err != nil {
		t.Skip("needs awk")
	}
	var b strings.Builder
	for i, v := range []string{"4000", "4200", "4100", "4150", "4050"} {
		b.WriteString("trial=" + string(rune('1'+i)) + " engine=dostoevsky workers=1 records/s=" + v + " p99_us=" + v[:3] + " peakRSS_KiB=10240 reopen_ms=100\n")
	}
	for i, v := range []string{"3000", "3200", "3100", "3300"} {
		b.WriteString("trial=" + string(rune('1'+i)) + " engine=jetstream workers=1 records/s=" + v + " p99_us=900 peakRSS_KiB=45056 reopen_ms=10\n")
	}
	log := write(t, t.TempDir(), "c.log", b.String())
	out, err := exec.Command("sh", "../../benchmarks/comparison/result.sh", log).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	text := string(out)
	for _, want := range []string{
		"lines=9 failed=0",
		"engine=dostoevsky workers=1 trials=5 records/s=4100 p99_us=410 total_rss_KiB=10240 reopen_ms=100",
		"engine=jetstream workers=1 trials=4 records/s=3150 p99_us=900 total_rss_KiB=45056 reopen_ms=10",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("result.sh output lacks %q:\n%s", want, text)
		}
	}
}

func TestResultScriptCountsFailedLines(t *testing.T) {
	if _, err := exec.LookPath("awk"); err != nil {
		t.Skip("needs awk")
	}
	log := write(t, t.TempDir(), "c.log", "trial=1 engine=redis workers=1 records/s=10 p99_us=5 peakRSS_KiB=100 total_rss_KiB=1000 reopen_ms=2\n"+
		"trial=2 engine=redis workers=1 FAILED publish 3: boom\n"+
		"trial=3 engine=redis workers=1 records/s=30 p99_us=7 peakRSS_KiB=100 total_rss_KiB=3000 reopen_ms=4\n")
	out, err := exec.Command("sh", "../../benchmarks/comparison/result.sh", log).CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	text := string(out)
	if !strings.Contains(text, "lines=3 failed=1") || !strings.Contains(text, "engine=redis workers=1 trials=2 records/s=20 p99_us=6 total_rss_KiB=2000 reopen_ms=3") {
		t.Fatalf("unexpected output:\n%s", text)
	}
}
