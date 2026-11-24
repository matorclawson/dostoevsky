package main

import (
	"strconv"
	"strings"
	"testing"
)

func tline(engine string, workers, trial int, rps string) string {
	return "trial=" + strconv.Itoa(trial) + " engine=" + engine + " workers=" + strconv.Itoa(workers) + " records/s=" + rps + " p99_us=5 peakRSS_KiB=1000 reopen_ms=2\n"
}

func summaryOf(t *testing.T, log string, want int, producers []int, allow bool) (string, error) {
	t.Helper()
	path := write(t, t.TempDir(), "c.log", log)
	var out strings.Builder
	err := summarize(&out, path, want, producers, allow)
	return out.String(), err
}

func goodSummaryLog() string {
	var b strings.Builder
	for _, e := range []string{"a", "b"} {
		for tr := 1; tr <= 3; tr++ {
			b.WriteString(tline(e, 1, tr, strconv.Itoa(100*tr)))
		}
	}
	return b.String()
}

func TestSummaryPrintsMedians(t *testing.T) {
	out, err := summaryOf(t, goodSummaryLog(), 3, []int{1}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"lines=6 failed=0", "engine=a workers=1 trials=3 records/s=200 p99_us=5 total_rss_KiB=1000 reopen_ms=2"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestSummaryUsesTotalRSSAndTheMeanOfAnEvenCount(t *testing.T) {
	log := "trial=1 engine=r workers=1 records/s=10 p99_us=5 peakRSS_KiB=100 total_rss_KiB=1000 reopen_ms=2\n" +
		"trial=2 engine=r workers=1 records/s=30 p99_us=7 peakRSS_KiB=100 total_rss_KiB=3000 reopen_ms=4\n"
	out, err := summaryOf(t, log, 2, []int{1}, false)
	if err != nil || !strings.Contains(out, "engine=r workers=1 trials=2 records/s=20 p99_us=6 total_rss_KiB=2000 reopen_ms=3") {
		t.Fatalf("%v\n%s", err, out)
	}
}

// One validator: the same strict checks the plotter relies on reject each of these.
func TestSummaryFailsClosed(t *testing.T) {
	cases := []struct {
		name      string
		log       string
		producers []int
		want      string
	}{
		{"empty input", "", []int{1}, "no trial lines"},
		{"only blank lines", "\n\n", []int{1}, "no trial lines"},
		{"malformed value", goodSummaryLog() + "trial=4 engine=a workers=1 records/s=oops p99_us=5 peakRSS_KiB=1 reopen_ms=2\n", []int{1}, "malformed trial line"},
		{"duplicate trial number", goodSummaryLog() + tline("a", 1, 2, "100"), []int{1}, "duplicate trial number 2"},
		{"leading-zero duplicate", goodSummaryLog() + "trial=01 engine=a workers=1 records/s=1 p99_us=5 peakRSS_KiB=1 reopen_ms=2\n", []int{1}, "not a canonical non-negative integer"},
		{"stray token in a trial line", "trial=1 engine=a workers=1 records/s=1 p99_us=5 peakRSS_KiB=1 reopen_ms=2 extra\n", []int{1}, "stray token"},
		{"repeated field", "trial=1 engine=a workers=1 records/s=1 records/s=2 p99_us=5 peakRSS_KiB=1 reopen_ms=2\n", []int{1}, "repeated"},
		{"negative number", "trial=1 engine=a workers=1 records/s=-5 p99_us=5 peakRSS_KiB=1 reopen_ms=2\n", []int{1}, "not a non-negative number"},
		{"missing producer count", goodSummaryLog(), []int{1, 8}, "missing: a at 8 producers"},
		{"failed trial", goodSummaryLog() + "trial=9 engine=a workers=1 FAILED boom\n", []int{1}, "FAILED trial"},
		{"stray text line", goodSummaryLog() + "go: downloading something\n", []int{1}, "not a trial line"},
		{"too few trials", tline("a", 1, 1, "1") + tline("a", 1, 2, "2"), []int{1}, "has 2 valid trials, want 3"},
	}
	for _, c := range cases {
		out, err := summaryOf(t, c.log, 3, c.producers, false)
		if err == nil || out != "" || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, stdout %q; want an error containing %q and no table", c.name, err, out, c.want)
		}
	}
}

func TestSummaryAllowIncompleteListsProblemsAndCountsFailures(t *testing.T) {
	out, err := summaryOf(t, goodSummaryLog()+"trial=9 engine=a workers=1 FAILED boom\n", 3, []int{1}, true)
	if err != nil || !strings.Contains(out, "PROBLEM: line 7: FAILED trial") || !strings.Contains(out, "lines=7 failed=1") {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestParseProducers(t *testing.T) {
	got, err := parseProducers("32, 1,8")
	if err != nil || len(got) != 3 || got[0] != 1 || got[1] != 8 || got[2] != 32 {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"", "0", "a", "1,,8", "01"} {
		if _, err := parseProducers(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
