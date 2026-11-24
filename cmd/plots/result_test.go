package main

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func trialLine(engine string, workers, trial int, rps string) string {
	return "trial=" + strconv.Itoa(trial) + " engine=" + engine + " workers=" + strconv.Itoa(workers) + " records/s=" + rps + " p99_us=5 peakRSS_KiB=1000 reopen_ms=2\n"
}

// result runs benchmarks/comparison/result.sh on log with the given environment.
func result(t *testing.T, log string, env ...string) (stdout, stderr string, code int) {
	t.Helper()
	if _, err := exec.LookPath("awk"); err != nil {
		t.Skip("needs awk")
	}
	path := write(t, t.TempDir(), "c.log", log)
	cmd := exec.Command("sh", "../../benchmarks/comparison/result.sh", path)
	cmd.Env = append(os.Environ(), env...)
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return out.String(), errb.String(), code
}

var strict = []string{"EXPECT_TRIALS=3", "EXPECT_PRODUCERS=1"}

func goodLog() string {
	var b strings.Builder
	for _, e := range []string{"a", "b"} {
		for tr := 1; tr <= 3; tr++ {
			b.WriteString(trialLine(e, 1, tr, strconv.Itoa(100*tr)))
		}
	}
	return b.String()
}

func TestResultScriptPrintsMedians(t *testing.T) {
	out, errOut, code := result(t, goodLog(), strict...)
	if code != 0 || errOut != "" {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	for _, want := range []string{"lines=6 failed=0", "engine=a workers=1 trials=3 records/s=200 p99_us=5 total_rss_KiB=1000 reopen_ms=2"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestResultScriptUsesTotalRSSWhenPresentAndTheMeanOfAnEvenCount(t *testing.T) {
	log := "trial=1 engine=r workers=1 records/s=10 p99_us=5 peakRSS_KiB=100 total_rss_KiB=1000 reopen_ms=2\n" +
		"trial=2 engine=r workers=1 records/s=30 p99_us=7 peakRSS_KiB=100 total_rss_KiB=3000 reopen_ms=4\n"
	out, _, code := result(t, log, "EXPECT_TRIALS=2", "EXPECT_PRODUCERS=1")
	if code != 0 || !strings.Contains(out, "engine=r workers=1 trials=2 records/s=20 p99_us=6 total_rss_KiB=2000 reopen_ms=3") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestResultScriptFailsClosed(t *testing.T) {
	cases := []struct {
		name, log, want string
	}{
		{"malformed line", goodLog() + "trial=4 engine=a workers=1 records/s=oops p99_us=5 peakRSS_KiB=1 reopen_ms=2\n", "malformed trial line"},
		{"duplicate trial number", goodLog() + trialLine("a", 1, 2, "100"), "duplicate trial number 2"},
		{"missing producer count", goodLog(), "missing: a at 8 producers"},
		{"failed trial", goodLog() + "trial=9 engine=a workers=1 FAILED boom\n", "FAILED trial"},
		{"stray text", goodLog() + "go: downloading something\n", "not a trial line"},
		{"too few trials", trialLine("a", 1, 1, "1") + trialLine("a", 1, 2, "2"), "has 2 valid trials, want 3"},
	}
	for _, c := range cases {
		env := strict
		if c.name == "missing producer count" {
			env = []string{"EXPECT_TRIALS=3", "EXPECT_PRODUCERS=1 8"}
		}
		out, errOut, code := result(t, c.log, env...)
		if code == 0 || out != "" || !strings.Contains(errOut, c.want) {
			t.Errorf("%s: exit %d, stdout %q, stderr %q; want a nonzero exit, no table and %q", c.name, code, out, errOut, c.want)
		}
	}
}

func TestResultScriptAllowIncompleteListsProblemsAndStillCountsFailures(t *testing.T) {
	log := goodLog() + "trial=9 engine=a workers=1 FAILED boom\n"
	out, _, code := result(t, log, "EXPECT_TRIALS=3", "EXPECT_PRODUCERS=1", "RESULT_ALLOW_INCOMPLETE=1")
	if code != 0 || !strings.Contains(out, "PROBLEM: line 7: FAILED trial") || !strings.Contains(out, "lines=7 failed=1") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}
