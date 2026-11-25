package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These tests run the real binary on fixture receipts and assert its exit code and
// output, so a bypass at the command line (a flag, a default list, a count of zero)
// cannot hide behind a function the unit tests call.

func buildPlots(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("needs the go tool")
	}
	bin := filepath.Join(t.TempDir(), "plots")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the plotter: %v\n%s", err, out)
	}
	return bin
}

// plots runs the binary in dir and returns its stdout, stderr and exit code.
func plots(t *testing.T, bin, dir string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return out.String(), errb.String(), code
}

func sevenArgs(receipt, meta, out string, extra ...string) []string {
	args := []string{"-receipt", receipt, "-section", "*", "-meta", meta, "-sketch", filepath.Join(filepath.Dir(out), "none"), "-out", out,
		"-engines", strings.Join(sevenEngines, ",")}
	return append(args, extra...)
}

// M2: ANY FAILED line is refused without -allow-failures, whatever the engine list,
// the trial count or the producer count of the failed row.
func TestCLIRefusesAnyFailedLineWithoutAllowFailures(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	m := write(t, dir, "m.txt", multiMeta)
	failedRedis := write(t, dir, "failed.log", multiLog(multiRPS, map[string]bool{"redis/32/3": true}))
	offSchedule := write(t, dir, "off.log", multiLog(multiRPS, nil)+"trial=1 engine=dostoevsky workers=99 FAILED boom\n")
	cases := []struct {
		name string
		args []string
	}{
		// The default engine list is two engines: the failed redis row is outside it.
		{"default engine list", []string{"-receipt", failedRedis, "-section", "*", "-meta", m, "-sketch", dir + "/none", "-out", dir + "/o1"}},
		{"trials=0 disables the count checks", []string{"-receipt", failedRedis, "-section", "*", "-meta", m, "-sketch", dir + "/none", "-out", dir + "/o2", "-trials", "0"}},
		{"a failed row outside the producer counts", []string{"-receipt", offSchedule, "-section", "*", "-meta", m, "-sketch", dir + "/none", "-out", dir + "/o3", "-engines", "dostoevsky,jetstream", "-trials", "0"}},
		{"seven engines", sevenArgs(failedRedis, m, dir+"/o4")},
	}
	for _, c := range cases {
		_, stderr, code := plots(t, bin, dir, c.args...)
		if code == 0 || !strings.Contains(stderr, "FAILED trial lines") {
			t.Errorf("%s: exit %d, stderr %q; want a refusal naming the FAILED trial lines", c.name, code, stderr)
		}
	}
	if _, stderr, code := plots(t, bin, dir, sevenArgs(failedRedis, m, dir+"/ok", "-allow-failures")...); code != 0 {
		t.Fatalf("-allow-failures did not accept a receipt with one failure: %s", stderr)
	}
}

// M1: every charted engine needs a label and an architecture, and the qualifiers that
// must never vanish (RabbitMQ not verified, Erlang provenance, Kafka inferred) must be
// in the notes. A seven-engine chart without them is refused.
func TestCLIRequiresLabelsArchitecturesAndQualifiers(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, nil))
	bare := write(t, dir, "bare.txt", "footer.1=h\ncommand=x\n")
	_, stderr, code := plots(t, bin, dir, sevenArgs(r, bare, dir+"/bare")...)
	if code == 0 {
		t.Fatal("seven engines were charted with no labels, architectures or qualifiers")
	}
	for _, want := range []string{"label.sqlite", "arch.rabbitmq", "durability per vendor default, not verified", "Erlang", "fsync per message inferred"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the refusal does not name %q:\n%s", want, stderr)
		}
	}
	noKafkaNote := write(t, dir, "nokafka.txt", strings.Replace(multiMeta, "note.2=Kafka: fsync per message inferred from log.flush.interval.messages=1\n", "", 1))
	if _, stderr, code := plots(t, bin, dir, sevenArgs(r, noKafkaNote, dir+"/nk")...); code == 0 || !strings.Contains(stderr, "fsync per message inferred") {
		t.Errorf("a missing Kafka qualifier was accepted: exit %d, %s", code, stderr)
	}
	full := write(t, dir, "full.txt", multiMeta)
	if _, stderr, code := plots(t, bin, dir, sevenArgs(r, full, dir+"/ok")...); code != 0 {
		t.Fatalf("a complete meta file was refused: %s", stderr)
	}
	mem, err := os.ReadFile(filepath.Join(dir, "ok", "memory.svg"))
	if err != nil || !strings.Contains(string(mem), "Peak memory, client plus server processes") {
		t.Fatalf("the multi-engine memory chart must say it counts client plus server processes: %v", err)
	}
}

// M3: a producer count where every engine failed every trial still has its place on the
// chart, with its failures listed.
func TestCLIShowsACountWhereEveryEngineFailed(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	failed := map[string]bool{}
	for _, e := range sevenEngines {
		for tr := 1; tr <= 5; tr++ {
			failed[e+"/32/"+string(rune('0'+tr))] = true
		}
	}
	r := write(t, dir, "c.log", multiLog(multiRPS, failed))
	m := write(t, dir, "m.txt", multiMeta)
	_, stderr, code := plots(t, bin, dir, sevenArgs(r, m, dir+"/o", "-allow-failures")...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	b, err := os.ReadFile(filepath.Join(dir, "o", "throughput.svg"))
	if err != nil {
		t.Fatal(err)
	}
	svg := string(b)
	for _, want := range []string{"32 producers", "Dostoevsky: 5 failed", "Kafka 4.4 (acks=all): 5 failed"} {
		if !strings.Contains(svg, want) {
			t.Errorf("throughput.svg lacks %q: the all-failed count was dropped", want)
		}
	}
}

// M4: nothing after FAILED is read, so a diagnostic that looks like metrics is not plotted.
func TestCLIFailedDiagnosticsAreNotPlotted(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	log := strings.Replace(multiLog(multiRPS, map[string]bool{"redis/32/3": true}),
		"trial=3 engine=redis workers=32 FAILED boom",
		"trial=3 engine=redis workers=32 FAILED boom records/s=99999 p99_us=1 peakRSS_KiB=1 reopen_ms=1", 1)
	r := write(t, dir, "c.log", log)
	m := write(t, dir, "m.txt", multiMeta)
	_, stderr, code := plots(t, bin, dir, sevenArgs(r, m, dir+"/o", "-allow-failures")...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "o", "throughput.svg"))
	svg := string(b)
	if strings.Contains(svg, "99,999") {
		t.Error("a number from a failed trial's diagnostic was plotted")
	}
	if !strings.Contains(svg, "median 9,003 of 4 trials, 1 failed") {
		t.Error("the redis bar must be the median of its four valid trials")
	}
}

// The committed charts of one or two engines must not change: running the real binary
// on the committed receipts reproduces them byte for byte.
func TestCLIExistingChartsAreByteIdentical(t *testing.T) {
	bin := buildPlots(t)
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	sets := []struct {
		committed string
		args      []string
	}{
		{"docs/img", []string{"-architecture=false", "-sketch", "/nonexistent"}},
		{"docs/img/production-03", []string{"-receipt", "docs/receipts/production-03/explicit-2.log", "-section", "*", "-meta", "docs/receipts/production-03/meta.txt", "-sketch", "/nonexistent", "-architecture=false"}},
	}
	for _, set := range sets {
		out := t.TempDir()
		args := append([]string{"-out", out}, set.args...)
		if _, stderr, code := plots(t, bin, root, args...); code != 0 {
			t.Fatalf("%s: exit %d: %s", set.committed, code, stderr)
		}
		for _, name := range []string{"throughput.svg", "memory.svg", "p99-latency.svg", "reopen.svg"} {
			want, err := os.ReadFile(filepath.Join(root, set.committed, name))
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(out, name))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(want, got) {
				t.Errorf("%s/%s changed: the generator no longer reproduces the committed chart", set.committed, name)
			}
		}
	}
}
