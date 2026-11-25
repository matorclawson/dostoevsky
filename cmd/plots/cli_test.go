package main

import (
	"bytes"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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
		{"docs/img/comparison", []string{"-receipt", "docs/receipts/production-03-comparison/comparison.log", "-section", "*", "-meta", "docs/receipts/production-03-comparison/meta.txt", "-sketch", "/nonexistent", "-architecture=false", "-engines", "dostoevsky,jetstream,sqlite,redis,valkey,kafka,rabbitmq"}},
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

// M1 (cx delta 288200): the engines checked are the ones in the receipt. The same complete
// seven-engine receipt with a footer-only meta file must not chart under the default engine
// list, and an expected engine with no line must not be silently absent.
func TestCLIChecksTheEnginesActuallyInTheReceipt(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, nil))
	footerOnly := write(t, dir, "footer.txt", "footer.1=h\ncommand=x\n")
	_, stderr, code := plots(t, bin, dir, "-receipt", r, "-section", "*", "-meta", footerOnly, "-sketch", dir+"/none", "-out", dir+"/o1")
	if code == 0 || !strings.Contains(stderr, "not in -engines") {
		t.Fatalf("seven engines charted under the default list: exit %d, %s", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "o1", "throughput.svg")); err == nil {
		t.Fatal("a chart was drawn before the engine set was checked")
	}
	// Leaving engines out on purpose is allowed, and then only the charted ones are drawn.
	_, stderr, code = plots(t, bin, dir, "-receipt", r, "-section", "*", "-meta", footerOnly, "-sketch", dir+"/none", "-out", dir+"/o2",
		"-ignore-engines", "sqlite,redis,valkey,kafka,rabbitmq")
	if code != 0 {
		t.Fatalf("an explicit exclusion was refused: %s", stderr)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "o2", "throughput.svg"))
	svg := string(b)
	// The "Not shown" line names the excluded engines on purpose, so assert on what is DRAWN:
	// two legend swatches, no third color slot, and no bar or tooltip for an excluded engine.
	if got := strings.Count(svg, `width="12" height="12"`); got != 2 {
		t.Errorf("%d legend swatches, want 2 (dostoevsky and jetstream)", got)
	}
	for _, drawn := range []string{`class="s2"`, `class="s3"`, "<title>redis", "<title>sqlite", "<title>kafka", "<title>rabbitmq", "<title>valkey"} {
		if strings.Contains(svg, drawn) {
			t.Errorf("an ignored engine was drawn: %s", drawn)
		}
	}
	if !strings.Contains(svg, "Not shown: sqlite, redis, valkey, kafka, rabbitmq (excluded with -ignore-engines)") {
		t.Error("the excluded engines are not named")
	}
	// An expected engine with no line at all, even with the count checks off.
	_, stderr, code = plots(t, bin, dir, "-receipt", r, "-section", "*", "-meta", footerOnly, "-sketch", dir+"/none", "-out", dir+"/o3",
		"-engines", "dostoevsky,jetstream,nosuch", "-ignore-engines", "sqlite,redis,valkey,kafka,rabbitmq", "-trials", "0")
	if code == 0 || !strings.Contains(stderr, "nosuch") {
		t.Fatalf("an expected engine with no line was accepted: exit %d, %s", code, stderr)
	}
}

// M3 (cx delta 288200): no text of the failure annotation runs off the chart. The widest line
// is measured with the same estimate the renderer wraps to, on the all-failed fixture.
func TestCLIFailureNotesFitTheChart(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	failed := map[string]bool{}
	for _, e := range sevenEngines {
		for tr := 1; tr <= 5; tr++ {
			failed[e+"/32/"+string(rune('0'+tr))] = true
			failed[e+"/8/"+string(rune('0'+tr))] = true
		}
	}
	r := write(t, dir, "c.log", multiLog(multiRPS, failed))
	m := write(t, dir, "m.txt", multiMeta)
	if _, stderr, code := plots(t, bin, dir, sevenArgs(r, m, dir+"/o", "-allow-failures")...); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	re := regexp.MustCompile(`<text x="([0-9.]+)" y="([0-9.]+)" font-size="([0-9.]+)" class="f">([^<]*)</text>`)
	for _, name := range []string{"throughput.svg", "memory.svg", "p99-latency.svg", "reopen.svg"} {
		b, err := os.ReadFile(filepath.Join(dir, "o", name))
		if err != nil {
			t.Fatal(err)
		}
		lines := re.FindAllStringSubmatch(string(b), -1)
		if len(lines) < 7 {
			t.Fatalf("%s: found %d failure lines, want at least one per engine", name, len(lines))
		}
		groupW := float64(width-left-right) / 3
		for _, l := range lines {
			x, _ := strconv.ParseFloat(l[1], 64)
			size, _ := strconv.ParseFloat(l[3], 64)
			text := html.UnescapeString(l[4])
			w := textWidth(text, size)
			if x+w > float64(width) {
				t.Errorf("%s: %q starts at %.0f and is about %.0f wide, past the %d-unit chart", name, text, x, w, width)
			}
			if w > groupW {
				t.Errorf("%s: %q is about %.0f wide, wider than its %.0f-unit group", name, text, w, groupW)
			}
		}
		// The legend and footer must sit below the last failure line.
		var last float64
		for _, l := range lines {
			y, _ := strconv.ParseFloat(l[2], 64)
			if y > last {
				last = y
			}
		}
		if i := strings.Index(string(b), `<rect class="s0" x="72"`); i >= 0 {
			legend := regexp.MustCompile(`y="([0-9]+)"`).FindStringSubmatch(string(b)[i:])
			if y, _ := strconv.ParseFloat(legend[1], 64); y < last+10 {
				t.Errorf("%s: the legend at y=%.0f overlaps the last failure line at y=%.0f", name, y, last)
			}
		}
	}
}

// The legend is laid out by measured width: with the long labels the real run will have, no
// entry overlaps its neighbor or runs past the chart, and short labels keep three columns.
func TestCLILegendFitsLongLabels(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, nil))
	long := strings.NewReplacer(
		"label.rabbitmq=RabbitMQ 4.4 (quorum queue)", "label.rabbitmq=RabbitMQ 4.4 quorum (sync not verified)",
		"arch.rabbitmq=server, loopback", "arch.rabbitmq=server over loopback",
		"arch.redis=server, unix socket", "arch.redis=server over a unix socket",
		"label.sqlite=SQLite (single writer)", "label.sqlite=SQLite 3.54 (WAL, FULL, single writer)",
	).Replace(multiMeta)
	longMeta := write(t, dir, "long.txt", long)
	var shortLines []string
	for _, l := range strings.Split(multiMeta, "\n") {
		if !strings.HasPrefix(l, "label.") && !strings.HasPrefix(l, "arch.") {
			shortLines = append(shortLines, l)
		}
	}
	for _, e := range sevenEngines {
		kind := "server"
		if e == "dostoevsky" || e == "jetstream" || e == "sqlite" {
			kind = "in process"
		}
		shortLines = append(shortLines, "label."+e+"="+strings.ToUpper(e[:1])+e[1:], "arch."+e+"="+kind)
	}
	shortMeta := write(t, dir, "short.txt", strings.Join(shortLines, "\n"))
	re := regexp.MustCompile(`<text x="([0-9]+)" y="([0-9]+)" font-size="11">([^<]*)</text>`)
	columns := func(name, meta string) int {
		out := filepath.Join(dir, name)
		if _, stderr, code := plots(t, bin, dir, sevenArgs(r, meta, out)...); code != 0 {
			t.Fatalf("exit %d: %s", code, stderr)
		}
		b, err := os.ReadFile(filepath.Join(out, "throughput.svg"))
		if err != nil {
			t.Fatal(err)
		}
		entries := re.FindAllStringSubmatch(string(b), -1)
		if len(entries) != 7 {
			t.Fatalf("%s: found %d legend entries, want 7", name, len(entries))
		}
		rows := map[string][]int{}
		for _, e := range entries {
			x, _ := strconv.Atoi(e[1])
			w := textWidth(html.UnescapeString(e[3]), 11)
			if float64(x)+w > float64(width-right) {
				t.Errorf("%s: legend entry %q runs to about %.0f, past the chart's right margin at %d", name, e[3], float64(x)+w, width-right)
			}
			rows[e[2]] = append(rows[e[2]], x)
		}
		most := 0
		for y, xs := range rows {
			sort.Ints(xs)
			if len(xs) > most {
				most = len(xs)
			}
			// Entries in one row must not overlap: each fits before the next one starts.
			for i := 0; i+1 < len(xs); i++ {
				for _, e := range entries {
					x, _ := strconv.Atoi(e[1])
					if e[2] == y && x == xs[i] {
						if end := float64(x) + textWidth(html.UnescapeString(e[3]), 11); end > float64(xs[i+1]) {
							t.Errorf("%s: legend entry %q (to about %.0f) overlaps the next one starting at %d", name, e[3], end, xs[i+1])
						}
					}
				}
			}
		}
		return most
	}
	if got := columns("short", shortMeta); got != 3 {
		t.Errorf("short labels use %d columns, want 3", got)
	}
	if got := columns("long", longMeta); got >= 3 {
		t.Errorf("long labels still use %d columns; they must fall back to fewer", got)
	}
}

// M1 (cx delta 288400): comparison rendering follows the DECLARED engine set. Seven declared,
// five that failed every trial: only Redis and Valkey have values, yet the chart is still a
// comparison chart with architectures in the legend, the comparison memory title, a description
// that names the engines present (not a fixed pair) and failure notes for the five.
func TestCLIComparisonRenderingFollowsTheDeclaredSet(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	failed := map[string]bool{}
	for _, e := range []string{"dostoevsky", "jetstream", "sqlite", "kafka", "rabbitmq"} {
		for _, w := range []string{"1", "8", "32"} {
			for tr := 1; tr <= 5; tr++ {
				failed[e+"/"+w+"/"+string(rune('0'+tr))] = true
			}
		}
	}
	r := write(t, dir, "c.log", multiLog(multiRPS, failed))
	m := write(t, dir, "m.txt", multiMeta)
	if _, stderr, code := plots(t, bin, dir, sevenArgs(r, m, dir+"/o", "-allow-failures")...); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(dir, "o", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	th, mem := read("throughput.svg"), read("memory.svg")
	if !strings.Contains(mem, "Peak memory, client plus server processes") {
		t.Error("a seven-engine run with two engines of data must keep the comparison memory title")
	}
	desc := regexp.MustCompile(`<desc id="d">([^<]*)</desc>`).FindStringSubmatch(th)
	if desc == nil {
		t.Fatal("no description")
	}
	for _, want := range []string{"Redis 8.12 (AOF always)", "Valkey 9.2 (AOF always)", "No valid trial: Dostoevsky"} {
		if !strings.Contains(desc[1], want) {
			t.Errorf("the description lacks %q: %s", want, desc[1])
		}
	}
	if strings.Contains(desc[1], "Dostoevsky and embedded JetStream") {
		t.Errorf("the description still names the legacy pair: %s", desc[1])
	}
	for _, want := range []string{"Redis 8.12 (AOF always) (server, unix socket)", "Valkey 9.2 (AOF always) (server, unix socket)", "Kafka 4.4 (acks=all) (server, loopback)",
		"Dostoevsky: 5 failed", "RabbitMQ 4.4 (quorum queue): 5 failed", `class="s3"`, `class="s4"`} {
		if !strings.Contains(th, want) {
			t.Errorf("throughput.svg lacks %q", want)
		}
	}
	if got := strings.Count(th, `width="11" height="11"`); got != 7 {
		t.Errorf("%d legend swatches, want one for each of the seven declared engines", got)
	}
}

// L1 (cx delta 288400): a single label wider than one column wraps in the legend instead of
// clipping, and its lines do not overlap vertically.
func TestCLILegendWrapsAnOverWideLabel(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, nil))
	longLabel := "Kafka 4.4 (acks=all, flush inferred) with a deliberately long description that goes on and on, far past one legend column, so that it has to continue on a second line"
	meta := write(t, dir, "m.txt", strings.Replace(multiMeta, "label.kafka=Kafka 4.4 (acks=all)", "label.kafka="+longLabel, 1))
	if _, stderr, code := plots(t, bin, dir, sevenArgs(r, meta, dir+"/o")...); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	b, err := os.ReadFile(filepath.Join(dir, "o", "throughput.svg"))
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`<text x="([0-9]+)" y="([0-9]+)" font-size="11">([^<]*)</text>`)
	var all []string
	prevY := -100
	for _, e := range re.FindAllStringSubmatch(string(b), -1) {
		x, _ := strconv.Atoi(e[1])
		y, _ := strconv.Atoi(e[2])
		text := html.UnescapeString(e[3])
		all = append(all, text)
		if float64(x)+textWidth(text, 11) > float64(width-right) {
			t.Errorf("legend line %q runs to about %.0f, past the right margin", text, float64(x)+textWidth(text, 11))
		}
		if y-prevY < 12 && y >= prevY {
			t.Errorf("legend line %q at y=%d is within 12 units of the line above (y=%d)", text, y, prevY)
		}
		prevY = y
	}
	joined := strings.Join(all, " ")
	for _, word := range strings.Fields(longLabel) {
		if !strings.Contains(joined, word) {
			t.Fatalf("the wrapped legend lost %q: %s", word, joined)
		}
	}
	if kafkaLines := strings.Count(joined, "Kafka 4.4"); kafkaLines < 1 {
		t.Fatal("the long label is missing from the legend")
	}
}

// cc M1 and L2 (pair 288500): comparison mode follows the declared engines at any count. RabbitMQ
// charted with only Dostoevsky needs its qualifiers and architecture, and engines left out with
// -ignore-engines are named on the chart.
func TestCLIAnyNonLegacyEngineMeansComparisonMode(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, nil))
	rest := "jetstream,sqlite,redis,valkey,kafka"
	args := func(meta, out string) []string {
		return []string{"-receipt", r, "-section", "*", "-meta", meta, "-sketch", dir + "/none", "-out", out,
			"-engines", "dostoevsky,rabbitmq", "-ignore-engines", rest}
	}
	noNotes := write(t, dir, "nonotes.txt", strings.NewReplacer(
		"note.1=RabbitMQ: durability per vendor default, not verified; Erlang from the Hex.pm prebuilt\n", "",
		"note.2=Kafka: fsync per message inferred from log.flush.interval.messages=1\n", "").Replace(multiMeta))
	_, stderr, code := plots(t, bin, dir, args(noNotes, dir+"/o1")...)
	if code == 0 || !strings.Contains(stderr, "durability per vendor default, not verified") || !strings.Contains(stderr, "Erlang") {
		t.Fatalf("RabbitMQ charted without its qualifiers: exit %d, %s", code, stderr)
	}
	bare := write(t, dir, "bare.txt", "footer.1=h\nnote.1=RabbitMQ: durability per vendor default, not verified; Erlang from the Hex.pm prebuilt\ncommand=x\n")
	if _, stderr, code := plots(t, bin, dir, args(bare, dir+"/o2")...); code == 0 || !strings.Contains(stderr, "arch.rabbitmq") {
		t.Fatalf("RabbitMQ charted with no architecture: exit %d, %s", code, stderr)
	}
	full := write(t, dir, "full.txt", multiMeta)
	if _, stderr, code := plots(t, bin, dir, args(full, dir+"/ok")...); code != 0 {
		t.Fatalf("a complete meta file was refused: %s", stderr)
	}
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join(dir, "ok", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	th := read("throughput.svg")
	for _, want := range []string{
		"RabbitMQ 4.4 (quorum queue) (server, loopback)", "durability per vendor default, not verified", "Dostoevsky (in process)",
		"Not shown: jetstream, sqlite, redis, valkey, kafka (excluded with -ignore-engines)",
	} {
		if !strings.Contains(th, want) {
			t.Errorf("throughput.svg lacks %q", want)
		}
	}
	if desc := regexp.MustCompile(`<desc id="d">([^<]*)</desc>`).FindStringSubmatch(th); desc == nil || !strings.Contains(desc[1], "Not shown: jetstream") {
		t.Errorf("the description does not name the omitted engines: %v", desc)
	}
	if !strings.Contains(read("memory.svg"), "Peak memory, client plus server processes") {
		t.Error("a Dostoevsky plus RabbitMQ chart must use the comparison memory title")
	}
	// The legacy pair with engines left out also names them, and its charts keep the legacy renderer.
	_, stderr, code = plots(t, bin, dir, "-receipt", r, "-section", "*", "-meta", write(t, dir, "footer.txt", "footer.1=h\ncommand=x\n"), "-sketch", dir+"/none",
		"-out", dir+"/legacy", "-ignore-engines", "sqlite,redis,valkey,kafka,rabbitmq")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	lb, _ := os.ReadFile(filepath.Join(dir, "legacy", "throughput.svg"))
	if !strings.Contains(string(lb), "Not shown: sqlite, redis, valkey, kafka, rabbitmq (excluded with -ignore-engines)") {
		t.Error("the legacy chart does not name the engines left out")
	}
}

func TestComparisonModeIsAnythingBeyondTheLegacyPair(t *testing.T) {
	for in, want := range map[string]bool{
		"": false, "dostoevsky": false, "jetstream": false, "dostoevsky,jetstream": false,
		"dostoevsky,rabbitmq": true, "rabbitmq": true, "dostoevsky,jetstream,sqlite": true, "kafka,jetstream": true,
	} {
		var list []string
		if in != "" {
			list = strings.Split(in, ",")
		}
		if got := comparisonMode(list); got != want {
			t.Errorf("comparisonMode(%q) = %v, want %v", in, got, want)
		}
	}
}

// Low (cx on 282300fb): when a bar reaches the top of the axis its vertical value label must not
// run into the subtitle. The plot area keeps headroom for the longest label, measured with the
// renderer's own estimate.
func TestCLIVerticalValueLabelsClearTheSubtitle(t *testing.T) {
	bin := buildPlots(t)
	dir := t.TempDir()
	rps := map[string]int{}
	for k, v := range multiRPS {
		rps[k] = v
	}
	rps["dostoevsky"] = 19990 // with the +1..+5 of the trials the tallest bar nearly reaches the 20,000 axis top
	r := write(t, dir, "c.log", multiLog(rps, nil))
	m := write(t, dir, "m.txt", multiMeta)
	if _, stderr, code := plots(t, bin, dir, sevenArgs(r, m, dir+"/o")...); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	rotated := regexp.MustCompile(`<text x="[0-9.]+" y="([0-9.]+)" transform="rotate\(-90 [0-9.]+ [0-9.]+\)" font-size="10" font-weight="600">([^<]*)</text>`)
	subtitle := regexp.MustCompile(`<text x="72" y="([0-9]+)" font-size="12" class="m">`)
	for _, name := range []string{"throughput.svg", "memory.svg", "p99-latency.svg", "reopen.svg"} {
		b, err := os.ReadFile(filepath.Join(dir, "o", name))
		if err != nil {
			t.Fatal(err)
		}
		lastSubtitle := 0.0
		for _, s := range subtitle.FindAllStringSubmatch(string(b), -1) {
			y, _ := strconv.ParseFloat(s[1], 64)
			if y > lastSubtitle {
				lastSubtitle = y
			}
		}
		labels := rotated.FindAllStringSubmatch(string(b), -1)
		if lastSubtitle == 0 || len(labels) == 0 {
			t.Fatalf("%s: found %d subtitle lines and %d vertical labels", name, len(subtitle.FindAllString(string(b), -1)), len(labels))
		}
		for _, l := range labels {
			y, _ := strconv.ParseFloat(l[1], 64)
			top := y - textWidth(l[2], 10) // the label is written upward from its baseline point
			if top < lastSubtitle+6 {
				t.Errorf("%s: the label %q reaches up to y=%.0f, into the subtitle whose last line is at y=%.0f", name, l[2], top, lastSubtitle)
			}
		}
	}
}
