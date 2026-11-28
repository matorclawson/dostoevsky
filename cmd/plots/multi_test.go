package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var sevenEngines = []string{"dostoevsky", "jetstream", "sqlite", "redis", "valkey", "kafka", "rabbitmq"}

const multiMeta = `footer.1=test host; Linux; Go 1.0; GOMAXPROCS=2
footer.2=512-byte payloads; 16000 records per trial; synchronous acknowledgments
note.1=RabbitMQ: durability per vendor default, not verified; Erlang from the Hex.pm prebuilt
note.2=Kafka: fsync per message inferred from log.flush.interval.messages=1
command=make x
label.dostoevsky=Dostoevsky
label.jetstream=Embedded JetStream
label.sqlite=SQLite (single writer)
label.redis=Redis 8.12 (AOF always)
label.valkey=Valkey 9.2 (AOF always)
label.kafka=Kafka 4.4 (acks=all)
label.rabbitmq=RabbitMQ 4.4 (quorum queue)
arch.dostoevsky=in process
arch.jetstream=in process
arch.sqlite=in process
arch.redis=server, unix socket
arch.valkey=server, unix socket
arch.kafka=server, loopback
arch.rabbitmq=server, loopback
`

// multiLog builds a receipt with every engine at 1, 8 and 32 producers. rps maps an
// engine to its records/s at 32 producers; failed lists "engine/workers/trial" lines to
// write as FAILED instead.
func multiLog(rps map[string]int, failed map[string]bool) string {
	var b strings.Builder
	for tr := 1; tr <= 5; tr++ {
		for _, e := range sevenEngines {
			for _, w := range []int{1, 8, 32} {
				id := e + "/" + strconv.Itoa(w) + "/" + strconv.Itoa(tr)
				if failed[id] {
					b.WriteString("trial=" + strconv.Itoa(tr) + " engine=" + e + " workers=" + strconv.Itoa(w) + " FAILED boom\n")
					continue
				}
				v := 1000 * w
				if w == 32 {
					v = rps[e]
				}
				line := "trial=" + strconv.Itoa(tr) + " engine=" + e + " workers=" + strconv.Itoa(w) +
					" records/s=" + strconv.Itoa(v+tr) + " p99_us=" + strconv.Itoa(2000+tr) + " peakRSS_KiB=10240 reopen_ms=" + strconv.Itoa(50+tr)
				if e == "redis" || e == "valkey" || e == "kafka" || e == "rabbitmq" {
					line += " server_rss_KiB=30000 total_rss_KiB=40240 arch=server"
				}
				b.WriteString(line + "\n")
			}
		}
	}
	return b.String()
}

func withEngines(t *testing.T, engines []string, allow bool) {
	t.Helper()
	oldE, oldA := expectedEngines, allowFailures
	expectedEngines, allowFailures = engines, allow
	t.Cleanup(func() { expectedEngines, allowFailures = oldE, oldA })
}

var multiRPS = map[string]int{"dostoevsky": 16000, "jetstream": 4000, "sqlite": 2000, "redis": 9000, "valkey": 9100, "kafka": 12000, "rabbitmq": 3000}

func TestSevenEngineChartsAreDeterministicAndLabeled(t *testing.T) {
	withEngines(t, sevenEngines, false)
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, nil))
	m := write(t, dir, "m.txt", multiMeta)
	var first map[string]string
	for round := 0; round < 2; round++ {
		out := filepath.Join(dir, "out")
		if err := run(r, "*", m, filepath.Join(dir, "none"), out, 5); err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, name := range []string{"throughput.svg", "memory.svg", "p99-latency.svg", "reopen.svg"} {
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
	for _, want := range []string{
		"SQLite (single writer)", "Redis 8.12 (AOF always) (server, unix socket)", "Kafka 4.4 (acks=all) (server, loopback)",
		"Dostoevsky (in process)", "Valkey 9.2 (AOF always)", "RabbitMQ 4.4 (quorum queue)", "Embedded JetStream",
		"rotate(-90", "Dostoevsky wins at 32 producers.", "class=\"s6\"", "durability per vendor default, not verified",
		"test host; Linux", "median 16,003 of 5 trials",
	} {
		if !strings.Contains(th, want) {
			t.Errorf("throughput.svg lacks %q", want)
		}
	}
	// Memory uses the server-inclusive total where a line has it and the process peak otherwise.
	mem := first["memory.svg"]
	if !strings.Contains(mem, "median 39.3 of 5 trials") || !strings.Contains(mem, "median 10.0 of 5 trials") {
		t.Errorf("memory.svg must show total RSS for servers (39.3 MiB) and peak RSS for in-process engines (10.0 MiB)")
	}
	// The sentence naming a winner is derived from the data, so changing the data changes it.
	flipped := map[string]int{}
	for k, v := range multiRPS {
		flipped[k] = v
	}
	flipped["redis"] = 20000
	r2 := write(t, dir, "c2.log", multiLog(flipped, nil))
	out2 := filepath.Join(dir, "out2")
	if err := run(r2, "*", m, filepath.Join(dir, "none"), out2, 5); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(out2, "throughput.svg"))
	if !strings.Contains(string(b), "Redis 8.12 (AOF always) wins at 32 producers.") {
		t.Error("the winner sentence did not follow the data")
	}
}

// A receipt with FAILED trials is refused unless the run says failures are allowed; then
// they are shown, per engine and producer count, and never dropped.
func TestFailedTrialsAreShownNeverDropped(t *testing.T) {
	failed := map[string]bool{"redis/32/3": true}
	for tr := 1; tr <= 5; tr++ { // kafka failed entirely at 8 producers
		failed["kafka/8/"+strconv.Itoa(tr)] = true
	}
	dir := t.TempDir()
	r := write(t, dir, "c.log", multiLog(multiRPS, failed))
	m := write(t, dir, "m.txt", multiMeta)

	withEngines(t, sevenEngines, false)
	err := run(r, "*", m, filepath.Join(dir, "none"), filepath.Join(dir, "refused"), 5)
	if err == nil || !strings.Contains(err.Error(), "FAILED trial lines") {
		t.Fatalf("a receipt with failures was accepted without allow-failures: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "refused", "throughput.svg")); statErr == nil {
		t.Fatal("a chart was written before the receipt was refused")
	}

	withEngines(t, sevenEngines, true)
	out := filepath.Join(dir, "shown")
	if err := run(r, "*", m, filepath.Join(dir, "none"), out, 5); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(out, "throughput.svg"))
	if err != nil {
		t.Fatal(err)
	}
	svg := string(b)
	for _, want := range []string{
		"Redis 8.12 (AOF always): 1 failed",  // under 32 producers
		"Kafka 4.4 (acks=all): 5 failed",     // under 8 producers: no bar, but not dropped
		"median 9,003 of 4 trials, 1 failed", // the redis bar says it has 4 trials
		"class=\"f\"",
	} {
		if !strings.Contains(svg, want) {
			t.Errorf("throughput.svg lacks %q", want)
		}
	}
}

func TestAlternativeKeysAndEngineOrder(t *testing.T) {
	tr := trial{"peakRSS_KiB": "100", "total_rss_KiB": "900"}
	if v, ok := tr.numAny("total_rss_KiB|peakRSS_KiB"); !ok || v != 900 {
		t.Fatalf("%v %v", v, ok)
	}
	if v, ok := (trial{"peakRSS_KiB": "100"}).numAny("total_rss_KiB|peakRSS_KiB"); !ok || v != 100 {
		t.Fatalf("fallback: %v %v", v, ok)
	}
	if _, ok := (trial{}).numAny("a|b"); ok {
		t.Fatal("a missing key was reported")
	}
	var trials []trial
	for _, e := range []string{"rabbitmq", "kafka", "zzz", "dostoevsky", "redis"} {
		trials = append(trials, trial{"engine": e, "workers": "1", "records/s": "1"})
	}
	var got []string
	for _, s := range collect(trials, "records/s", 1) {
		got = append(got, s.engine)
	}
	if strings.Join(got, ",") != "dostoevsky,redis,kafka,rabbitmq,zzz" {
		t.Fatalf("series order %v", got)
	}
}

// Every series needs a color of its own: styleMulti defines maxSeries of them, and one more is refused, not drawn.
func TestAChartRefusesMoreSeriesThanItHasColors(t *testing.T) {
	for i := 0; i < maxSeries; i++ {
		for _, class := range []string{fmt.Sprintf(".s%d{", i), fmt.Sprintf(".d%d{", i)} {
			if !strings.Contains(styleMulti, class) {
				t.Errorf("no style for %s", class)
			}
		}
	}
	if strings.Contains(styleMulti, fmt.Sprintf(".s%d{", maxSeries)) {
		t.Errorf("there are more colors than maxSeries")
	}
	meta := map[string]string{}
	var engines []string
	for i := 0; i <= maxSeries; i++ {
		e := fmt.Sprintf("e%d", i)
		engines = append(engines, e)
		meta["label."+e], meta["arch."+e] = e, "in process"
	}
	if err := requireComparisonMeta(engines[:maxSeries], meta); err != nil {
		t.Fatalf("%d engines: %v", maxSeries, err)
	}
	if err := requireComparisonMeta(engines, meta); err == nil || !strings.Contains(err.Error(), "series colors") {
		t.Fatalf("%d engines: %v", len(engines), err)
	}
}
