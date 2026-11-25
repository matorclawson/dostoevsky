package servers

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers/stubserver"
)

func TestMain(m *testing.M) {
	stubserver.Main()
	os.Exit(m.Run())
}

func TestEachEngineStartsAnswersAsItselfAndStops(t *testing.T) {
	exe, _ := os.Executable()
	for _, engine := range []string{"gateway", "redis", "nats"} { // kafka has no stub: it needs a real broker
		t.Run(engine, func(t *testing.T) {
			t.Setenv("SERVERS_STUB", engine)
			s, err := Start(engine, Options{Bin: exe, Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(s.Addr, "127.0.0.1:") || len(s.Token) != 64 {
				t.Fatalf("addr %q token length %d", s.Addr, len(s.Token))
			}
			// No waiting: Start itself guarantees a reading.
			if _, err := s.Proc.PeakKiB(); err != nil {
				t.Fatal(err)
			}
			if err := s.Stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEveryTrialGetsItsOwnPortAndToken(t *testing.T) {
	exe, _ := os.Executable()
	t.Setenv("SERVERS_STUB", "redis")
	a, err := Start("redis", Options{Bin: exe, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	b, err := Start("redis", Options{Bin: exe, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	if a.Addr == b.Addr || a.Token == b.Token {
		t.Fatalf("two trials shared %s / a token", a.Addr)
	}
}

func TestADifferentProcessOnTheAddressIsNotAccepted(t *testing.T) {
	// The stub answers as a nats-server with another name: the launched binary must be refused.
	exe, _ := os.Executable()
	t.Setenv("SERVERS_STUB", "nats")
	s := &Server{Engine: "nats", Token: strings.Repeat("a", 64), name: "bench-someone-else"}
	other, err := Start("nats", Options{Bin: exe, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Stop()
	s.Addr = other.Addr // right address, but this token's name is not the one that process was started with
	if err := s.confirm(context.Background()); err == nil {
		t.Fatal("a server with another name was accepted as ours")
	}
}

func TestConfigsCarryTheDocumentedDurability(t *testing.T) {
	r := RedisConfig("/d", 7000, "tok")
	for _, want := range []string{"port 7000\n", "bind 127.0.0.1\n", "requirepass tok\n", `dir "/d"` + "\n", "appendonly yes\n", "appendfsync always\n", "save \"\"\n"} {
		if !strings.Contains(r, want) {
			t.Errorf("redis config lacks %q", want)
		}
	}
	n := NATSConfig("/d", 7001, strings.Repeat("a", 64), "bench-abc")
	for _, want := range []string{"listen: 127.0.0.1:7001\n", "sync_interval: always", `store_dir: "/d/nats"`, `server_name: "bench-abc"`} {
		if !strings.Contains(n, want) {
			t.Errorf("nats config lacks %q:\n%s", want, n)
		}
	}
}

func TestUnknownEngineAndMissingBinary(t *testing.T) {
	if _, err := Start("nope", Options{Bin: "/bin/true", Dir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "unknown engine") {
		t.Fatalf("unknown engine: %v", err)
	}
	if _, err := Start("redis", Options{Dir: t.TempDir()}); err == nil {
		t.Fatal("started without a binary")
	}
}

// A server that answers the readiness check and is gone a moment later must still have been
// measured: the reading is taken by Start, not by a ticker that may lose the race.
func TestAServerThatStopsAtOnceStillHasAReading(t *testing.T) {
	exe, _ := os.Executable()
	for _, engine := range []string{"redis", "nats"} {
		t.Setenv("SERVERS_STUB", engine)
		t.Setenv("SERVERS_STUB_EXIT", "1")
		s, err := Start(engine, Options{Bin: exe, Dir: t.TempDir(), Sample: time.Hour}) // the ticker never fires
		if err != nil {
			t.Fatalf("%s: %v", engine, err)
		}
		kib, err := s.Proc.PeakKiB()
		s.Stop() // the exit is expected to be reported as an error or not; only the reading matters here
		if err != nil || kib == 0 {
			t.Fatalf("%s: no reading for a server that stopped at once: %d KiB, %v", engine, kib, err)
		}
	}
}

func TestKafkaNeedsItsHomesAndIsAnEngine(t *testing.T) {
	found := false
	for _, e := range Engines {
		found = found || e == "kafka"
	}
	if !found {
		t.Fatal("kafka is not listed as an engine")
	}
	if _, err := Start("kafka", Options{Dir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "BENCH_KAFKA_HOME") {
		t.Fatalf("kafka started without its homes: %v", err)
	}
}

func TestTheNatsNameIsNotDerivedFromTheToken(t *testing.T) {
	exe, _ := os.Executable()
	t.Setenv("SERVERS_STUB", "nats")
	s, err := Start("nats", Options{Bin: exe, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if strings.Contains(s.name, s.Token[:8]) || !strings.HasPrefix(s.name, "bench-") || len(s.name) != len("bench-")+16 {
		t.Fatalf("name %q looks derived from the token %q", s.name, s.Token[:8])
	}
}

func TestACancelledStartStopsTheServer(t *testing.T) {
	exe, _ := os.Executable()
	t.Setenv("SERVERS_STUB", "redis")
	t.Setenv("SERVERS_STUB_HANG", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, err := Start("redis", Options{Ctx: ctx, Bin: exe, Dir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("a start cancelled while the server never listened: %v", err)
	}
	if time.Since(begin) > 15*time.Second {
		t.Fatal("the cancelled start waited out the readiness bound")
	}
}
