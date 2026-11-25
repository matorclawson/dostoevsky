package servers

import (
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
	for _, engine := range Engines {
		t.Run(engine, func(t *testing.T) {
			t.Setenv("SERVERS_STUB", engine)
			s, err := Start(engine, Options{Bin: exe, Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(s.Addr, "127.0.0.1:") || len(s.Token) != 64 {
				t.Fatalf("addr %q token length %d", s.Addr, len(s.Token))
			}
			var perr error
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				if _, perr = s.Proc.PeakKiB(); perr == nil {
					break // the first sample (a ps run on macOS) can take a moment
				}
			}
			if perr != nil {
				t.Fatal(perr)
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
	s := &Server{Engine: "nats", Token: strings.Repeat("a", 64)}
	other, err := Start("nats", Options{Bin: exe, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Stop()
	s.Addr = other.Addr // right address, but this token's name is not the one that process was started with
	if err := s.confirm(); err == nil {
		t.Fatal("a server with another name was accepted as ours")
	}
}

func TestConfigsCarryTheDocumentedDurability(t *testing.T) {
	r := RedisConfig("/d", 7000, "tok")
	for _, want := range []string{"port 7000\n", "bind 127.0.0.1\n", "requirepass tok\n", "appendonly yes\n", "appendfsync always\n", "save \"\"\n"} {
		if !strings.Contains(r, want) {
			t.Errorf("redis config lacks %q", want)
		}
	}
	n := NATSConfig("/d", 7001, strings.Repeat("a", 64))
	for _, want := range []string{"listen: 127.0.0.1:7001\n", "sync_interval: always", `store_dir: "/d/nats"`} {
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
