package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers/stubserver"
)

func TestMain(m *testing.M) {
	stubserver.Main() // the test binary doubles as the stand-in server
	os.Exit(m.Run())
}

func TestServeReadyThenStoppedWithMemory(t *testing.T) {
	for _, engine := range []string{"gateway", "redis", "nats"} {
		t.Run(engine, func(t *testing.T) {
			t.Setenv("SERVERS_STUB", engine)
			stdin, closeStdin := io.Pipe()
			var out safeBuffer
			done := make(chan error, 1)
			exe, _ := os.Executable()
			go func() { done <- serve(engine, exe, stdin, &out) }()
			deadline := time.Now().Add(30 * time.Second)
			for !strings.Contains(out.String(), "READY ") {
				if time.Now().After(deadline) {
					t.Fatalf("no READY line: %q", out.String())
				}
				select {
				case err := <-done:
					t.Fatalf("serve ended early: %v (%q)", err, out.String())
				case <-time.After(20 * time.Millisecond):
				}
			}
			line := strings.SplitN(out.String(), "\n", 2)[0]
			for _, want := range []string{"addr=127.0.0.1:", " pid=", " token="} {
				if !strings.Contains(line, want) {
					t.Fatalf("READY line lacks %q: %s", want, line)
				}
			}
			closeStdin.Close() // the driver is done
			if err := <-done; err != nil {
				t.Fatalf("serve: %v", err)
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != 2 || !strings.HasPrefix(lines[1], "STOPPED server_rss_KiB=") || !strings.HasSuffix(lines[1], " verdict=clean") || !strings.Contains(lines[1], " rss_source="+serverproc.Source()+" ") {
				t.Fatalf("stdout is not exactly READY then STOPPED: %q", out.String())
			}
		})
	}
}

func TestServeRefusesWhatItCannotStart(t *testing.T) {
	var out bytes.Buffer
	if err := serve("nope", "/bin/true", strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "unknown engine") {
		t.Fatalf("unknown engine: %v", err)
	}
	if err := serve("redis", "", strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "no server binary") {
		t.Fatalf("no binary: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("printed %q although nothing started", out.String())
	}
}
