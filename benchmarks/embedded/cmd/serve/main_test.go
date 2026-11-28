package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
			go func() { done <- serve(context.Background(), engine, exe, stdin, &out) }()
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
	if err := serve(context.Background(), "nope", "/bin/true", strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "unknown engine") {
		t.Fatalf("unknown engine: %v", err)
	}
	if err := serve(context.Background(), "redis", "", strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "no server binary") {
		t.Fatalf("no binary: %v", err)
	}
	t.Setenv("BENCH_KAFKA_HOME", "")
	t.Setenv("BENCH_JAVA_HOME", "")
	if err := serve(context.Background(), "kafka", "", strings.NewReader(""), &out); err == nil || !strings.Contains(err.Error(), "BENCH_KAFKA_HOME") {
		t.Fatalf("kafka without its homes: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("printed %q although nothing started", out.String())
	}
}

func TestServerBinComesFromTheFlagThenTheEnginesVariableThenTheGenericOne(t *testing.T) {
	env := map[string]string{"BENCH_REDIS_SERVER": "/opt/redis-server", "BENCH_SERVER_BIN": "/generic"}
	get := func(k string) string { return env[k] }
	for _, tc := range []struct{ engine, flag, want string }{
		{"redis", "/flag", "/flag"},
		{"redis", "", "/opt/redis-server"},
		{"nats", "", "/generic"},
		{"gateway", "", "/generic"},
	} {
		if got := serverBin(tc.engine, tc.flag, get); got != tc.want {
			t.Errorf("%s with flag %q: %q, want %q", tc.engine, tc.flag, got, tc.want)
		}
	}
	if got := serverBin("nats", "", func(string) string { return "" }); got != "" {
		t.Errorf("nothing set gave %q", got)
	}
}

// A stop that arrives while the server is still starting must stop it: serve is cancelled
// mid-start, returns promptly, prints nothing, leaves no process and removes its directory.
func TestACancelDuringStartupLeavesNothingBehind(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp) // the trial directory lands here, so leftovers are visible
	t.Setenv("SERVERS_STUB", "redis")
	t.Setenv("SERVERS_STUB_HANG", "1") // a server that never listens
	exe, _ := os.Executable()
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- serve(ctx, "redis", exe, strings.NewReader(""), &out) }()
	time.Sleep(500 * time.Millisecond) // the stub is up and has written its pid, and is not ready
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "cancelled") {
			t.Fatalf("serve after a cancel during startup: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not return after the cancel")
	}
	if out.Len() != 0 {
		t.Fatalf("printed %q although the server never became ready", out.String())
	}
	if left, _ := os.ReadDir(tmp); len(left) != 0 {
		t.Fatalf("the trial directory (it holds the token) was left behind: %v", left)
	}
}

func TestTheStubReallyWasRunningAndIsGoneAfterTheCancel(t *testing.T) {
	// Companion to the test above, checking the process: start, cancel, then the stub's pid must not answer.
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	t.Setenv("SERVERS_STUB", "redis")
	t.Setenv("SERVERS_STUB_HANG", "1")
	exe, _ := os.Executable()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, "redis", exe, strings.NewReader(""), io.Discard) }()
	var pid int
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && pid == 0; time.Sleep(20 * time.Millisecond) {
		matches, _ := filepath.Glob(filepath.Join(tmp, "embedded-serve-*", "hang.pid"))
		if len(matches) == 1 {
			if b, err := os.ReadFile(matches[0]); err == nil {
				pid, _ = strconv.Atoi(strings.TrimSpace(string(b)))
			}
		}
	}
	if pid == 0 {
		t.Fatal("the hanging stub never started")
	}
	cancel()
	<-done
	gone := false
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && !gone; time.Sleep(50 * time.Millisecond) {
		gone = syscall.Kill(pid, 0) != nil
	}
	if !gone {
		t.Fatalf("pid %d was orphaned by a cancel during startup", pid)
	}
}

// The order is decided by the command line the user types, flag default included: the earlier test
// called serverBin directly and could not see a flag default that beat the engine's variable.
func TestTheCommandLineResolvesTheBinaryInTheDocumentedOrder(t *testing.T) {
	env := map[string]string{"BENCH_REDIS_SERVER": "/opt/redis-server", "BENCH_SERVER_BIN": "/generic"}
	get := func(k string) string { return env[k] }
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-engine", "redis", "-bin", "/flag"}, "/flag"},
		{[]string{"-engine", "redis"}, "/opt/redis-server"}, // the engine's variable beats the generic one
		{[]string{"-engine", "nats"}, "/generic"},
		{[]string{"-engine=gateway"}, "/generic"},
	} {
		_, got, err := parseArgs(tc.args, get, io.Discard)
		if err != nil || got != tc.want {
			t.Errorf("%v: %q, %v; want %q", tc.args, got, err, tc.want)
		}
	}
	if _, _, err := parseArgs([]string{"-nope"}, get, io.Discard); err == nil {
		t.Error("an unknown flag was accepted")
	}
}

func TestHelpIsASuccessWithUsageOnTheWriterItWasGiven(t *testing.T) {
	var usage bytes.Buffer
	_, _, err := parseArgs([]string{"-help"}, func(string) string { return "" }, &usage)
	if !errors.Is(err, flag.ErrHelp) || exitCodeForArgs(err) != 0 {
		t.Fatalf("-help: err %v, exit %d; want flag.ErrHelp and exit 0", err, exitCodeForArgs(err))
	}
	if !strings.Contains(usage.String(), "-engine") || !strings.Contains(usage.String(), "-bin") {
		t.Fatalf("no usage text: %q", usage.String())
	}
	_, _, err = parseArgs([]string{"-nope"}, func(string) string { return "" }, io.Discard)
	if err == nil || exitCodeForArgs(err) != 2 {
		t.Fatalf("an unknown flag: err %v, exit %d; want an error and exit 2", err, exitCodeForArgs(err))
	}
}

func TestWhereTheUsageTextGoesFollowsTheResultOfParsing(t *testing.T) {
	none := func(string) string { return "" }
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"-help"}, 0},
		{[]string{"-nope", "-help"}, 2},
		{[]string{"-nope"}, 2},
	} {
		var stdout, stderr bytes.Buffer
		_, _, code, done := arguments(tc.args, none, &stdout, &stderr)
		told, silent := &stderr, &stdout
		if tc.code == 0 {
			told, silent = &stdout, &stderr
		}
		if !done || code != tc.code || silent.Len() != 0 || !strings.Contains(told.String(), "-engine") {
			t.Errorf("%v: code %d done %v, stdout %q, stderr %q", tc.args, code, done, stdout.String(), stderr.String())
		}
	}
}
