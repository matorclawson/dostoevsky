package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConfigIsPrivateToTheTrial(t *testing.T) {
	conf, sock := serverConfig("/data/trial-1")
	if !strings.Contains(conf, "port 0\n") || !strings.Contains(conf, "unixsocket /data/trial-1/server.sock\n") || sock != "/data/trial-1/server.sock" {
		t.Fatalf("server is not private to the trial:\n%s", conf)
	}
	if strings.Contains(conf, "6390") || strings.Contains(conf, "bind ") {
		t.Fatalf("a fixed TCP address is configured:\n%s", conf)
	}
	for _, want := range []string{"appendonly yes\n", "appendfsync always\n"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("missing durability setting %q", want)
		}
	}
}

// An ambiguous XADD must never be retried with a new generated ID.
func TestClientDoesNotRetryCommands(t *testing.T) {
	o := clientOptions("/x.sock")
	if o.MaxRetries != -1 || o.Network != "unix" || o.Addr != "/x.sock" {
		t.Fatalf("options: retries %d, network %q, addr %q", o.MaxRetries, o.Network, o.Addr)
	}
}

func TestStreamIDsRoundTripThroughTheHarnessOffset(t *testing.T) {
	for _, id := range []string{"1700000000000-0", "1700000000123-7", "5-65535"} {
		v, err := pack(id)
		if err != nil {
			t.Fatal(err)
		}
		if got := unpack(v); got != id {
			t.Fatalf("%s came back as %s", id, got)
		}
	}
	if _, err := pack("1-65536"); err == nil {
		t.Fatal("a sequence that does not fit 16 bits was accepted")
	}
}

func TestServerPidIsParsedFromInfo(t *testing.T) {
	pid, err := serverPid("# Server\r\nredis_version:8.12.0\r\nprocess_id:4242\r\ntcp_port:0\r\n")
	if err != nil || pid != 4242 {
		t.Fatalf("pid %d, %v", pid, err)
	}
	if _, err := serverPid("# Server\r\n"); err == nil {
		t.Fatal("missing process_id accepted")
	}
}

// A fake server that answers loading:1, then a LOADING error, then loading:0.
func fakeInfo(replies ...any) (func(context.Context, string) (string, error), *int) {
	calls := 0
	return func(context.Context, string) (string, error) {
		r := replies[min(calls, len(replies)-1)]
		calls++
		if err, ok := r.(error); ok {
			return "", err
		}
		return r.(string), nil
	}, &calls
}

func TestReadinessWaitsForTheDatasetToLoad(t *testing.T) {
	info, calls := fakeInfo("# Persistence\r\nloading:1\r\n", "loading:1\r\n", errors.New("LOADING Redis is loading the dataset in memory"), "# Persistence\r\nloading:0\r\n")
	if err := waitLoaded(context.Background(), info, time.Millisecond, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if *calls != 4 {
		t.Fatalf("polled %d times, want 4 (two loading:1, one LOADING error, then loading:0)", *calls)
	}
}

func TestReadinessFailsOnOtherErrorsAndOnTheBound(t *testing.T) {
	info, _ := fakeInfo(errors.New("ERR something else"))
	if err := waitLoaded(context.Background(), info, time.Millisecond, time.Second); err == nil || !strings.Contains(err.Error(), "something else") {
		t.Fatalf("a non-LOADING error must fail readiness: %v", err)
	}
	stuck, _ := fakeInfo("loading:1\r\n")
	if err := waitLoaded(context.Background(), stuck, time.Millisecond, 50*time.Millisecond); err == nil || !strings.Contains(err.Error(), "still loading") {
		t.Fatalf("a dataset that never finishes loading must hit the bound: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stuck2, _ := fakeInfo("loading:1\r\n")
	if err := waitLoaded(ctx, stuck2, time.Millisecond, time.Hour); err == nil || !strings.Contains(err.Error(), "readiness stopped") {
		t.Fatalf("a cancelled trial context must stop readiness: %v", err)
	}
	missing, _ := fakeInfo("# Persistence\r\nrdb_changes_since_last_save:0\r\n")
	if err := waitLoaded(context.Background(), missing, time.Millisecond, time.Second); err == nil || !strings.Contains(err.Error(), "no loading field") {
		t.Fatalf("a reply without the loading field must fail: %v", err)
	}
}

func TestLoadingFlag(t *testing.T) {
	for in, want := range map[string]bool{"loading:0": false, "x\r\nloading:1\r\ny": true} {
		if got, err := loadingFlag(in); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	if _, err := loadingFlag("loading:2"); err == nil {
		t.Error("an unknown loading value was accepted")
	}
}
