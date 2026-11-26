package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
	if o.MaxRetries != -1 || !o.ContextTimeoutEnabled || o.Network != "unix" || o.Addr != "/x.sock" {
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

// A loading:0 that arrives only after the readiness deadline is not ready: the
// bound is for the whole wait, not for each poll.
func TestLateLoadedReplyIsRejected(t *testing.T) {
	late := func(context.Context, string) (string, error) {
		time.Sleep(150 * time.Millisecond)
		return "loading:0\r\n", nil
	}
	err := waitLoaded(context.Background(), late, time.Millisecond, 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "after the") {
		t.Fatalf("a reply after the deadline was accepted: %v", err)
	}
}

// Each poll gets only the budget that is left.
func TestEachPollIsBoundedByTheRemainingBudget(t *testing.T) {
	var budgets []time.Duration
	info := func(c context.Context, _ string) (string, error) {
		d, ok := c.Deadline()
		if !ok {
			t.Error("a poll had no deadline")
		}
		budgets = append(budgets, time.Until(d))
		return "loading:1\r\n", nil
	}
	_ = waitLoaded(context.Background(), info, 10*time.Millisecond, 120*time.Millisecond)
	if len(budgets) < 2 {
		t.Fatalf("only %d polls", len(budgets))
	}
	for _, b := range budgets {
		if b > 125*time.Millisecond {
			t.Errorf("a poll was given %v although the whole wait is 120ms", b)
		}
	}
}

// 315700: a verification read waiting on a server that does not answer ends when the run context is cancelled.
func TestCancellingTheRunContextEndsAWaitingRead(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c) // held open, never answered
			mu.Unlock()
		}
	}()
	opts := clientOptions("")
	opts.Network, opts.Addr = "tcp", ln.Addr().String()
	client := redis.NewClient(opts)
	t.Cleanup(func() {
		client.Close()
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := store{client: client, ctx: func() context.Context { return ctx }}
	time.AfterFunc(50*time.Millisecond, cancel)
	begin := time.Now()
	if _, err := s.Read(1 << 16); !errors.Is(err, context.Canceled) || time.Since(begin) > 2*time.Second {
		t.Fatalf("%v after %v", err, time.Since(begin))
	}
}
