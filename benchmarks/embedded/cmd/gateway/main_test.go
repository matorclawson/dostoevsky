package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/clienthttp"
)

func testStore(t *testing.T, token string) store {
	t.Helper()
	n, err := d.Open(t.TempDir(), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"*": 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	h, err := clienthttp.New(n, "a-test-token-with-entropy")
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return store{client: newClient(4), base: srv.URL, token: token}
}

func TestPublishThenReadBackOverHTTP(t *testing.T) {
	s := testStore(t, "a-test-token-with-entropy")
	for i, want := range []string{"first", "second\x00\xff", ""} {
		off, err := s.Publish([]byte(want))
		if err != nil || off != uint64(i) {
			t.Fatalf("publish %d: offset %d, %v", i, off, err)
		}
	}
	for i, want := range []string{"first", "second\x00\xff", ""} {
		got, err := s.Read(uint64(i))
		if err != nil || string(got) != want {
			t.Fatalf("read %d: %q, %v", i, got, err)
		}
	}
	if _, err := s.Read(99); err == nil || !strings.Contains(err.Error(), "nothing there") {
		t.Fatalf("a read past the end: %v", err)
	}
}

func TestAWrongTokenIsAFailureNeverACountedPublish(t *testing.T) {
	s := testStore(t, "not-the-right-token-at-all")
	if _, err := s.Publish([]byte("x")); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("publish with a wrong token: %v", err)
	}
}

func TestMemoryNeedsAServerProcess(t *testing.T) {
	if _, err := (store{}).ServerPeakKiB(); err == nil {
		t.Fatal("server memory was reported without a server")
	}
}

// The headline control is the DEFAULT: the gateway as the language cells get it, unpinned, label "gateway".
// A pinned run is a separate row and only when asked for.
func TestTheGatewayControlIsUnpinnedByDefaultAndPinnedOnlyOnRequest(t *testing.T) {
	t.Setenv("BENCH_GATEWAY_GOMAXPROCS", "")
	t.Setenv("GOMAXPROCS", "2") // set in the parent: the headline child must NOT inherit it
	if engineLabel() != "gateway" || len(gatewayEnv()) != 1 || gatewayEnv()[0] != "GOMAXPROCS=" {
		t.Fatalf("default: label %q env %v (it must clear an inherited GOMAXPROCS)", engineLabel(), gatewayEnv())
	}
	t.Setenv("BENCH_GATEWAY_GOMAXPROCS", "unpinned")
	if engineLabel() != "gateway" || len(gatewayEnv()) != 1 || gatewayEnv()[0] != "GOMAXPROCS=" {
		t.Fatalf("unpinned: label %q env %v", engineLabel(), gatewayEnv())
	}
	t.Setenv("BENCH_GATEWAY_GOMAXPROCS", "1")
	if engineLabel() != "gateway-pinned1" || len(gatewayEnv()) != 1 || gatewayEnv()[0] != "GOMAXPROCS=1" {
		t.Fatalf("pinned: label %q env %v", engineLabel(), gatewayEnv())
	}
}

func TestTheGatewayProcsSettingIsValidated(t *testing.T) {
	for _, ok := range []string{"", "unpinned", "1", "2", "16"} {
		if err := checkProcs(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"0", "-1", "01", "1.5", "two", " 1", "1 ", "pinned", "UNPINNED"} {
		if checkProcs(bad) == nil {
			t.Errorf("%q accepted: the label would not say what ran", bad)
		}
	}
}

// 315700: a request waiting on the gateway ends when the run context is cancelled, and says so (the memory guard
// recognizes its own cancel by errors.Is).
func TestCancellingTheRunContextEndsAWaitingGatewayRequest(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // runs before srv.Close
	for name, call := range map[string]func(store) error{
		"read":    func(s store) error { _, err := s.Read(0); return err },
		"publish": func(s store) error { _, err := s.Publish([]byte("x")); return err },
	} {
		ctx, cancel := context.WithCancel(context.Background())
		s := store{client: newClient(1), base: srv.URL, token: "t", ctx: func() context.Context { return ctx }}
		time.AfterFunc(50*time.Millisecond, cancel)
		begin := time.Now()
		err := call(s)
		if !errors.Is(err, context.Canceled) || time.Since(begin) > 5*time.Second {
			t.Errorf("%s: %v after %v", name, err, time.Since(begin))
		}
		cancel()
	}
}
