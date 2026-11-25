package main

import (
	"net/http/httptest"
	"strings"
	"testing"

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
