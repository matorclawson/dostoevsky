package clienthttp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

func TestProtocol(t *testing.T) {
	n, err := d.Open(t.TempDir(), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"jobs": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	const token = "a-test-token-with-entropy"
	h, err := New(n, token)
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path, auth string, b []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, Prefix+path, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	check := func(method, path string, b []byte, status int) *httptest.ResponseRecorder {
		t.Helper()
		w := call(method, path, token, b)
		if w.Code != status {
			t.Fatalf("%s %s got %d: %q", method, path, w.Code, w.Body.String())
		}
		return w
	}
	if call("POST", "publish?topic=jobs", "wrong", nil).Code != 401 {
		t.Fatal("missing auth rejection")
	}
	check("POST", "publish?topic=jobs&id=%xx", nil, 400)
	check("POST", "publish?topic=jobs&topic=other", nil, 400)
	check("POST", "publish?topic=jobs", make([]byte, d.MaxValue+1), 413)
	const publish = "publish?topic=jobs&id=x&key=%2B%2F%CE%BB"
	for i := 0; i < 2; i++ {
		w := check("POST", publish, nil, 200)
		if w.Header().Get("Dostoevsky-Offset") != "0" {
			t.Fatal("idempotent offset")
		}
	}
	if code := strings.TrimSpace(check("POST", publish, []byte("different"), 409).Body.String()); code != "conflict" {
		t.Fatal(code)
	}
	const read = "read?topic=jobs&partition=0&offset="
	w := check("GET", read+"0", nil, 200)
	key, err := url.PathUnescape(w.Header().Get("Dostoevsky-Key"))
	if key != "+/λ" || err != nil || w.Body.Len() != 0 {
		t.Fatal("empty record or key corrupted")
	}
	check("GET", read+"9007199254740993", nil, 204)
	check("GET", read+"1&wait_ms=10", nil, 204)
	check("GET", read+"-1", nil, 400)
	check("GET", read+"1&wait_ms=30001", nil, 400)
	const group = "topic=jobs&partition=0&group=worker"
	state := []byte{0, 255}
	check("POST", "commit?"+group+"&expected=0", state, 204)
	check("POST", "commit?"+group+"&expected=0", nil, 409)
	pos := check("GET", "position?"+group, nil, 200)
	if pos.Header().Get("Dostoevsky-Next") != "1" || !bytes.Equal(pos.Body.Bytes(), state) {
		t.Fatal("checkpoint mismatch")
	}
	if err := n.Retain(context.Background(), map[d.Partition]int64{{Topic: "jobs"}: 1}); err != nil {
		t.Fatal(err)
	}
	check("GET", read+"0", nil, http.StatusGone)
}

func TestNewRefusesAWeakTokenOrNoStore(t *testing.T) {
	n, err := d.Open(t.TempDir(), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"jobs": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err := New(n, "short"); err == nil {
		t.Fatal("a token under 16 bytes was accepted")
	}
	if _, err := New(nil, "a-test-token-with-entropy"); err == nil {
		t.Fatal("a nil store was accepted")
	}
}

func TestMissingAuthorizationIsRejectedBeforeAnyWork(t *testing.T) {
	n, err := d.Open(t.TempDir(), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"jobs": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	h, _ := New(n, "a-test-token-with-entropy")
	for _, path := range []string{"publish?topic=jobs", "read?topic=jobs&partition=0&offset=0", "nope"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", Prefix+path, nil))
		if w.Code != 401 {
			t.Fatalf("%s without a token got %d", path, w.Code)
		}
	}
	if _, err := n.Read(context.Background(), d.Partition{Topic: "jobs"}, 0, false); err == nil {
		t.Fatal("an unauthenticated publish reached the log")
	}
}

// A long poll must return the record as soon as it is published, not at the end of
// its wait.
func TestLongPollReturnsWhenARecordArrives(t *testing.T) {
	n, err := d.Open(t.TempDir(), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"jobs": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	const token = "a-test-token-with-entropy"
	h, _ := New(n, token)
	got := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest("GET", Prefix+"read?topic=jobs&partition=0&offset=0&wait_ms=20000", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		got <- w
	}()
	time.Sleep(200 * time.Millisecond) // let the poll block
	if _, err := n.Publish(context.Background(), "jobs", d.Message{Value: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-got:
		if w.Code != 200 || w.Body.String() != "hello" {
			t.Fatalf("long poll got %d %q", w.Code, w.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll did not return when a record was published")
	}
}

// A journal at its cap is told apart from a request that is too large.
func TestAFullJournalIsNotAnOversizedRequest(t *testing.T) {
	n, err := d.Open(t.TempDir(), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"jobs": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.SetJournalCap(n.JournalSize()); err != nil {
		t.Fatal(err)
	}
	const token = "a-test-token-with-entropy"
	h, err := New(n, token)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", Prefix+"publish?topic=jobs", strings.NewReader("v"))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 507 || strings.TrimSpace(w.Body.String()) != "journal_full" {
		t.Fatalf("%d %q", w.Code, w.Body.String())
	}
}
