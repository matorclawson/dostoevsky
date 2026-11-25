package dostoevsky_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky"
)

// The code between "snippet:NAME" and "endsnippet:NAME" markers appears verbatim
// in docs/getting-started.md and docs/integrations.md, and TestDocSnippetsMatch
// fails when a copy drifts. Each example runs its snippet, so every step in the
// guides is executed by `go test`.

// demo opens a fresh single-node store with one topic of one partition. The
// snippets assume cfg, dir, node, ctx and p as this helper defines them.
func demo() (dir string, cfg dostoevsky.Config, node *dostoevsky.Node, ctx context.Context, p dostoevsky.Partition, done func()) {
	dir, _ = os.MkdirTemp("", "dostoevsky-guide")
	cfg = dostoevsky.Config{Node: "app", Nodes: []string{"app"}, Topics: map[string]int{"orders": 1}}
	var err error
	if node, err = dostoevsky.Open(dir, cfg); err != nil {
		panic(err)
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	p = dostoevsky.Partition{Topic: "orders", Number: 0}
	return dir, cfg, node, ctx, p, func() { cancel(); node.Close(); os.RemoveAll(dir) }
}

func Example_publishAndRead() {
	_, _, node, ctx, p, done := demo()
	defer done()
	// snippet:publish
	rec, err := node.Publish(ctx, "orders", dostoevsky.Message{Key: "account-42", ID: "order-1", Value: []byte("paid")})
	if err != nil {
		panic(err)
	}
	// Retrying with the same ID returns the original record instead of a duplicate.
	again, err := node.Publish(ctx, "orders", dostoevsky.Message{Key: "account-42", ID: "order-1", Value: []byte("paid")})
	if err != nil {
		panic(err)
	}
	got, err := node.Read(ctx, p, rec.Offset, false)
	if err != nil {
		panic(err)
	}
	fmt.Println(rec.Offset, again.Offset, string(got.Value))
	// endsnippet:publish

	// Output: 0 0 paid
}

func Example_consumerGroups() {
	_, _, node, ctx, p, done := demo()
	defer done()
	// snippet:groups
	for _, id := range []string{"a", "b", "c"} {
		if _, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: id, Value: []byte(id)}); err != nil {
			panic(err)
		}
	}
	// "billing" consumes two records. "audit" has not started and still sees all three.
	for offset := int64(0); offset < 2; offset++ {
		if err := node.Commit(ctx, p, "billing", offset, nil); err != nil {
			panic(err)
		}
	}
	billing, _ := node.Position(ctx, p, "billing")
	audit, _ := node.Position(ctx, p, "audit")
	// A second commit at an offset that already moved loses: one winner per cursor.
	stale := node.Commit(ctx, p, "billing", 0, nil)
	fmt.Println(billing.Next, audit.Next, errors.Is(stale, dostoevsky.ErrConflict))
	// endsnippet:groups

	// Output: 2 0 true
}

func Example_restartAndResume() {
	dir, cfg, node, ctx, p, done := demo()
	defer func() { node.Close(); done() }()
	var err error // the guide's setup block declares it
	// snippet:restart
	for _, id := range []string{"a", "b"} {
		if _, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: id, Value: []byte(id)}); err != nil {
			panic(err)
		}
	}
	if err := node.Commit(ctx, p, "billing", 0, []byte(`{"seen":1}`)); err != nil {
		panic(err)
	}
	if err := node.Close(); err != nil {
		panic(err)
	}
	// Reopening the same directory recovers records, offsets and group state.
	node, err = dostoevsky.Open(dir, cfg)
	if err != nil {
		panic(err)
	}
	pos, _ := node.Position(ctx, p, "billing")
	fmt.Println(pos.Next, string(pos.State))
	// endsnippet:restart

	// Output: 1 {"seen":1}
}

func Example_retention() {
	_, _, node, ctx, p, done := demo()
	defer done()
	// snippet:retention
	for i := 0; i < 5; i++ {
		if _, err := node.Publish(ctx, "orders", dostoevsky.Message{Value: []byte(fmt.Sprint("event-", i))}); err != nil {
			panic(err)
		}
	}
	for offset := int64(0); offset < 3; offset++ {
		if err := node.Commit(ctx, p, "billing", offset, nil); err != nil {
			panic(err)
		}
	}
	// Remove every record below offset 3. A floor above any group's cursor is refused.
	if err := node.Retain(ctx, map[dostoevsky.Partition]int64{p: 3}); err != nil {
		panic(err)
	}
	_, expired := node.Read(ctx, p, 0, false)
	kept, _ := node.Read(ctx, p, 3, false)
	fmt.Println(errors.Is(expired, dostoevsky.ErrExpired), string(kept.Value))
	// endsnippet:retention

	// Output: true event-3
}

func Example_backup() {
	dir, cfg, node, ctx, p, done := demo()
	defer done()
	backupDir := filepath.Join(filepath.Dir(dir), filepath.Base(dir)+"-backup")
	defer os.RemoveAll(backupDir)
	// snippet:backup
	if _, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: "order-1", Value: []byte("paid")}); err != nil {
		panic(err)
	}
	// The destination must not exist. Backup syncs the copy before it returns.
	if err := node.Backup(ctx, backupDir); err != nil {
		panic(err)
	}
	copyNode, err := dostoevsky.Open(backupDir, cfg)
	if err != nil {
		panic(err)
	}
	defer copyNode.Close()
	rec, _ := copyNode.Read(ctx, p, 0, false)
	fmt.Println(string(rec.Value))
	// endsnippet:backup

	// Output: paid
}

func Example_httpHandler() {
	_, _, node, _, _, done := demo()
	defer done()
	// snippet:http
	// An HTTP handler that publishes the request body. The response means the event is durable.
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		rec, err := node.Publish(r.Context(), "orders", dostoevsky.Message{ID: r.Header.Get("Idempotency-Key"), Value: body})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "offset %d\n", rec.Offset)
	}
	// endsnippet:http
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("POST", "/events", strings.NewReader("paid"))
		req.Header.Set("Idempotency-Key", "order-1")
		w := httptest.NewRecorder()
		handler(w, req)
		fmt.Print(w.Body.String())
	}
	// Output:
	// offset 0
	// offset 0
}

var docSnippets = map[string][]string{
	"docs/getting-started.md": {"publish", "groups", "restart", "retention", "backup"},
	"docs/integrations.md":    {"http"},
}

// source returns the trimmed, non-blank lines of one snippet in this file.
func source(t *testing.T, name string) []string {
	t.Helper()
	b, err := os.ReadFile("getting_started_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	in := false
	for _, line := range strings.Split(string(b), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "// endsnippet:"+name:
			return out
		case in && trimmed != "":
			out = append(out, trimmed)
		case trimmed == "// snippet:"+name:
			in = true
		}
	}
	t.Fatalf("snippet %q not found", name)
	return nil
}

// documented returns the lines of the fenced block that follows "<!-- snippet:NAME -->".
func documented(t *testing.T, path, name string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	armed, in := false, false
	for _, line := range strings.Split(string(b), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "<!-- snippet:"+name+" -->":
			armed = true
		case armed && strings.HasPrefix(trimmed, "```") && !in:
			in = true
		case in && strings.HasPrefix(trimmed, "```"):
			return out
		case in && trimmed != "":
			out = append(out, trimmed)
		}
	}
	t.Fatalf("%s: no fenced block after snippet marker %q", path, name)
	return nil
}

func TestDocSnippetsMatch(t *testing.T) {
	for path, names := range docSnippets {
		for _, name := range names {
			want, got := strings.Join(source(t, name), "\n"), strings.Join(documented(t, path, name), "\n")
			if want != got {
				t.Errorf("%s snippet %q differs from getting_started_test.go\ndoc:\n%s\ntest:\n%s", path, name, got, want)
			}
		}
	}
}
