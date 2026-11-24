package dostoevsky_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky"
)

// The README quickstart is the code between the two markers. TestQuickstartMatchesREADME
// fails when the two drift apart, so the first thing a visitor copies always runs.
func Example_quickstart() {
	dir, _ := os.MkdirTemp("", "dostoevsky-quickstart")
	defer os.RemoveAll(dir)
	ctx, stop := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stop()
	// quickstart:begin
	node, err := dostoevsky.Open(dir, dostoevsky.Config{Node: "app", Nodes: []string{"app"}, Topics: map[string]int{"orders": 1}})
	if err != nil {
		panic(err)
	}
	defer node.Close()
	rec, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: "order-1", Value: []byte("paid")})
	if err != nil {
		panic(err)
	}
	paid := 0
	// Fold commits its state and the input offset together, then waits for more events.
	_ = dostoevsky.Fold(ctx, node, rec.Partition, "paid", 0, func(_ context.Context, n int, _ dostoevsky.Record) (int, error) {
		paid = n + 1
		return paid, nil
	})
	fmt.Println("paid events:", paid)
	// quickstart:end
	// Output: paid events: 1
}

func block(t *testing.T, path, begin, end string, trim func(string) string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	in := false
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case strings.Contains(line, end):
			return out
		case in:
			out = append(out, strings.TrimSpace(line))
		case strings.Contains(line, begin):
			in = true
		}
	}
	t.Fatalf("%s: no %q ... %q block", path, begin, end)
	return nil
}

func TestQuickstartMatchesREADME(t *testing.T) {
	code := block(t, "example_quickstart_test.go", "quickstart:begin", "quickstart:end", nil)
	readme := block(t, "README.md", "<!-- quickstart:begin -->", "<!-- quickstart:end -->", nil)
	// The README block is a fenced code block: drop the fence lines and blanks.
	var got []string
	for _, l := range readme {
		if l != "" && !strings.HasPrefix(l, "```") {
			got = append(got, l)
		}
	}
	if strings.Join(got, "\n") != strings.Join(code, "\n") {
		t.Fatalf("README quickstart differs from example_quickstart_test.go\nREADME:\n%s\nexample:\n%s", strings.Join(got, "\n"), strings.Join(code, "\n"))
	}
}
