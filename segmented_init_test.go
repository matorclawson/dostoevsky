package dostoevsky

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// unmark puts a segmented directory back to the state just before its format marker was
// written.
func unmark(t *testing.T, dir string) {
	t.Helper()
	if err := os.Truncate(filepath.Join(dir, "journal.v1"), 0); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedFirstInitializationCanBeStartedAgain(t *testing.T) {
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	dir := t.TempDir()
	n, err := OpenSegmented(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	unmark(t, dir)
	if n, err = OpenSegmented(dir, cfg); err != nil {
		t.Fatalf("restart of an interrupted initialization: %v", err)
	}
	if _, err = n.Publish(context.Background(), "events", Message{Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	// With a record in it the directory is not an abandoned initialization, whatever
	// happened to the marker: nothing may be removed.
	unmark(t, dir)
	before, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		t.Fatal(err)
	}
	if n, err = OpenSegmented(dir, cfg); err == nil {
		n.Close()
		t.Fatal("an unmarked directory holding a record was initialized over")
	}
	after, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil || len(after) != len(before) {
		t.Fatalf("segments changed: %d then %d, %v", len(before), len(after), err)
	}
}
