package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	d "github.com/rodriveracom/dostoevsky"
)

// A frame-valid directory whose content does not replay keeps its older format.
func TestFailedReplayLeavesTheOlderFormat(t *testing.T) {
	cfg := d.Config{Node: "workflow", Nodes: []string{"workflow"}, Topics: map[string]int{topic: 1}}
	dir := t.TempDir()
	n, err := d.OpenSegmented(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = n.Publish(context.Background(), topic, d.Message{Value: []byte("not a transition")}); err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "journal.v1"))
	if err != nil {
		t.Fatal(err)
	}
	if s, err := Open(dir); err == nil {
		s.Close()
		t.Fatal("a directory that does not replay was opened")
	}
	after, err := os.ReadFile(filepath.Join(dir, "journal.v1"))
	if err != nil || string(after) != string(before) {
		t.Fatalf("the format marker changed: %q then %q, %v", before, after, err)
	}
	if _, err = os.Stat(filepath.Join(dir, "WORKFLOW-ID")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WORKFLOW-ID was written: %v", err)
	}
	if n, err = d.OpenSegmented(dir, cfg); err != nil {
		t.Fatalf("the previous opener no longer reads the directory: %v", err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
}
