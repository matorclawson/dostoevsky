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

// A new workflow directory stopped before or during its format marker opens again.
func TestInterruptedWorkflowInitializationCanBeStartedAgain(t *testing.T) {
	for _, keep := range []int64{0, 5} {
		dir := t.TempDir()
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		// Back to the state before the marker was complete: no identity yet, and the
		// beginning of the segmented marker.
		if err = os.Remove(filepath.Join(dir, "WORKFLOW-ID")); err != nil {
			t.Fatal(err)
		}
		marker := "DOSTOEVSKY-SEGMENTED-01\n"
		if err = os.WriteFile(filepath.Join(dir, "journal.v1"), []byte(marker[:keep]), 0600); err != nil {
			t.Fatal(err)
		}
		if s, err = Open(dir); err != nil {
			t.Fatalf("%d bytes: %v", keep, err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// The other shapes a stopped initialization leaves, through the workflow opener.
func TestWorkflowOpensEveryAbandonedShape(t *testing.T) {
	const first, staged = "seg-0000000000000001.log", "seg-0000000000000001.tmp"
	shapes := map[string]func(segments string) error{
		"no range file": func(p string) error { return os.Remove(filepath.Join(p, "RANGE")) },
		"empty": func(p string) error {
			return errors.Join(os.Remove(filepath.Join(p, "RANGE")), os.Remove(filepath.Join(p, first)))
		},
		"staging only": func(p string) error {
			return errors.Join(os.Remove(filepath.Join(p, "RANGE")), os.Rename(filepath.Join(p, first), filepath.Join(p, staged)))
		},
	}
	for name, shape := range shapes {
		dir := t.TempDir()
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if err = errors.Join(os.Remove(filepath.Join(dir, "WORKFLOW-ID")), os.Truncate(filepath.Join(dir, "journal.v1"), 0)); err != nil {
			t.Fatal(err)
		}
		if err = shape(filepath.Join(dir, "segments")); err != nil {
			t.Fatal(err)
		}
		if s, err = Open(dir); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
