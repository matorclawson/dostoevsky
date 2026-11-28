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

// A torn first write of the marker leaves a prefix of it; that is the same interrupted
// initialization. A range file naming more than the first segment is not.
func TestTornMarkerAndForeignRange(t *testing.T) {
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	for _, keep := range []int{1, 12, len(segmentMarker) - 1} {
		dir := t.TempDir()
		n, err := OpenSegmented(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = n.Close(); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(dir, "journal.v1")
		if err = os.Truncate(marker, int64(keep)); err != nil {
			t.Fatal(err)
		}
		if n, err = Open(dir, cfg); err == nil {
			n.Close()
			t.Fatalf("%d bytes: plain Open accepted a torn marker", keep)
		}
		if n, err = OpenSegmented(dir, cfg); err != nil {
			t.Fatalf("%d bytes: %v", keep, err)
		}
		if err = n.Close(); err != nil {
			t.Fatal(err)
		}
		unmark(t, dir)
		named := filepath.Join(dir, "segments", "RANGE")
		if err = os.WriteFile(named, rangeBytes(1, 3), 0600); err != nil {
			t.Fatal(err)
		}
		if n, err = OpenSegmented(dir, cfg); err == nil {
			n.Close()
			t.Fatal("a directory whose range names later segments was cleared")
		}
		if _, err = os.Stat(named); err != nil {
			t.Fatalf("the range file was removed: %v", err)
		}
	}
}
