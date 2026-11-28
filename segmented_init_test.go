package dostoevsky

import (
	"context"
	"errors"
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

// abandoned returns a directory left by a first initialization that stopped with keep
// bytes of its marker written.
func abandoned(t *testing.T, cfg Config, keep int) string {
	t.Helper()
	dir := t.TempDir()
	n, err := OpenSegmented(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(filepath.Join(dir, "journal.v1"), int64(keep)); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A failure at each directory sync of the cleanup must leave something the next open
// can still recover.
func TestInterruptedCleanupOfAnInitializationCanBeRetried(t *testing.T) {
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	failure := errors.New("injected sync failure")
	original := syncDir
	defer func() { syncDir = original }()
	for _, keep := range []int{0, 7} {
		for _, at := range []string{"segments", "parent"} {
			dir := abandoned(t, cfg, keep)
			target := dir
			if at == "segments" {
				target = filepath.Join(dir, "segments")
			}
			failed := false
			syncDir = func(p string) error {
				if p == target && !failed {
					failed = true
					return failure
				}
				return original(p)
			}
			if n, err := OpenSegmented(dir, cfg); !errors.Is(err, failure) {
				if err == nil {
					n.Close()
				}
				t.Fatalf("keep %d, %s: %v, want the injected failure", keep, at, err)
			}
			syncDir = original
			n, err := OpenSegmented(dir, cfg)
			if err != nil {
				t.Fatalf("keep %d, %s: retry: %v", keep, at, err)
			}
			if err = n.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// The other shapes a stopped initialization or cleanup leaves.
func TestAbandonedInitializationShapes(t *testing.T) {
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	shapes := map[string]func(segments string) error{
		"no range file": func(p string) error { return os.Remove(filepath.Join(p, "RANGE")) },
		"empty": func(p string) error {
			return errors.Join(os.Remove(filepath.Join(p, "RANGE")), os.Remove(filepath.Join(p, segName(1))))
		},
		"staging only": func(p string) error {
			return errors.Join(os.Remove(filepath.Join(p, "RANGE")), os.Rename(filepath.Join(p, segName(1)), filepath.Join(p, segTmpName(1))))
		},
	}
	for name, shape := range shapes {
		dir := abandoned(t, cfg, 0)
		if err := shape(filepath.Join(dir, "segments")); err != nil {
			t.Fatal(err)
		}
		n, err := OpenSegmented(dir, cfg)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err = n.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
