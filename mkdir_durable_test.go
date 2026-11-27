package dostoevsky

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func TestOpenSyncsEveryDirectoryAboveTheData(t *testing.T) {
	var synced []string
	fail := ""
	failure := errors.New("injected sync failure")
	original := syncParent
	syncParent = func(dir string) error {
		synced = append(synced, dir)
		if dir == fail {
			return failure
		}
		return original(dir)
	}
	defer func() { syncParent = original }()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(base, "a", "b", "c")
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	want := []string{base, filepath.Join(base, "a"), filepath.Join(base, "a", "b")}
	// A sync that fails part of the way stops Open.
	fail = base
	if _, err = Open(dir, cfg); !errors.Is(err, failure) {
		t.Fatalf("Open with a failing sync: %v", err)
	}
	// The retry finds every directory present and still syncs all of them.
	for _, round := range []string{"retry", "reopen"} {
		synced, fail = nil, ""
		n, err := Open(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = n.Close(); err != nil {
			t.Fatal(err)
		}
		for _, d := range want {
			if !slices.Contains(synced, d) {
				t.Fatalf("%s did not sync %s: %v", round, d, synced)
			}
		}
	}
}
