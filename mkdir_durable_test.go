package dostoevsky

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestOpenSyncsTheParentsOfNewDirectories(t *testing.T) {
	var synced []string
	original := syncParent
	syncParent = func(dir string) error {
		synced = append(synced, dir)
		return original(dir)
	}
	defer func() { syncParent = original }()
	base := t.TempDir()
	dir := filepath.Join(base, "a", "b", "c")
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	n, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	want := []string{base, filepath.Join(base, "a"), filepath.Join(base, "a", "b")}
	if !slices.Equal(synced, want) {
		t.Fatalf("synced %v, want %v", synced, want)
	}
	synced = nil
	if n, err = Open(dir, cfg); err != nil {
		t.Fatal(err)
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	if want = []string{filepath.Join(base, "a", "b")}; !slices.Equal(synced, want) {
		t.Fatalf("reopen synced %v, want %v", synced, want)
	}
}
