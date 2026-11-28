package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUnixSocketRefusesAWritableAncestor(t *testing.T) {
	base := shortDir(t)
	shared := filepath.Join(base, "shared")
	if err := os.Mkdir(shared, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0777); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(shared, "run", "s")
	if ln, err := listenUnix(path); err == nil {
		ln.Close()
		t.Fatal("listened below a directory anyone can write to")
	}
	// The same layout under a sticky directory is accepted: others cannot move our entry.
	if err := os.Chmod(shared, 0777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	ln, err := listenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
}

func TestUnixSocketRefusesALinkThroughAWritableDirectory(t *testing.T) {
	base := shortDir(t)
	shared := filepath.Join(base, "shared")
	safe := filepath.Join(base, "safe")
	for _, d := range []string{shared, filepath.Join(shared, "t"), safe} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(shared, 0777); err != nil {
		t.Fatal(err)
	}
	// safe/link leads through shared, where another user could replace t.
	if err := os.Symlink(filepath.Join(shared, "t"), filepath.Join(safe, "link")); err != nil {
		t.Fatal(err)
	}
	if ln, err := listenUnix(filepath.Join(safe, "link", "run", "s")); err == nil {
		ln.Close()
		t.Fatal("followed a link through a directory anyone can write to")
	}
	if _, err := os.Lstat(filepath.Join(shared, "t", "run")); !os.IsNotExist(err) {
		t.Fatalf("a directory was created before the path was refused: %v", err)
	}
}
