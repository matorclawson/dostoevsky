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
