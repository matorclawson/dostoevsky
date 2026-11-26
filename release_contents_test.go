package dostoevsky_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// releaseSources is the top-level directories a source release has to hold. In a git checkout these are the ones with
// tracked files, so build output and other ignored or untracked directories do not count. Outside one (the extracted
// release itself, which may sit inside a checkout's ignored dist/) they are the directories that are there.
func releaseSources(t *testing.T) []string {
	t.Helper()
	here, err := filepath.Abs(".")
	if err == nil {
		here, err = filepath.EvalSymlinks(here)
	}
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	add := func(name string) {
		if !strings.HasPrefix(name, ".") && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	top, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if root, e := filepath.EvalSymlinks(strings.TrimSpace(string(top))); err == nil && e == nil && root == here {
		files, err := exec.Command("git", "ls-files", "-z").Output()
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range strings.Split(string(files), "\x00") {
			if dir, _, nested := strings.Cut(file, "/"); nested {
				add(dir)
			}
		}
		return names
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() && e.Name() != "dist" {
			add(e.Name())
		}
	}
	return names
}

// The source release is built from an explicit list in scripts/release.sh. A directory left off it is missing from the
// tarball: its packages cannot be built there and links into it break. Every source directory must be listed.
func TestTheSourceReleaseCopiesEveryDirectory(t *testing.T) {
	script, err := os.ReadFile("scripts/release.sh")
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, line := range strings.Split(string(script), "\n") {
		if fields := strings.Fields(line); len(fields) > 3 && fields[0] == "cp" && fields[1] == "-R" {
			listed = append(listed, fields[2:len(fields)-1]...)
		}
	}
	sources := releaseSources(t)
	if !slices.Contains(sources, "scripts") {
		t.Fatalf("the source directories were not found: %v", sources)
	}
	for _, name := range sources {
		if !slices.Contains(listed, name) {
			t.Errorf("scripts/release.sh does not copy %s/ into the source release (copies %v)", name, listed)
		}
	}
}
