package dostoevsky_test

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// The source release is built from an explicit list in scripts/release.sh. A directory left off it is missing from the
// tarball: its packages cannot be built there and links into it break. Every top-level directory must be listed.
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
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || strings.HasPrefix(name, ".") || name == "dist" {
			continue
		}
		if !slices.Contains(listed, name) {
			t.Errorf("scripts/release.sh does not copy %s/ into the source release (copies %v)", name, listed)
		}
	}
}
