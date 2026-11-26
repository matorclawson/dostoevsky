package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// A unix socket path is limited to about 100 bytes, so the test directory is kept short.
func shortDir(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "ds")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestUnixSocketIsPrivate(t *testing.T) {
	path := filepath.Join(shortDir(t), "run", "s")
	ln, err := listenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	for p, want := range map[string]os.FileMode{filepath.Dir(path): 0700, path: 0600} {
		info, err := os.Lstat(p)
		if err != nil || info.Mode().Perm() != want {
			t.Fatalf("%s: %v %v, want %v", p, info.Mode(), err, want)
		}
	}
}

func TestUnixSocketRefusals(t *testing.T) {
	base := shortDir(t)
	open := filepath.Join(base, "open")
	if err := os.Mkdir(open, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0755); err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(base, "private")
	if err := os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(private, "file")
	if err := os.WriteFile(file, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	pointer := filepath.Join(private, "pointer")
	if err := os.Symlink(file, pointer); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(open, "s"), filepath.Join(link, "s"), file, pointer} {
		if ln, err := listenUnix(path); err == nil {
			ln.Close()
			t.Errorf("%s accepted", path)
		}
	}
	if b, err := os.ReadFile(file); err != nil || string(b) != "keep" {
		t.Fatalf("existing file touched: %q %v", b, err)
	}
}

func TestAStaleSocketOfOursIsReplaced(t *testing.T) {
	path := filepath.Join(shortDir(t), "run", "s")
	first, err := listenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	// Leave the file behind, as a killed process would.
	first.(*net.UnixListener).SetUnlinkOnClose(false)
	first.Close()
	second, err := listenUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
}
