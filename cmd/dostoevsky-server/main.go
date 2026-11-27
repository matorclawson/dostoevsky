// Command dostoevsky-server runs a single embedded node behind the native wire protocol (docs/wire-v1.md).
//
// The token in DOSTOEVSKY_TOKEN grants access to the whole store, so the server listens on loopback by default and
// refuses any other address unless TLS is configured. Make a token with: dostoevsky-gateway -gen-token
package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/wireserver"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func ownedByUs(info fs.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Geteuid()
}

// unixListener holds the lock of its socket path until it is closed.
type unixListener struct {
	*net.UnixListener
	lock *os.File
}

func (l *unixListener) Close() error {
	// The socket is removed first, while the path is still locked.
	return errors.Join(l.UnixListener.Close(), l.lock.Close())
}

// lockPath takes the lock that says who owns the socket path. The file stays: removing it would let two servers
// lock different files.
func lockPath(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another server holds the socket path %s: %w", path, err)
	}
	return f, nil
}

// trustedDir resolves the existing directory dir one component at a time and returns the
// path with no symbolic link left in it. Every directory passed through, including those
// a link leads through, and dir itself, must belong to root or to this user and must not
// be writable by group or others unless it is sticky (as /tmp is); every link followed
// must belong to root or to this user. Clients send the token to the socket below it, so
// no other user may be able to move or redirect it.
func trustedDir(dir string) (string, error) {
	if !filepath.IsAbs(dir) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd + string(filepath.Separator) + dir
	}
	check := func(p string, info fs.FileInfo) error {
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (st.Uid != 0 && int(st.Uid) != os.Geteuid()) {
			return fmt.Errorf("socket path: %s belongs to another user", p)
		}
		if info.IsDir() && info.Mode().Perm()&0022 != 0 && info.Mode()&fs.ModeSticky == 0 {
			return fmt.Errorf("socket path: %s is writable by other users and is not sticky", p)
		}
		return nil
	}
	root := string(filepath.Separator)
	info, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	if err = check(root, info); err != nil {
		return "", err
	}
	// Not cleaned first: ".." after a link means the parent of the link's target.
	at, rest, links := root, strings.Split(dir, root), 0
	for len(rest) > 0 {
		name := rest[0]
		rest = rest[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			at = filepath.Dir(at)
			continue
		}
		next := filepath.Join(at, name)
		info, err := os.Lstat(next)
		if err != nil {
			return "", err
		}
		if err = check(next, info); err != nil {
			return "", err
		}
		if info.Mode().Type() == fs.ModeSymlink {
			if links++; links > 40 {
				return "", fmt.Errorf("socket path: too many symbolic links at %s", next)
			}
			target, err := os.Readlink(next)
			if err != nil {
				return "", err
			}
			if filepath.IsAbs(target) {
				at = root
			}
			rest = append(strings.Split(target, root), rest...)
			continue
		}
		if !info.IsDir() {
			return "", fmt.Errorf("socket path: %s is not a directory", next)
		}
		at = next
	}
	return at, nil
}

// listenUnix creates the socket at path inside a directory only this user can enter. A socket this user left behind
// is removed only under the path's lock and when nothing answers on it.
func listenUnix(path string) (net.Listener, error) {
	// The directory above the socket's own is resolved and checked before anything is
	// created, and the socket is bound at the resolved path.
	given, name := filepath.Split(path)
	given = strings.TrimRight(given, string(filepath.Separator))
	if given == "" {
		given = "."
	}
	above, own := filepath.Split(given)
	if above == "" {
		above = "."
	}
	if own == "" || own == "." || own == ".." || name == "" {
		return nil, fmt.Errorf("socket path %s must name a socket inside its own directory", path)
	}
	parent, err := trustedDir(above)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(parent, own)
	path = filepath.Join(dir, name)
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 || !ownedByUs(info) {
		return nil, fmt.Errorf("socket directory %s must be a directory of this user with mode 0700, not a link", dir)
	}
	lock, err := lockPath(path)
	if err != nil {
		return nil, err
	}
	ln, err := bindUnix(path)
	if err != nil {
		lock.Close()
		return nil, err
	}
	return &unixListener{ln, lock}, nil
}

// bindUnix is called with the path's lock held.
func bindUnix(path string) (*net.UnixListener, error) {
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if info.Mode().Type() != fs.ModeSocket || !ownedByUs(info) {
			return nil, fmt.Errorf("refusing to start: %s exists and is not a socket of this user", path)
		}
		// A listener that did not take the lock may still be serving it.
		c, err := net.Dial("unix", path)
		if err == nil {
			c.Close()
			return nil, fmt.Errorf("refusing to start: %s is in use", path)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("refusing to replace %s: %w", path, err)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

func run() error {
	addr := flag.String("listen", "127.0.0.1:8788", "TCP listen address; non-loopback requires -tls-cert and -tls-key")
	unix := flag.String("unix", "", "listen on this unix socket path instead of TCP")
	dir := flag.String("data", ".dev/server-data", "journal directory")
	parts := flag.Int("partitions", 1, "partitions per dynamic topic; fixed for directory lifetime")
	cert := flag.String("tls-cert", "", "TLS certificate file (PEM)")
	key := flag.String("tls-key", "", "TLS private key file (PEM)")
	journalCap := flag.Int64("journal-cap", 0, "maximum journal bytes (zero is unlimited)")
	readTasks := flag.Int("read-tasks", 0, "maximum server-wide read and position tasks (zero selects the default)")
	segmented := flag.Bool("segmented", false, "create a segmented store in a new directory")
	flag.Parse()
	if *journalCap < 0 || *readTasks < 0 {
		return errors.New("limits must be non-negative")
	}
	if (*cert == "") != (*key == "") {
		return errors.New("set both -tls-cert and -tls-key, or neither")
	}
	cfg := wireserver.Config{MaxReadTasks: *readTasks, Token: os.Getenv("DOSTOEVSKY_TOKEN")}
	if len(cfg.Token) < 16 {
		return errors.New("set DOSTOEVSKY_TOKEN to at least 16 bytes; make one with: dostoevsky-gateway -gen-token")
	}
	if *cert != "" {
		pair, err := tls.LoadX509KeyPair(*cert, *key)
		if err != nil {
			return err
		}
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	}
	open := d.Open
	if *segmented {
		open = d.OpenSegmented
	}
	node, err := open(*dir, d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"*": *parts}})
	if err != nil {
		return err
	}
	defer node.Close()
	if err = node.SetJournalCap(*journalCap); err != nil {
		return err
	}
	srv, err := wireserver.New(node, cfg)
	if err != nil {
		return err
	}
	defer srv.Close() // runs before node.Close
	var ln net.Listener
	if *unix != "" {
		ln, err = listenUnix(*unix)
	} else {
		ln, err = net.Listen("tcp", *addr)
	}
	if err != nil {
		return err
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	done := make(chan error, 1)
	// Serve checks the address actually bound and refuses plaintext off this machine.
	go func() { done <- srv.Serve(ln) }()
	fmt.Printf("listening %s (tls=%t)\n", ln.Addr(), cfg.TLS != nil)
	select {
	case err = <-done:
		return err
	case <-stop:
		return nil
	}
}
