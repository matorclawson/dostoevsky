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

// listenUnix creates the socket at path inside a directory only this user can enter. The caller holds the lock of
// its data directory, which is what allows removing a socket this user left behind.
func listenUnix(path string) (net.Listener, error) {
	dir := filepath.Dir(path)
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
	info, err = os.Lstat(path)
	switch {
	case err == nil:
		if info.Mode().Type() != fs.ModeSocket || !ownedByUs(info) {
			return nil, fmt.Errorf("refusing to start: %s exists and is not a socket of this user", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	ln, err := net.Listen("unix", path)
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
	flag.Parse()
	if (*cert == "") != (*key == "") {
		return errors.New("set both -tls-cert and -tls-key, or neither")
	}
	cfg := wireserver.Config{Token: os.Getenv("DOSTOEVSKY_TOKEN")}
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
	// Open first: it takes the data directory's lock.
	node, err := d.Open(*dir, d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"*": *parts}})
	if err != nil {
		return err
	}
	defer node.Close()
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
