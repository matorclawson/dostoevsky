// Command dostoevsky-gateway runs a single embedded node with the public client API.
//
// The bearer token grants access to the whole store, so the gateway listens on
// loopback by default and refuses any other address unless TLS is configured.
// Create a token with: dostoevsky-gateway -gen-token
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/clienthttp"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// newToken returns 32 random bytes from crypto/rand as unpadded base64url (43 characters).
func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// checkListen refuses a non-loopback listen address unless TLS is on. A host name other
// than "localhost" cannot be shown to be loopback, so it counts as non-loopback.
func checkListen(addr string, useTLS bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address %q: %w", addr, err)
	}
	loopback := host == "localhost"
	if ip := net.ParseIP(host); ip != nil {
		loopback = ip.IsLoopback()
	}
	if !loopback && !useTLS {
		return fmt.Errorf("refusing to serve the whole-store token in plaintext on %q: use a loopback address or set -tls-cert and -tls-key", addr)
	}
	return nil
}

// listen binds addr and then checks the address the operating system actually bound,
// not only the flag: without TLS it must be loopback, or the listener is closed.
func listen(addr string, useTLS bool) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if err := checkBound(ln.Addr(), useTLS); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

func checkBound(a net.Addr, useTLS bool) error {
	tcp, ok := a.(*net.TCPAddr)
	if useTLS {
		return nil
	}
	if !ok || !tcp.IP.IsLoopback() {
		return fmt.Errorf("refusing to serve the whole-store token in plaintext: the listener is bound to %v, which is not loopback", a)
	}
	return nil
}

func run() error {
	addr := flag.String("listen", "127.0.0.1:8787", "HTTP listen address; non-loopback requires -tls-cert and -tls-key")
	dir := flag.String("data", ".dev/client-data", "journal directory")
	parts := flag.Int("partitions", 1, "partitions per dynamic topic; fixed for directory lifetime")
	cert := flag.String("tls-cert", "", "TLS certificate file (PEM)")
	key := flag.String("tls-key", "", "TLS private key file (PEM)")
	capMiB := flag.Int64("max-journal-mib", 0, "cap the journal at this size; publishes are refused when it is reached (0: no cap)")
	gen := flag.Bool("gen-token", false, "print a new random token (32 bytes, base64url) and exit")
	flag.Parse()
	if *gen {
		t, err := newToken()
		if err != nil {
			return err
		}
		fmt.Println(t)
		return nil
	}
	if (*cert == "") != (*key == "") {
		return fmt.Errorf("set both -tls-cert and -tls-key, or neither")
	}
	useTLS := *cert != ""
	if err := checkListen(*addr, useTLS); err != nil {
		return err
	}
	token := os.Getenv("DOSTOEVSKY_TOKEN")
	if len(token) < 16 {
		return fmt.Errorf("set DOSTOEVSKY_TOKEN to at least 16 bytes; make one with -gen-token")
	}
	// Checked before Open, which may create or recover the journal.
	if *capMiB < 0 || *capMiB > 1<<40 {
		return fmt.Errorf("-max-journal-mib must be between 0 and %d", int64(1)<<40)
	}
	node, err := d.Open(*dir, d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"*": *parts}})
	if err != nil {
		return err
	}
	defer node.Close()
	if cut := node.Truncated(); cut.Bytes > 0 {
		log.Printf("journal: removed an incomplete final frame, %d bytes at offset %d", cut.Bytes, cut.At)
	}
	if err := node.SetJournalCap(*capMiB << 20); err != nil {
		return err
	}
	h, err := clienthttp.New(node, token)
	if err != nil {
		return err
	}
	ln, err := listen(*addr, useTLS)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 40 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan error, 1)
	if useTLS {
		pair, err := tls.LoadX509KeyPair(*cert, *key)
		if err != nil {
			return err
		}
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
		go func() { done <- srv.ServeTLS(ln, "", "") }()
	} else {
		go func() { done <- srv.Serve(ln) }()
	}
	fmt.Printf("listening %s (tls=%t)\n", ln.Addr(), useTLS)
	select {
	case err = <-done:
	case <-ctx.Done():
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = srv.Shutdown(drain)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
