// Command dostoevsky-gateway runs a single embedded node with the public client API.
package main

import (
	"context"
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
func run() error {
	addr := flag.String("listen", "127.0.0.1:8787", "HTTP listen address")
	dir := flag.String("data", ".dev/client-data", "journal directory")
	parts := flag.Int("partitions", 1, "partitions per dynamic topic; fixed for directory lifetime")
	flag.Parse()
	token := os.Getenv("DOSTOEVSKY_TOKEN")
	if len(token) < 16 {
		return fmt.Errorf("set DOSTOEVSKY_TOKEN to at least 16 bytes")
	}
	node, err := d.Open(*dir, d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"*": *parts}})
	if err != nil {
		return err
	}
	defer node.Close()
	h, err := clienthttp.New(node, token)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 40 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	fmt.Printf("listening %s\n", ln.Addr())
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
