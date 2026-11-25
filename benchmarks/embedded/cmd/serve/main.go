// Command serve starts one engine server for a benchmark trial and keeps it running until
// its standard input closes or it receives SIGTERM or SIGINT. It is the one place the
// language client benchmarks get their servers from, so they run exactly the code the Go
// comparison runs (internal/servers).
//
//	serve -engine gateway|redis|nats -bin /path/to/server
//
// Standard output carries two lines for the driver and nothing else:
//
//	READY addr=127.0.0.1:PORT pid=N token=SECRET
//	STOPPED server_rss_KiB=N verdict=clean
//
// The token is private to this trial. Memory is the server tree's sampled VmHWM
// (Linux; the same sampler as the Go comparison). On any failure serve prints
// "FAILED: ..." to standard error and exits non-zero.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers"
)

func main() {
	engine := flag.String("engine", "", "gateway, redis or nats")
	bin := flag.String("bin", os.Getenv("BENCH_SERVER_BIN"), "server executable (default $BENCH_SERVER_BIN)")
	flag.Parse()
	if err := serve(*engine, *bin, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "FAILED:", err)
		os.Exit(1)
	}
}

func serve(engine, bin string, stdin io.Reader, out io.Writer) (err error) {
	dir, err := os.MkdirTemp("", "embedded-serve-")
	if err != nil {
		return err
	}
	defer func() {
		if rerr := os.RemoveAll(dir); rerr != nil && err == nil {
			err = rerr
		}
	}()
	s, err := servers.Start(engine, servers.Options{Bin: bin, Dir: dir})
	if err != nil {
		return err
	}
	stopped := false
	defer func() {
		if !stopped {
			s.Stop()
		}
	}()
	fmt.Fprintf(out, "READY addr=%s pid=%d token=%s\n", s.Addr, s.Proc.Pid(), s.Token)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)
	closed := make(chan struct{})
	go func() { io.Copy(io.Discard, stdin); close(closed) }()
	select {
	case <-closed:
	case <-sig:
	}

	kib, merr := s.Proc.PeakKiB() // read before the server goes away
	stopped = true
	if err = s.Stop(); err != nil {
		return fmt.Errorf("the server did not stop cleanly: %w", err)
	}
	if merr != nil {
		return fmt.Errorf("server memory: %w", merr)
	}
	fmt.Fprintf(out, "STOPPED server_rss_KiB=%d verdict=clean\n", kib)
	return nil
}
