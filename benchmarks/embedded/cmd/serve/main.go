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
//	STOPPED server_rss_KiB=N rss_source=VmHWM|ps-sampled verdict=clean
//
// The token is private to this trial: a driver must never copy the READY line into a
// receipt or a log. Memory is the server tree's peak resident size, and rss_source says how it
// was measured: VmHWM (Linux, the kernel's peak, as in the Go comparison) or ps-sampled (macOS,
// the largest `ps -o rss=` sample; never compare it with VmHWM). On any failure serve prints
// "FAILED: ..." to standard error and exits non-zero.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers"
)

func main() {
	engine := flag.String("engine", "", "gateway, redis, nats or kafka")
	bin := flag.String("bin", os.Getenv("BENCH_SERVER_BIN"), "server executable (default $BENCH_SERVER_BIN)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT) // before any start
	defer stop()
	if err := serve(ctx, *engine, serverBin(*engine, *bin, os.Getenv), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "FAILED:", err)
		os.Exit(1)
	}
}

// serverBin picks the server executable: the flag, else BENCH_<ENGINE>_SERVER, else BENCH_SERVER_BIN.
func serverBin(engine, flagValue string, getenv func(string) string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := getenv("BENCH_" + strings.ToUpper(engine) + "_SERVER"); v != "" {
		return v
	}
	return getenv("BENCH_SERVER_BIN")
}

// serve runs one server until stdin closes or ctx is cancelled. ctx comes from main, which
// installs the signal handling BEFORE anything is started, so a SIGTERM that arrives while the
// server is still starting stops it instead of orphaning it (it has its own process group).
func serve(ctx context.Context, engine, bin string, stdin io.Reader, out io.Writer) (err error) {
	dir, err := os.MkdirTemp("", "embedded-serve-")
	if err != nil {
		return err
	}
	cleanup := func() error {
		if dir == "" {
			return nil
		}
		d := dir
		dir = ""
		return os.RemoveAll(d)
	}
	defer func() {
		if rerr := cleanup(); rerr != nil && err == nil {
			err = rerr
		}
	}()
	s, err := servers.Start(engine, servers.Options{Ctx: ctx, Bin: bin, Dir: dir})
	if err != nil {
		return err
	}
	stopped := false
	defer func() {
		if !stopped {
			s.Stop()
		}
	}()
	token := s.Token
	if token == "" {
		token = "none" // kafka has no token
	}
	if _, err = fmt.Fprintf(out, "READY addr=%s pid=%d token=%s\n", s.Addr, s.Proc.Pid(), token); err != nil {
		return fmt.Errorf("writing the READY line: %w", err)
	}

	closed := make(chan struct{})
	go func() { io.Copy(io.Discard, stdin); close(closed) }()
	select {
	case <-closed:
	case <-ctx.Done():
	}

	s.Proc.Sample() // a last reading before the server is signaled
	kib, merr := s.Proc.PeakKiB()
	stopped = true
	if err = s.Stop(); err != nil {
		return fmt.Errorf("the server did not stop cleanly: %w", err)
	}
	if merr != nil {
		return fmt.Errorf("server memory: %w", merr)
	}
	// The verdict is printed only after the server is gone AND its files are removed.
	if err = cleanup(); err != nil {
		return fmt.Errorf("removing the trial directory: %w", err)
	}
	if _, err = fmt.Fprintf(out, "STOPPED server_rss_KiB=%d rss_source=%s verdict=clean\n", kib, s.Proc.Source()); err != nil {
		return fmt.Errorf("writing the STOPPED line: %w", err)
	}
	return nil
}
