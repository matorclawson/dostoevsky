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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers"
)

func main() {
	// Usage goes to stdout when it was asked for (-h, -help), to stderr when it comes with an error.
	usage := io.Writer(os.Stderr)
	for _, a := range os.Args[1:] {
		if a == "-h" || a == "-help" || a == "--help" {
			usage = os.Stdout
		}
	}
	engine, bin, err := parseArgs(os.Args[1:], os.Getenv, usage)
	if err != nil {
		if code := exitCodeForArgs(err); code != 0 {
			fmt.Fprintln(os.Stderr, "FAILED:", err)
			os.Exit(code)
		}
		os.Exit(0) // -help is not a failure
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT) // before any start
	defer stop()
	if err := serve(ctx, engine, bin, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "FAILED:", err)
		os.Exit(1)
	}
}

// parseArgs reads the command line and resolves the server executable in the documented order:
// -bin, else BENCH_<ENGINE>_SERVER, else BENCH_SERVER_BIN. The flag's own default is EMPTY: a
// default taken from BENCH_SERVER_BIN would beat the engine-specific variable.
func parseArgs(args []string, getenv func(string) string, usage io.Writer) (engine, bin string, err error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(usage)
	e := fs.String("engine", "", "gateway, redis, nats or kafka")
	b := fs.String("bin", "", "server executable; default $BENCH_<ENGINE>_SERVER (BENCH_REDIS_SERVER, BENCH_NATS_SERVER, BENCH_GATEWAY_SERVER), else $BENCH_SERVER_BIN; not used by kafka")
	if err = fs.Parse(args); err != nil {
		return "", "", err
	}
	return *e, serverBin(*e, *b, getenv), nil
}

// exitCodeForArgs: asking for help (flag.ErrHelp) is a success, any other argument error is a usage error.
func exitCodeForArgs(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	return 2
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
	start := time.Now()
	s, err := servers.Start(engine, servers.Options{Ctx: ctx, Bin: bin, Home: os.Getenv("BENCH_KAFKA_HOME"), Java: os.Getenv("BENCH_JAVA_HOME"), Dir: dir})
	if err != nil {
		return err
	}
	readyMS := float64(time.Since(start)) / 1e6
	s.Proc.Sample()
	bootPeak, bootErr := s.Proc.PeakKiB()
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
	if bootErr != nil {
		return fmt.Errorf("boot memory: %w", bootErr)
	}
	if merr != nil {
		return fmt.Errorf("server memory: %w", merr)
	}
	// The verdict is printed only after the server is gone AND its files are removed.
	if err = cleanup(); err != nil {
		return fmt.Errorf("removing the trial directory: %w", err)
	}
	if _, err = fmt.Fprintf(out, "STOPPED server_rss_KiB=%d rss_source=%s fresh_ready_ms=%.3f boot_peak_KiB=%d verdict=clean\n", kib, s.Proc.Source(), readyMS, bootPeak); err != nil {
		return fmt.Errorf("writing the STOPPED line: %w", err)
	}
	return nil
}
