// Command redis runs the publisher workload against Redis or Valkey (XADD with
// the append-only file synced on every write) as a separate server process
// reached over a unix socket in the trial's private directory. The server binary comes from BENCH_SERVER_BIN and the label on the
// receipt line from BENCH_ENGINE_LABEL (default "redis").
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers"
)

const stream = "events"

type store struct {
	client *redis.Client
	proc   *serverproc.Proc
	// ctx is nil outside tests: commands then run under the harness's run context.
	ctx func() context.Context
}

func (s store) context() context.Context {
	if s.ctx != nil {
		return s.ctx()
	}
	return run.Context()
}

// A stream ID is "<milliseconds>-<sequence>". Pack it into the harness's uint64
// offset so a fresh client can read the record back after a restart.
func pack(id string) (uint64, error) {
	ms, seq, ok := strings.Cut(id, "-")
	if !ok {
		return 0, fmt.Errorf("bad stream id %q", id)
	}
	m, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return 0, err
	}
	q, err := strconv.ParseUint(seq, 10, 64)
	if err != nil || q > 0xffff {
		return 0, fmt.Errorf("bad stream id %q", id)
	}
	return m<<16 | q, nil
}

func unpack(v uint64) string { return fmt.Sprintf("%d-%d", v>>16, v&0xffff) }

func (s store) Publish(data []byte) (uint64, error) {
	id, err := s.client.XAdd(s.context(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"d": data}}).Result()
	if err != nil {
		return 0, err
	}
	return pack(id)
}

func (s store) Read(v uint64) ([]byte, error) {
	id := unpack(v)
	// go-redis does not interrupt a read already on the socket when its context is cancelled, so the wait itself is bound
	// to the run context. Verification only: the timed publish path has no extra goroutine.
	ctx := s.context()
	var msgs []redis.XMessage
	err := run.Within(ctx, 60*time.Second, func() (e error) {
		msgs, e = s.client.XRange(ctx, stream, id, id).Result()
		return e
	})
	if err != nil {
		return nil, err
	}
	if len(msgs) != 1 {
		return nil, fmt.Errorf("stream id %s: %d entries", id, len(msgs))
	}
	d, ok := msgs[0].Values["d"].(string)
	if !ok {
		return nil, fmt.Errorf("stream id %s: no payload", id)
	}
	return []byte(d), nil
}

func (s store) ServerPeakKiB() (uint64, error) { return s.proc.PeakKiB() }

func (s store) Close() {
	s.client.Close()
	if err := s.proc.Stop(30 * time.Second); err != nil {
		panic(err)
	}
}

// serverConfig is private to one trial: no TCP port, and a unix socket inside the
// trial's own directory, so no other process can be the one that answers.
func serverConfig(dir string) (conf, sock string) {
	sock = filepath.Join(dir, "server.sock")
	conf = fmt.Sprintf("port 0\nunixsocket %s\nunixsocketperm 700\ndir %s\n", sock, dir) + servers.RedisDurability
	return conf, sock
}

// clientOptions disables command retries. go-redis retries failed commands by
// default; a retried XADD would append a second entry under a new ID, and the
// harness only verifies the IDs it was told about. ContextTimeoutEnabled lets a
// context deadline bound each command, so the readiness deadline reaches the socket.
func clientOptions(sock string) *redis.Options {
	return &redis.Options{Network: "unix", Addr: sock, PoolSize: 64, MaxRetries: -1, ContextTimeoutEnabled: true}
}

// loadingFlag reads loading from INFO persistence.
func loadingFlag(info string) (bool, error) {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "loading:"); ok {
			switch v {
			case "0":
				return false, nil
			case "1":
				return true, nil
			}
			return false, fmt.Errorf("INFO persistence: unexpected loading value %q", v)
		}
	}
	return false, fmt.Errorf("INFO persistence: no loading field")
}

// isLoadingErr reports a LOADING reply, which means "not ready yet" during readiness.
func isLoadingErr(err error) bool { return err != nil && strings.HasPrefix(err.Error(), "LOADING") }

// waitLoaded polls INFO persistence until the dataset has finished loading. A
// LOADING error counts as not ready; any other error, the trial context ending or
// the bound passing is a failure.
func waitLoaded(ctx context.Context, info func(context.Context, string) (string, error), every, bound time.Duration) error {
	// One deadline for the whole wait: each poll gets only the budget that is left,
	// and a loading:0 that arrives after the deadline does not count as ready.
	deadline := time.Now().Add(bound)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return fmt.Errorf("the dataset was still loading after %v", bound)
		}
		c, cancel := context.WithTimeout(ctx, min(5*time.Second, left))
		text, err := info(c, "persistence")
		cancel()
		switch {
		case err == nil:
			loading, perr := loadingFlag(text)
			if perr != nil {
				return perr
			}
			if !loading {
				if time.Now().After(deadline) {
					return fmt.Errorf("loading:0 arrived after the %v readiness deadline", bound)
				}
				return nil
			}
		case isLoadingErr(err):
			// still loading
		default:
			return fmt.Errorf("INFO persistence: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("readiness stopped: %w", ctx.Err())
		case <-time.After(min(every, max(time.Until(deadline), 0))):
		}
	}
}

// serverPid reads process_id from INFO server.
func serverPid(info string) (int, error) {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "process_id:"); ok {
			return strconv.Atoi(v)
		}
	}
	return 0, fmt.Errorf("process_id not found in INFO server")
}

func main() {
	label := os.Getenv("BENCH_ENGINE_LABEL")
	if label == "" {
		label = "redis"
	}
	bin := os.Getenv("BENCH_SERVER_BIN")
	run.Main(label, func(dir string, _ bool) (run.Store, error) {
		if bin == "" {
			return nil, fmt.Errorf("BENCH_SERVER_BIN is not set")
		}
		confText, sock := serverConfig(dir)
		conf := filepath.Join(dir, "server.conf")
		if err := os.WriteFile(conf, []byte(confText), 0o600); err != nil {
			return nil, err
		}
		proc, err := serverproc.Start(bin, []string{conf}, nil, filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if err = proc.WaitReady("unix", sock, 60*time.Second); err != nil {
			proc.Stop(5 * time.Second)
			return nil, err
		}
		client := redis.NewClient(clientOptions(sock))
		fail := func(err error) (run.Store, error) {
			client.Close()
			proc.Stop(5 * time.Second)
			return nil, err
		}
		ctx := run.Context()
		info := func(c context.Context, section string) (string, error) { return client.Info(c, section).Result() }
		// The socket lives in this trial's private directory, but readiness is also
		// tied to the launched child: the answering process must be our own. INFO is
		// answered while the dataset is still loading, so this comes first.
		srv, err := info(ctx, "server")
		if err == nil {
			var pid int
			if pid, err = serverPid(srv); err == nil && pid != proc.Pid() {
				err = fmt.Errorf("process %d answered, not the launched server %d", pid, proc.Pid())
			}
		}
		if err != nil {
			return fail(err)
		}
		// After a restart the server replays its append-only file; until INFO reports
		// loading:0 every data command is answered LOADING. Waiting here, under the trial
		// context and a bound, is readiness. A LOADING reply anywhere else (publish,
		// verification) is still a failure.
		if err = waitLoaded(ctx, info, 50*time.Millisecond, 180*time.Second); err != nil {
			return fail(err)
		}
		// The first successful data command ends the timed restart: reopen_ms is the
		// restart until loading:0 plus this read ("restart-to-ready including AOF replay").
		if err = client.Ping(ctx).Err(); err != nil {
			return fail(err)
		}
		if err = client.XLen(ctx, stream).Err(); err != nil {
			return fail(err)
		}
		return store{client: client, proc: proc}, nil
	})
}
