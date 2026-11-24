// Command redis runs the publisher workload against Redis or Valkey (XADD with
// the append-only file synced on every write) as a separate server process on
// loopback. The server binary comes from BENCH_SERVER_BIN and the label on the
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
)

const (
	port   = "6390"
	stream = "events"
)

type store struct {
	client *redis.Client
	proc   *serverproc.Proc
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
	id, err := s.client.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"d": data}}).Result()
	if err != nil {
		return 0, err
	}
	return pack(id)
}

func (s store) Read(v uint64) ([]byte, error) {
	id := unpack(v)
	msgs, err := s.client.XRange(context.Background(), stream, id, id).Result()
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

func (s store) ServerPeakKiB() uint64 { return s.proc.PeakKiB() }

func (s store) Close() {
	s.client.Close()
	if err := s.proc.Stop(30 * time.Second); err != nil {
		panic(err)
	}
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
		conf := filepath.Join(dir, "server.conf")
		cfg := fmt.Sprintf("port %s\nbind 127.0.0.1\ndir %s\nappendonly yes\nappendfsync always\nsave \"\"\nprotected-mode yes\n", port, dir)
		if err := os.WriteFile(conf, []byte(cfg), 0o600); err != nil {
			return nil, err
		}
		proc, err := serverproc.Start(bin, []string{conf}, nil, filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		addr := "127.0.0.1:" + port
		if err = proc.WaitReady(addr, 60*time.Second); err != nil {
			proc.Stop(5 * time.Second)
			return nil, err
		}
		client := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
		if err = client.Ping(context.Background()).Err(); err != nil {
			client.Close()
			proc.Stop(5 * time.Second)
			return nil, err
		}
		return store{client, proc}, nil
	})
}
