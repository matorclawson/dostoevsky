// Command rabbit runs the publisher workload against a single-node RabbitMQ
// quorum queue with publisher confirms. RabbitMQ is a separate Erlang VM on
// loopback with ports, node name and data directory private to the trial.
//
// RabbitMQ's documentation does not state that the quorum queue log is synced
// before a publisher confirm, so this engine's result is labeled "durability per
// vendor default, not verified" wherever it is shown.
//
// Environment: BENCH_RABBIT_HOME (the unpacked rabbitmq_server-<version>
// directory), BENCH_ENGINE_LABEL (default "rabbitmq"). erl must be on PATH.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
)

const queue = "events"

// serverEnv holds everything that makes the node private to one trial.
func serverEnv(dir, node string, amqpPort, distPort, epmdPort int) []string {
	return []string{
		"HOME=" + filepath.Join(dir, "home"),
		"RABBITMQ_NODENAME=" + node,
		"RABBITMQ_NODE_IP_ADDRESS=127.0.0.1",
		"RABBITMQ_NODE_PORT=" + strconv.Itoa(amqpPort),
		"RABBITMQ_DIST_PORT=" + strconv.Itoa(distPort),
		"ERL_EPMD_PORT=" + strconv.Itoa(epmdPort),
		"RABBITMQ_MNESIA_BASE=" + filepath.Join(dir, "mnesia"),
		"RABBITMQ_LOG_BASE=" + filepath.Join(dir, "logs"),
		"RABBITMQ_ENABLED_PLUGINS_FILE=" + filepath.Join(dir, "enabled_plugins"),
		"RABBITMQ_CONFIG_FILES=" + filepath.Join(dir, "conf.d"),
	}
}

// config bounds memory the same way for every run (the 1 GiB watermark is stated on the chart).
const config = "vm_memory_high_watermark.absolute = 1GiB\nloopback_users = none\n"

type store struct {
	conn  *amqp.Connection
	pool  chan *amqp.Channel
	proc  *serverproc.Proc
	mu    sync.Mutex
	drain map[uint64][]byte
}

// Publish sends one persistent message on its own confirm channel and waits for
// the broker's confirm. The harness's record index is the first eight bytes of
// the payload and doubles as the offset: a queue has none.
func (s *store) Publish(data []byte) (uint64, error) {
	ch := <-s.pool
	defer func() { s.pool <- ch }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: data})
	if err != nil {
		return 0, err
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("broker nacked the message")
	}
	return binary.BigEndian.Uint64(data), nil
}

// Read serves verification: the first call drains the queue (every acknowledged
// message must still be there after the restart) and indexes it by record index.
func (s *store) Read(index uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drain == nil {
		ch := <-s.pool
		defer func() { s.pool <- ch }()
		s.drain = map[uint64][]byte{}
		for {
			msg, ok, err := ch.Get(queue, true)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			if len(msg.Body) >= 8 {
				s.drain[binary.BigEndian.Uint64(msg.Body)] = msg.Body
			}
		}
	}
	v, ok := s.drain[index]
	if !ok {
		return nil, fmt.Errorf("record %d is not in the queue after the restart", index)
	}
	return v, nil
}

func (s *store) ServerPeakKiB() (uint64, error) { return s.proc.PeakKiB() }

func (s *store) Close() {
	s.conn.Close()
	if err := s.proc.Stop(60 * time.Second); err != nil {
		panic(err)
	}
}

func main() {
	label := os.Getenv("BENCH_ENGINE_LABEL")
	if label == "" {
		label = "rabbitmq"
	}
	home := os.Getenv("BENCH_RABBIT_HOME")
	run.Main(label, func(dir string, fresh bool) (run.Store, error) {
		if home == "" {
			return nil, fmt.Errorf("BENCH_RABBIT_HOME is not set")
		}
		var ports [3]int
		for i := range ports {
			p, err := serverproc.FreePort()
			if err != nil {
				return nil, err
			}
			ports[i] = p
		}
		node := fmt.Sprintf("bench%d@localhost", os.Getpid())
		for _, d := range []string{"home", "conf.d", "mnesia", "logs"} {
			if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
				return nil, err
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "enabled_plugins"), []byte("[].\n"), 0o600); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, "conf.d", "rabbitmq.conf"), []byte(config), 0o600); err != nil {
			return nil, err
		}
		proc, err := serverproc.Start(filepath.Join(home, "sbin", "rabbitmq-server"), nil, serverEnv(dir, node, ports[0], ports[1], ports[2]), filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		fail := func(err error) (run.Store, error) {
			proc.Stop(60 * time.Second)
			return nil, err
		}
		addr := "127.0.0.1:" + strconv.Itoa(ports[0])
		if err = proc.WaitReady("tcp", addr, 180*time.Second); err != nil {
			return fail(err)
		}
		var conn *amqp.Connection
		deadline := time.Now().Add(180 * time.Second)
		for {
			if conn, err = amqp.Dial("amqp://guest:guest@" + addr + "/"); err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fail(fmt.Errorf("broker did not accept an AMQP connection: %v", err))
			}
			time.Sleep(200 * time.Millisecond)
		}
		// Tie readiness to the launched node: the cluster name carries its private name.
		if name, _ := conn.Properties["cluster_name"].(string); name != node {
			conn.Close()
			return fail(fmt.Errorf("broker reports node %q, not this trial's %q", name, node))
		}
		const channels = 64
		pool := make(chan *amqp.Channel, channels)
		for i := 0; i < channels; i++ {
			ch, err := conn.Channel()
			if err != nil {
				conn.Close()
				return fail(err)
			}
			if err = ch.Confirm(false); err != nil {
				conn.Close()
				return fail(err)
			}
			pool <- ch
		}
		if fresh {
			ch := <-pool
			_, err = ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
			pool <- ch
			if err != nil {
				conn.Close()
				return fail(err)
			}
		}
		return &store{conn: conn, pool: pool, proc: proc}, nil
	})
}
