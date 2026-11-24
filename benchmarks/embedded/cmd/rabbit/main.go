// Command rabbit runs the publisher workload against a single-node RabbitMQ
// quorum queue with publisher confirms. RabbitMQ is a separate Erlang VM on
// loopback with ports, node name, EPMD and data directory private to the trial.
//
// RabbitMQ's documentation does not state that the quorum queue log is synced
// before a publisher confirm, so this engine's result is labeled "durability per
// vendor default, not verified" wherever it is shown. Verification is a
// sequential fetch from the queue after the restart.
//
// Environment: BENCH_RABBIT_HOME (the unpacked rabbitmq_server-<version>
// directory), BENCH_EPMD (the epmd binary, default "epmd" from PATH),
// BENCH_ENGINE_LABEL (default "rabbitmq"). erl must be on PATH.
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
		"ERL_EPMD_ADDRESS=127.0.0.1",
		"RABBITMQ_MNESIA_BASE=" + filepath.Join(dir, "mnesia"),
		"RABBITMQ_LOG_BASE=" + filepath.Join(dir, "logs"),
		"RABBITMQ_ENABLED_PLUGINS_FILE=" + filepath.Join(dir, "enabled_plugins"),
		"RABBITMQ_CONFIG_FILES=" + filepath.Join(dir, "conf.d"),
	}
}

// config bounds memory the same way for every run (the 1 GiB watermark is stated on the chart).
const config = "vm_memory_high_watermark.absolute = 1GiB\nloopback_users = none\n"

// publisher is one confirm channel and the channel on which the broker returns
// messages it could not route.
type publisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

type store struct {
	conn         *amqp.Connection
	pool         chan publisher
	proc         *serverproc.Proc // the broker
	epmd         *serverproc.Proc // the node's EPMD, started by the harness
	mu           sync.Mutex
	parent       context.Context
	reader       *amqp.Channel
	deliveries   <-chan amqp.Delivery
	stopConsumer context.CancelFunc
	fetched      map[uint64][]byte
}

// Publish sends one persistent, mandatory message on its own confirm channel and
// waits for the broker's confirm. A message the broker returns as unroutable is a
// failure even though a confirm may follow. The harness's record index is the
// first eight bytes of the payload and doubles as the offset: a queue has none.
func (s *store) Publish(data []byte) (uint64, error) {
	p := <-s.pool
	defer func() { s.pool <- p }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: data})
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
	select {
	case r := <-p.returns:
		return 0, fmt.Errorf("broker returned the message as unroutable: %d %s", r.ReplyCode, r.ReplyText)
	default:
	}
	return binary.BigEndian.Uint64(data), nil
}

// BindContext receives the harness's context, cancelled when the memory guard
// trips and when the trial ends. Verification stops waiting when it is done.
func (s *store) BindContext(ctx context.Context) {
	s.mu.Lock()
	s.parent = ctx
	s.mu.Unlock()
}

// nextDelivery waits for the next delivery, but never longer than d and never past
// ctx: nothing is left blocked, because a delivery channel needs no goroutine.
func nextDelivery(ctx context.Context, deliveries <-chan amqp.Delivery, d time.Duration) (amqp.Delivery, error) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case m, ok := <-deliveries:
		if !ok {
			return amqp.Delivery{}, fmt.Errorf("the consumer was cancelled")
		}
		return m, nil
	case <-ctx.Done():
		return amqp.Delivery{}, fmt.Errorf("verification stopped: %w", ctx.Err())
	case <-timer.C:
		return amqp.Delivery{}, fmt.Errorf("no delivery within %v", d)
	}
}

// Read serves verification through one consumer for the whole verification, bound
// to the harness's context, so a memory-guard trip or a stalled broker ends the wait
// at once. It reads only until the requested record has arrived; every acknowledged
// message must still be in the queue after the restart. Each wait is bounded.
func (s *store) Read(index uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fetched == nil {
		s.fetched = map[uint64][]byte{}
	}
	parent := s.parent
	if parent == nil {
		parent = context.Background()
	}
	if s.deliveries == nil {
		p := <-s.pool
		s.reader = p.ch
		ctx, cancel := context.WithCancel(parent)
		d, err := p.ch.ConsumeWithContext(ctx, queue, "verify", true, false, false, false, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		s.deliveries, s.stopConsumer = d, cancel
	}
	for {
		if v, ok := s.fetched[index]; ok {
			return v, nil
		}
		msg, err := nextDelivery(parent, s.deliveries, 10*time.Second)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", index, err)
		}
		if len(msg.Body) >= 8 {
			s.fetched[binary.BigEndian.Uint64(msg.Body)] = msg.Body
		}
	}
}

// ServerPeakKiB adds the broker's tree and the node's EPMD, both sampled peaks.
func (s *store) ServerPeakKiB() (uint64, error) {
	b, err := s.proc.PeakKiB()
	if err != nil {
		return 0, err
	}
	e, err := s.epmd.PeakKiB()
	if err != nil {
		return 0, err
	}
	return b + e, nil
}

// Close shuts down in order: stop the consumer and close the channels, stop the
// broker, then the node's EPMD, and only then drop the connection (which the
// broker's exit has already ended).
func (s *store) Close() {
	s.mu.Lock()
	if s.stopConsumer != nil {
		s.stopConsumer()
	}
	s.mu.Unlock()
	if s.reader != nil {
		s.reader.Close()
	}
	for i := 0; i < cap(s.pool); i++ {
		select {
		case p := <-s.pool:
			p.ch.Close()
		default:
		}
	}
	err := s.proc.Stop(60 * time.Second)
	if e := s.epmd.Stop(10 * time.Second); e != nil && err == nil {
		err = e
	}
	s.conn.Close() // already ended by the broker's exit; the error is not a trial failure
	if err != nil {
		panic(err)
	}
}

func main() {
	label := os.Getenv("BENCH_ENGINE_LABEL")
	if label == "" {
		label = "rabbitmq"
	}
	home := os.Getenv("BENCH_RABBIT_HOME")
	epmdBin := os.Getenv("BENCH_EPMD")
	if epmdBin == "" {
		epmdBin = "epmd"
	}
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
		env := serverEnv(dir, node, ports[0], ports[1], ports[2])
		// The harness starts the node's EPMD itself, on this trial's private port, so
		// it is stopped with the trial and counted in the memory total. Otherwise
		// RabbitMQ would leave a detached daemon outside the broker's process group.
		epmd, err := serverproc.Start(epmdBin, []string{"-port", strconv.Itoa(ports[2])}, env, filepath.Join(dir, "epmd.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if err = epmd.WaitReady("tcp", "127.0.0.1:"+strconv.Itoa(ports[2]), 30*time.Second); err != nil {
			epmd.Stop(10 * time.Second)
			return nil, err
		}
		proc, err := serverproc.Start(filepath.Join(home, "sbin", "rabbitmq-server"), nil, env, filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			epmd.Stop(10 * time.Second)
			return nil, err
		}
		fail := func(err error) (run.Store, error) {
			proc.Stop(60 * time.Second)
			epmd.Stop(10 * time.Second)
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
		pool := make(chan publisher, channels)
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
			pool <- publisher{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 8))}
		}
		if fresh {
			p := <-pool
			_, err = p.ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
			pool <- p
			if err != nil {
				conn.Close()
				return fail(err)
			}
		}
		return &store{conn: conn, pool: pool, proc: proc, epmd: epmd}, nil
	})
}
