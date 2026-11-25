// Command kafka runs the publisher workload against a single-node Apache Kafka
// (KRaft) broker started from an unpacked release: acks=all, a replication
// factor of one and log.flush.interval.messages=1. The broker is a separate JVM
// process on loopback with ports chosen for this trial.
//
// Environment: BENCH_KAFKA_HOME (the unpacked kafka_2.13-<version> directory),
// BENCH_JAVA_HOME (the JDK), BENCH_ENGINE_LABEL (default "kafka").
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/servers"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The broker's configuration, client options, clean-shutdown check and topic handling live in
// internal/servers, shared with the language client benchmarks. These names stay for this
// command's own use and its tests.
func brokerConfig(dir string, port, controllerPort int) string {
	return servers.KafkaConfig(dir, port, controllerPort)
}
func clientOptions(addr string) []kgo.Opt { return servers.KafkaClientOptions(addr) }
func cleanShutdown(logDir string) error   { return servers.KafkaCleanShutdown(logDir) }
func closeClient(c *kgo.Client)           { servers.CloseKafkaClient(c) }

const topic = servers.KafkaTopic

// Every blocking call to the broker below runs under the trial context (cancelled
// when the memory guard trips) with its own deadline; where franz-go has no
// context form the call goes through run.Within. Cleanup always ends with
// proc.Stop, which kills the broker's process group even if a close hung.
//
//	exec kafka-storage.sh       exec.CommandContext, 120 s
//	WaitReady (TCP)             run.Within, timeout + 5 s
//	kgo.NewClient               called directly: it only builds the client and dials
//	                            lazily, so no wrapper goroutine writes shared state
//	Request (CreateTopics,
//	  Metadata)                 context with a 10 to 30 s deadline
//	ProduceSync                 context with a 60 s deadline (franz-go stops waiting when
//	                            it ends; Client.Close below aborts anything still buffered)
//	PollFetches                 context with the verification deadline
//	Client.Close                run.Within, 10 s
//	proc.Stop                   SIGTERM then SIGKILL after the grace period
type store struct {
	client *kgo.Client
	proc   *serverproc.Proc
	addr   string
	logDir string
	mu     sync.Mutex
	loaded map[uint64][]byte
	reader *kgo.Client
	parent context.Context
}

// BindContext receives the harness's context, cancelled when the memory guard
// trips and when the trial ends; verification polls stop when it is done.
func (s *store) BindContext(ctx context.Context) {
	s.mu.Lock()
	s.parent = ctx
	s.mu.Unlock()
}

func (s *store) Publish(data []byte) (uint64, error) {
	ctx, cancel := context.WithTimeout(run.Context(), 60*time.Second)
	defer cancel()
	res := s.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: data})
	if err := res.FirstErr(); err != nil {
		return 0, err
	}
	return uint64(res[0].Record.Offset), nil
}

// Read serves verification: the first call consumes the partition from the
// start, reading until the requested offset has arrived.
func (s *store) Read(offset uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.parent
	if parent == nil {
		parent = context.Background()
	}
	if s.reader == nil {
		// kgo.NewClient only builds the client (it dials lazily), so it is called
		// directly: no wrapper goroutine writes shared state after a timeout.
		r, err := kgo.NewClient(kgo.SeedBrokers(s.addr), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			return nil, err
		}
		s.reader, s.loaded = r, map[uint64][]byte{}
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if v, ok := s.loaded[offset]; ok {
			return v, nil
		}
		ctx, cancel := context.WithDeadline(parent, deadline)
		fetches := s.reader.PollFetches(ctx)
		cancel()
		if err := fetches.Err0(); err != nil {
			return nil, err
		}
		fetches.EachRecord(func(r *kgo.Record) { s.loaded[uint64(r.Offset)] = r.Value })
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("offset %d not found within the verification deadline", offset)
		}
	}
}

func (s *store) ServerPeakKiB() (uint64, error) { return s.proc.PeakKiB() }

func (s *store) Close() {
	closeClient(s.reader)
	closeClient(s.client)
	// kafka-server-start's JVM traps SIGTERM and exits 143 after a clean shutdown.
	// That is accepted only because we sent the SIGTERM and Kafka's own clean-shutdown
	// marker is present in the log directory.
	if err := s.proc.StopWith(60*time.Second, func() error { return cleanShutdown(s.logDir) }); err != nil {
		panic(err)
	}
}

func main() {
	label := os.Getenv("BENCH_ENGINE_LABEL")
	if label == "" {
		label = "kafka"
	}
	home, java := os.Getenv("BENCH_KAFKA_HOME"), os.Getenv("BENCH_JAVA_HOME")
	// The cluster ID and both ports belong to the trial: they are chosen when the
	// store is first opened and reused by the reopen, so the reopened broker listens
	// where the trial's clients expect it and no other listener can stand in.
	var clusterID string
	var port, controller int
	run.Main(label, func(dir string, fresh bool) (run.Store, error) {
		if home == "" || java == "" {
			return nil, fmt.Errorf("BENCH_KAFKA_HOME and BENCH_JAVA_HOME must be set")
		}
		var err error
		if fresh || port == 0 {
			if port, err = serverproc.FreePort(); err != nil {
				return nil, err
			}
			if controller, err = serverproc.FreePort(); err != nil {
				return nil, err
			}
		}
		k, err := servers.StartKafka(servers.KafkaOptions{Home: home, Java: java, Dir: dir, Fresh: fresh, ClusterID: clusterID, Port: port, ControllerPort: controller})
		if err != nil {
			return nil, err
		}
		clusterID = k.ClusterID
		return &store{client: k.Client, proc: k.Proc, addr: k.Addr, logDir: k.LogDir}, nil
	})
}
