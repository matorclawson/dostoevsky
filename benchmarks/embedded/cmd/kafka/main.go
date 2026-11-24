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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const topic = "events"

// brokerConfig is one trial's server.properties. The keys come from the Kafka
// 4.4 KRaft documentation (https://kafka.apache.org/44/operations/kraft/); the
// durability keys are listed in benchmarks/comparison/ENGINES.md.
func brokerConfig(dir string, port, controllerPort int) string {
	return strings.Join([]string{
		"process.roles=broker,controller",
		"node.id=1",
		fmt.Sprintf("controller.quorum.bootstrap.servers=127.0.0.1:%d", controllerPort),
		fmt.Sprintf("listeners=PLAINTEXT://127.0.0.1:%d,CONTROLLER://127.0.0.1:%d", port, controllerPort),
		fmt.Sprintf("advertised.listeners=PLAINTEXT://127.0.0.1:%d", port),
		"controller.listener.names=CONTROLLER",
		"listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"inter.broker.listener.name=PLAINTEXT",
		"log.dirs=" + filepath.Join(dir, "kafka-logs"),
		"num.partitions=1",
		"default.replication.factor=1",
		"min.insync.replicas=1",
		"offsets.topic.replication.factor=1",
		"transaction.state.log.replication.factor=1",
		"transaction.state.log.min.isr=1",
		"group.initial.rebalance.delay.ms=0",
		"log.flush.interval.messages=1",
		"auto.create.topics.enable=false",
		"",
	}, "\n")
}

// clientOptions: acks=all, no idempotent producer and no record retries, so an
// ambiguous produce fails the trial instead of being sent again. Topics are never
// auto-created: the harness creates the topic before the timed span.
func clientOptions(addr string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordRetries(0),
	}
}

type store struct {
	client *kgo.Client
	proc   *serverproc.Proc
	addr   string
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
	res := s.client.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: data})
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
	if s.reader == nil {
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
		parent := s.parent
		if parent == nil {
			parent = context.Background()
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
	if s.reader != nil {
		s.reader.Close()
	}
	s.client.Close()
	if err := s.proc.Stop(60 * time.Second); err != nil {
		panic(err)
	}
}

// ensureTopic creates the topic (when fresh) with one partition and a replication
// factor of one, then waits until its leader is ready. It runs before the timed
// publish span so topic creation can never land inside a measured publish.
func ensureTopic(client *kgo.Client, fresh bool, deadline time.Time) error {
	if fresh {
		req := kmsg.NewPtrCreateTopicsRequest()
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = topic, 1, 1
		req.Topics = append(req.Topics, t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := client.Request(ctx, req)
		cancel()
		if err != nil {
			return fmt.Errorf("create topic: %w", err)
		}
		for _, rt := range resp.(*kmsg.CreateTopicsResponse).Topics {
			if rt.ErrorCode != 0 {
				return fmt.Errorf("create topic %s: error code %d", rt.Topic, rt.ErrorCode)
			}
		}
	}
	for {
		md := kmsg.NewPtrMetadataRequest()
		mt := kmsg.NewMetadataRequestTopic()
		name := topic
		mt.Topic = &name
		md.Topics = append(md.Topics, mt)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.Request(ctx, md)
		cancel()
		if err == nil {
			for _, rt := range resp.(*kmsg.MetadataResponse).Topics {
				if rt.ErrorCode == 0 && len(rt.Partitions) == 1 && rt.Partitions[0].ErrorCode == 0 && rt.Partitions[0].Leader >= 0 {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s has no ready leader: %v", topic, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func script(home, name string) string { return filepath.Join(home, "bin", name) }

func main() {
	label := os.Getenv("BENCH_ENGINE_LABEL")
	if label == "" {
		label = "kafka"
	}
	home, java := os.Getenv("BENCH_KAFKA_HOME"), os.Getenv("BENCH_JAVA_HOME")
	var clusterID string
	run.Main(label, func(dir string, fresh bool) (run.Store, error) {
		if home == "" || java == "" {
			return nil, fmt.Errorf("BENCH_KAFKA_HOME and BENCH_JAVA_HOME must be set")
		}
		env := []string{"JAVA_HOME=" + java, "KAFKA_HEAP_OPTS=-Xms1g -Xmx1g", "LOG_DIR=" + filepath.Join(dir, "logs")}
		port, err := serverproc.FreePort()
		if err != nil {
			return nil, err
		}
		controller, err := serverproc.FreePort()
		if err != nil {
			return nil, err
		}
		conf := filepath.Join(dir, "server.properties")
		if err = os.WriteFile(conf, []byte(brokerConfig(dir, port, controller)), 0o600); err != nil {
			return nil, err
		}
		storage := func(args ...string) (string, error) {
			cmd := exec.Command(script(home, "kafka-storage.sh"), args...)
			cmd.Env = append(os.Environ(), env...)
			out, err := cmd.CombinedOutput()
			return strings.TrimSpace(string(out)), err
		}
		if fresh {
			if clusterID, err = storage("random-uuid"); err != nil || clusterID == "" {
				return nil, fmt.Errorf("random-uuid: %v %s", err, clusterID)
			}
			if out, err := storage("format", "--standalone", "-t", clusterID, "-c", conf); err != nil {
				return nil, fmt.Errorf("format: %v: %s", err, out)
			}
		}
		proc, err := serverproc.Start(script(home, "kafka-server-start.sh"), []string{conf}, env, filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		addr := "127.0.0.1:" + strconv.Itoa(port)
		fail := func(err error) (run.Store, error) {
			proc.Stop(30 * time.Second)
			return nil, err
		}
		if err = proc.WaitReady("tcp", addr, 180*time.Second); err != nil {
			return fail(err)
		}
		client, err := kgo.NewClient(clientOptions(addr)...)
		if err != nil {
			return fail(err)
		}
		// Readiness is tied to our broker: it must report the cluster ID that this
		// trial formatted, and it must answer before the deadline.
		deadline := time.Now().Add(180 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, e := client.Request(ctx, kmsg.NewPtrMetadataRequest())
			cancel()
			if e == nil {
				if md, ok := resp.(*kmsg.MetadataResponse); ok && md.ClusterID != nil && *md.ClusterID == clusterID {
					break
				}
				if md, ok := resp.(*kmsg.MetadataResponse); ok && md.ClusterID != nil {
					client.Close()
					return fail(fmt.Errorf("broker reports cluster %q, not this trial's %q", *md.ClusterID, clusterID))
				}
			}
			if time.Now().After(deadline) {
				client.Close()
				return fail(fmt.Errorf("broker did not become ready: %v", e))
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err = ensureTopic(client, fresh, time.Now().Add(120*time.Second)); err != nil {
			client.Close()
			return fail(err)
		}
		return &store{client: client, proc: proc, addr: addr}, nil
	})
}
