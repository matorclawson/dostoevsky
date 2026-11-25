package servers

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaTopic is the one topic every Kafka trial uses.
const KafkaTopic = "events"

// The code below moved here from benchmarks/embedded/cmd/kafka without change in behavior, so
// the Go comparison and the language client benchmarks start the broker with the same code.

// KafkaConfig is one trial's server.properties. The keys come from the Kafka
// 4.4 KRaft documentation (https://kafka.apache.org/44/operations/kraft/); the
// durability keys are listed in benchmarks/comparison/ENGINES.md.
func KafkaConfig(dir string, port, controllerPort int) string {
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

// KafkaClientOptions: acks=all, no idempotent producer and no record retries, so an
// ambiguous produce fails the trial instead of being sent again. Topics are never
// auto-created: the harness creates the topic before the timed span.
func KafkaClientOptions(addr string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordRetries(0),
	}
}

// CloseKafkaClient relies on franz-go's Client.Close, which stops the client and fails
// every record still buffered (it does not flush them), instead of calling
// AbortBufferedRecords first: a trial that reaches cleanup has either finished or
// failed, and nothing buffered may be waited for. The broker is killed right after.
func CloseKafkaClient(c *kgo.Client) {
	if c != nil {
		// A close that hangs must not hold cleanup open; the broker is killed next.
		run.Within(context.Background(), 10*time.Second, func() error { c.Close(); return nil })
	}
}

// EnsureKafkaTopic creates the topic (when fresh) with one partition and a replication
// factor of one, then waits until its leader is ready. It runs before the timed
// publish span so topic creation can never land inside a measured publish.
func EnsureKafkaTopic(client *kgo.Client, fresh bool, deadline time.Time) error {
	if fresh {
		req := kmsg.NewPtrCreateTopicsRequest()
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = KafkaTopic, 1, 1
		req.Topics = append(req.Topics, t)
		ctx, cancel := context.WithTimeout(run.Context(), 30*time.Second)
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
		name := KafkaTopic
		mt.Topic = &name
		md.Topics = append(md.Topics, mt)
		ctx, cancel := context.WithTimeout(run.Context(), 10*time.Second)
		resp, err := client.Request(ctx, md)
		cancel()
		if err == nil {
			for _, rt := range resp.(*kmsg.MetadataResponse).Topics {
				if rt.ErrorCode == 0 && len(rt.Partitions) == 1 && rt.Partitions[0].ErrorCode == 0 && rt.Partitions[0].Leader >= 0 {
					return nil
				}
			}
		}
		if err := run.Context().Err(); err != nil {
			return fmt.Errorf("topic readiness stopped: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s has no ready leader: %v", KafkaTopic, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// KafkaCleanShutdown confirms from Kafka's own state that the broker shut down cleanly:
// its log manager writes .kafka_cleanshutdown into each log directory when it does.
func KafkaCleanShutdown(logDir string) error {
	if _, err := os.Stat(filepath.Join(logDir, ".kafka_cleanshutdown")); err != nil {
		var names []string
		if entries, e := os.ReadDir(logDir); e == nil {
			for _, en := range entries {
				names = append(names, en.Name())
			}
		}
		return fmt.Errorf("no .kafka_cleanshutdown marker in %s (it holds %v): %w", logDir, names, err)
	}
	return nil
}

func script(home, name string) string { return filepath.Join(home, "bin", name) }

// KafkaOptions describe one broker start. Ports and the cluster ID belong to the trial: a
// reopen passes the same ones back so the broker listens where the trial's clients expect it.
type KafkaOptions struct {
	Home, Java     string // the unpacked kafka_2.13-<version> directory and the JDK
	Dir            string // the trial's private directory
	Fresh          bool   // format a new log directory and create the topic
	ClusterID      string // used when not Fresh
	Port           int
	ControllerPort int
}

// KafkaBroker is a started, ready broker and a client connected to it.
type KafkaBroker struct {
	Proc      *serverproc.Proc
	Addr      string
	ClusterID string
	LogDir    string
	Client    *kgo.Client // built with KafkaClientOptions; the caller closes it
}

// Stop ends the broker. Kafka's JVM traps SIGTERM and exits 143 after a clean shutdown; that is
// accepted only because we sent the SIGTERM and Kafka's own clean-shutdown marker is present.
func (k *KafkaBroker) Stop() error {
	return k.Proc.StopWith(60*time.Second, func() error { return KafkaCleanShutdown(k.LogDir) })
}

// StartKafka formats (when fresh), starts the broker, waits until it reports this trial's
// cluster ID, and makes sure the topic has a ready leader, all under run.Context().
func StartKafka(o KafkaOptions) (*KafkaBroker, error) {
	if o.Home == "" || o.Java == "" {
		return nil, fmt.Errorf("BENCH_KAFKA_HOME and BENCH_JAVA_HOME must be set")
	}
	home, java, dir, fresh, clusterID, port, controller := o.Home, o.Java, o.Dir, o.Fresh, o.ClusterID, o.Port, o.ControllerPort
	env := []string{"JAVA_HOME=" + java, "KAFKA_HEAP_OPTS=-Xms1g -Xmx1g", "LOG_DIR=" + filepath.Join(dir, "logs")}
	var err error
	conf := filepath.Join(dir, "server.properties")
	if err = os.WriteFile(conf, []byte(KafkaConfig(dir, port, controller)), 0o600); err != nil {
		return nil, err
	}
	storage := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(run.Context(), 120*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, script(home, "kafka-storage.sh"), args...)
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
	fail := func(err error) (*KafkaBroker, error) {
		proc.Stop(30 * time.Second)
		return nil, err
	}
	const ready = 180 * time.Second
	if err = run.Within(run.Context(), ready+5*time.Second, func() error { return proc.WaitReady("tcp", addr, ready) }); err != nil {
		return fail(err)
	}
	client, err := kgo.NewClient(KafkaClientOptions(addr)...) // builds the client; it dials lazily
	if err != nil {
		return fail(err)
	}
	// Readiness is tied to our broker: it must report the cluster ID that this
	// trial formatted, and it must answer before the deadline.
	deadline := time.Now().Add(ready)
	for {
		ctx, cancel := context.WithTimeout(run.Context(), 5*time.Second)
		resp, e := client.Request(ctx, kmsg.NewPtrMetadataRequest())
		cancel()
		if e == nil {
			if md, ok := resp.(*kmsg.MetadataResponse); ok && md.ClusterID != nil {
				if *md.ClusterID == clusterID {
					break
				}
				CloseKafkaClient(client)
				return fail(fmt.Errorf("broker reports cluster %q, not this trial's %q", *md.ClusterID, clusterID))
			}
		}
		if cerr := run.Context().Err(); cerr != nil {
			CloseKafkaClient(client)
			return fail(fmt.Errorf("broker readiness stopped: %w", cerr))
		}
		if time.Now().After(deadline) {
			CloseKafkaClient(client)
			return fail(fmt.Errorf("broker did not become ready: %v", e))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err = EnsureKafkaTopic(client, fresh, time.Now().Add(120*time.Second)); err != nil {
		CloseKafkaClient(client)
		return fail(err)
	}
	proc.Sample()
	return &KafkaBroker{Proc: proc, Addr: addr, ClusterID: clusterID, LogDir: filepath.Join(dir, "kafka-logs"), Client: client}, nil
}
