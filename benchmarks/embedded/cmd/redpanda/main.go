// Command redpanda runs the publisher workload against a single-node Redpanda
// broker unpacked from the vendor's signed apt package (dpkg-deb -x, no install,
// no root): acks=all, one partition, replication factor one, write caching off and
// developer mode off. The broker is a separate process on loopback with ports
// chosen for this trial.
//
// Durability, per the vendor's documentation
// (https://docs.redpanda.com/streaming/current/develop/config-topics/): with write
// caching disabled Redpanda fsyncs each acks=all batch to disk on a majority of
// replicas before it acknowledges the write. That is per batch, not per message,
// and the run cannot observe the fsync itself. Developer mode (and dev-container
// mode) bypasses fsync, so it is never used; before every trial the harness reads
// developer_mode and write_caching_default back from the admin API and fails the
// trial on any other value.
//
// Environment: BENCH_REDPANDA_HOME (the unpacked opt/redpanda directory, holding
// libexec/redpanda and lib/), BENCH_REDPANDA_LOADER (unset: run libexec/redpanda
// with LD_LIBRARY_PATH=lib; "ld": run lib/ld.so --library-path lib libexec/redpanda),
// BENCH_REDPANDA_MEMORY (Seastar --memory, default 1G), BENCH_REDPANDA_OVERPROVISIONED
// (set to 1 to add --overprovisioned), BENCH_ENGINE_LABEL (default "redpanda").
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/serverproc"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const topic = "events"

// brokerConfig is one trial's redpanda.yaml: loopback listeners on this trial's
// ports, this trial's data directory, a single node, and developer mode off.
// Cluster properties (write caching) stay at their defaults and are read back.
func brokerConfig(dir string, kafka, rpc, admin int) string {
	return strings.Join([]string{
		"redpanda:",
		"  data_directory: " + strconv.Quote(filepath.Join(dir, "data")),
		"  node_id: 0",
		"  developer_mode: false",
		"  seed_servers: []",
		"  rpc_server:",
		"    address: 127.0.0.1",
		fmt.Sprintf("    port: %d", rpc),
		"  advertised_rpc_api:",
		"    address: 127.0.0.1",
		fmt.Sprintf("    port: %d", rpc),
		"  kafka_api:",
		"    - address: 127.0.0.1",
		fmt.Sprintf("      port: %d", kafka),
		"  advertised_kafka_api:",
		"    - address: 127.0.0.1",
		fmt.Sprintf("      port: %d", kafka),
		"  admin:",
		"    - address: 127.0.0.1",
		fmt.Sprintf("      port: %d", admin),
		"",
	}, "\n")
}

var memoryFlag = regexp.MustCompile(`^[1-9][0-9]*[MG]$`)

// launch returns the executable, its arguments and the extra environment. Flags
// are fixed apart from --memory and the optional --overprovisioned; nothing that
// relaxes durability is ever added.
func launch(home, loader, memory string, overprovisioned bool, conf string) (bin string, args, env []string, err error) {
	if home == "" {
		return "", nil, nil, fmt.Errorf("BENCH_REDPANDA_HOME is not set")
	}
	if !memoryFlag.MatchString(memory) {
		return "", nil, nil, fmt.Errorf("BENCH_REDPANDA_MEMORY=%q must look like 1G or 1536M", memory)
	}
	flags := []string{"--redpanda-cfg", conf, "--smp", "1", "--memory", memory, "--lock-memory=false", "--default-log-level=info"}
	if overprovisioned {
		flags = append(flags, "--overprovisioned")
	}
	libs, exe := filepath.Join(home, "lib"), filepath.Join(home, "libexec", "redpanda")
	switch loader {
	case "":
		return exe, flags, []string{"LD_LIBRARY_PATH=" + libs}, nil
	case "ld":
		return filepath.Join(libs, "ld.so"), append([]string{"--library-path", libs, exe}, flags...), nil, nil
	}
	return "", nil, nil, fmt.Errorf("BENCH_REDPANDA_LOADER=%q must be empty or ld", loader)
}

// raiseNofile lifts the soft open-file limit to the hard limit (no privilege is
// needed); the broker inherits it.
func raiseNofile() error {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return err
	}
	if lim.Cur < lim.Max {
		lim.Cur = lim.Max
		return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim)
	}
	return nil
}

// checkNodeConfig requires the admin API's node configuration to show this trial's
// data directory (so the process that answers is the one we launched) and developer
// mode off. Anything missing is a failure: a value that cannot be read back is not
// assumed.
func checkNodeConfig(body []byte, dir string) error {
	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return fmt.Errorf("node_config: %w", err)
	}
	dev, ok := cfg["developer_mode"].(bool)
	if !ok {
		return fmt.Errorf("node_config has no boolean developer_mode")
	}
	if dev {
		return fmt.Errorf("developer_mode is true: fsync would be bypassed")
	}
	if !strings.Contains(string(body), filepath.Join(dir, "data")) {
		return fmt.Errorf("node_config does not show this trial's data directory %s", filepath.Join(dir, "data"))
	}
	return nil
}

// checkClusterConfig requires write_caching_default to be present and false.
func checkClusterConfig(body []byte) error {
	var cfg map[string]any
	if err := json.Unmarshal(body, &cfg); err != nil {
		return fmt.Errorf("cluster_config: %w", err)
	}
	wc, ok := cfg["write_caching_default"].(bool)
	if !ok {
		return fmt.Errorf("cluster_config has no boolean write_caching_default")
	}
	if wc {
		return fmt.Errorf("write_caching_default is true: acknowledgments would not wait for the disk")
	}
	return nil
}

func checkReady(body []byte) (bool, error) {
	var st struct{ Status string }
	if err := json.Unmarshal(body, &st); err != nil {
		return false, fmt.Errorf("status/ready: %w", err)
	}
	return st.Status == "ready", nil
}

func adminGet(ctx context.Context, admin, path string) ([]byte, error) {
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(c, http.MethodGet, "http://"+admin+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return body, nil
}

// clientOptions: acks=all, no idempotent producer and no record retries, as for Kafka.
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

func (s *store) Read(offset uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent := s.parent
	if parent == nil {
		parent = context.Background()
	}
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

func closeClient(c *kgo.Client) {
	if c != nil {
		run.Within(context.Background(), 10*time.Second, func() error { c.Close(); return nil })
	}
}

// Close stops the broker with SIGTERM and accepts only a clean exit: no exit-code
// exception is made, so if Redpanda reports anything else the trial fails and the
// receipt says so.
func (s *store) Close() {
	closeClient(s.reader)
	closeClient(s.client)
	if err := s.proc.Stop(60 * time.Second); err != nil {
		panic(err)
	}
}

// ensureTopic creates the topic (when fresh) with one partition and a replication
// factor of one, then waits for its leader, before the timed publish span.
func ensureTopic(client *kgo.Client, fresh bool, deadline time.Time) error {
	if fresh {
		req := kmsg.NewPtrCreateTopicsRequest()
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = topic, 1, 1
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
		name := topic
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
			return fmt.Errorf("topic %s has no ready leader: %v", topic, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func main() {
	label := os.Getenv("BENCH_ENGINE_LABEL")
	if label == "" {
		label = "redpanda"
	}
	memory := os.Getenv("BENCH_REDPANDA_MEMORY")
	if memory == "" {
		memory = "1G"
	}
	// The ports belong to the trial: chosen at the first open and reused by the
	// reopen, so the reopened broker listens where the trial's clients expect it.
	var kafka, rpc, admin int
	run.Main(label, func(dir string, fresh bool) (run.Store, error) {
		var err error
		if fresh || kafka == 0 {
			for _, p := range []*int{&kafka, &rpc, &admin} {
				if *p, err = serverproc.FreePort(); err != nil {
					return nil, err
				}
			}
		}
		conf := filepath.Join(dir, "redpanda.yaml")
		bin, args, env, err := launch(os.Getenv("BENCH_REDPANDA_HOME"), os.Getenv("BENCH_REDPANDA_LOADER"), memory, os.Getenv("BENCH_REDPANDA_OVERPROVISIONED") == "1", conf)
		if err != nil {
			return nil, err
		}
		if err = os.MkdirAll(filepath.Join(dir, "data"), 0o700); err != nil {
			return nil, err
		}
		if err = os.WriteFile(conf, []byte(brokerConfig(dir, kafka, rpc, admin)), 0o600); err != nil {
			return nil, err
		}
		if err = raiseNofile(); err != nil {
			return nil, fmt.Errorf("raise open-file limit: %w", err)
		}
		fmt.Fprintf(os.Stderr, "redpanda: %s %s\n", bin, strings.Join(args, " "))
		proc, err := serverproc.Start(bin, args, env, filepath.Join(dir, "server.log"), 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		kaddr, aaddr := "127.0.0.1:"+strconv.Itoa(kafka), "127.0.0.1:"+strconv.Itoa(admin)
		fail := func(err error) (run.Store, error) {
			proc.Stop(30 * time.Second)
			return nil, err
		}
		const ready = 180 * time.Second
		if err = run.Within(run.Context(), ready+5*time.Second, func() error { return proc.WaitReady("tcp", aaddr, ready) }); err != nil {
			return fail(err)
		}
		// Ready means the admin API reports it; the first successful topic metadata
		// below ends the timed restart (restart to ready).
		deadline := time.Now().Add(ready)
		for {
			body, e := adminGet(run.Context(), aaddr, "/v1/status/ready")
			if e == nil {
				ok, perr := checkReady(body)
				if perr != nil {
					return fail(perr)
				}
				if ok {
					break
				}
			}
			if cerr := run.Context().Err(); cerr != nil {
				return fail(fmt.Errorf("broker readiness stopped: %w", cerr))
			}
			if time.Now().After(deadline) {
				return fail(fmt.Errorf("broker did not become ready: %v", e))
			}
			time.Sleep(200 * time.Millisecond)
		}
		node, err := adminGet(run.Context(), aaddr, "/v1/node_config")
		if err == nil {
			err = checkNodeConfig(node, dir)
		}
		if err != nil {
			return fail(err)
		}
		cluster, err := adminGet(run.Context(), aaddr, "/v1/cluster_config?include_defaults=true")
		if err == nil {
			err = checkClusterConfig(cluster)
		}
		if err != nil {
			return fail(err)
		}
		if fresh {
			fmt.Fprintf(os.Stderr, "redpanda readback: developer_mode=false write_caching_default=false data_directory=%s\n", filepath.Join(dir, "data"))
		}
		client, err := kgo.NewClient(clientOptions(kaddr)...)
		if err != nil {
			return fail(err)
		}
		if err = ensureTopic(client, fresh, time.Now().Add(120*time.Second)); err != nil {
			closeClient(client)
			return fail(err)
		}
		return &store{client: client, proc: proc, addr: kaddr}, nil
	})
}
