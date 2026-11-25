package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBrokerConfigIsPrivateAndNotDeveloperMode(t *testing.T) {
	conf := brokerConfig("/data/trial-1", 31001, 31002, 31003)
	for _, want := range []string{
		"developer_mode: false\n",
		"data_directory: \"/data/trial-1/data\"\n",
		"node_id: 0\n",
		"seed_servers: []\n",
		"      port: 31001\n",
		"    port: 31002\n",
		"      port: 31003\n",
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("redpanda.yaml lacks %q:\n%s", want, conf)
		}
	}
	if strings.Contains(conf, "0.0.0.0") || strings.Contains(conf, "9092") || strings.Contains(conf, "9644") || strings.Contains(conf, "33145") {
		t.Error("a wildcard or default shared address or port is configured")
	}
	if strings.Contains(conf, "developer_mode: true") || strings.Contains(conf, "write_caching") {
		t.Error("the config must not enable developer mode or touch write caching")
	}
}

func TestLaunchFlagsAndLoaders(t *testing.T) {
	bin, args, env, err := launch("/opt/bench/rp", "", "1G", false, "/d/redpanda.yaml")
	if err != nil || bin != "/opt/bench/rp/libexec/redpanda" || len(env) != 1 || env[0] != "LD_LIBRARY_PATH=/opt/bench/rp/lib" {
		t.Fatalf("default launch: %q %v %v %v", bin, args, env, err)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"--redpanda-cfg /d/redpanda.yaml", "--smp 1", "--memory 1G", "--lock-memory=false"} {
		if !strings.Contains(joined, want) {
			t.Errorf("args lack %q: %s", want, joined)
		}
	}
	for _, bad := range []string{"--overprovisioned", "dev-container", "unsafe", "--mode"} {
		if strings.Contains(joined, bad) {
			t.Errorf("args contain %q: %s", bad, joined)
		}
	}
	bin, args, env, err = launch("/opt/bench/rp", "ld", "2G", true, "/d/redpanda.yaml")
	if err != nil || bin != "/opt/bench/rp/lib/ld.so" || len(env) != 0 || args[0] != "--library-path" || args[2] != "/opt/bench/rp/libexec/redpanda" {
		t.Fatalf("ld launch: %q %v %v %v", bin, args, env, err)
	}
	if !strings.Contains(strings.Join(args, " "), "--overprovisioned") || !strings.Contains(strings.Join(args, " "), "--memory 2G") {
		t.Errorf("flags not applied: %v", args)
	}
	for _, c := range []struct{ home, loader, mem string }{{"", "", "1G"}, {"/h", "x", "1G"}, {"/h", "", "1g"}, {"/h", "", "0G"}, {"/h", "", "1G --mode dev-container"}} {
		if _, _, _, err := launch(c.home, c.loader, c.mem, false, "c"); err == nil {
			t.Errorf("accepted %+v", c)
		}
	}
}

func TestNodeConfigReadbackIsStrict(t *testing.T) {
	for name, good := range map[string]string{
		"object": `{"developer_mode": false, "data_directory": {"path": "/data/trial-1/data"}}`,
		"string": `{"developer_mode": false, "data_directory": "/data/trial-1/data"}`,
		"clean":  `{"developer_mode": false, "data_directory": "/data/trial-1/data/"}`,
	} {
		if err := checkNodeConfig([]byte(good), "/data/trial-1"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for name, body := range map[string]string{
		"developer":            `{"developer_mode": true, "data_directory": "/data/trial-1/data"}`,
		"absent developer":     `{"data_directory": "/data/trial-1/data"}`,
		"string developer":     `{"developer_mode": "false", "data_directory": "/data/trial-1/data"}`,
		"other trial":          `{"developer_mode": false, "data_directory": "/data/trial-2/data"}`,
		"shared prefix":        `{"developer_mode": false, "data_directory": "/data/trial-1/data2"}`,
		"shared prefix longer": `{"developer_mode": false, "data_directory": "/data/trial-1/data-old"}`,
		"parent only":          `{"developer_mode": false, "data_directory": "/data/trial-1"}`,
		"path elsewhere":       `{"developer_mode": false, "wal_directory": "/data/trial-1/data"}`,
		"path in another key":  `{"developer_mode": false, "data_directory": "", "x": "/data/trial-1/data"}`,
		"object without path":  `{"developer_mode": false, "data_directory": {"other": "/data/trial-1/data"}}`,
		"wrong type":           `{"developer_mode": false, "data_directory": 7}`,
		"not json":             `nope`,
		"empty":                `{}`,
		"null":                 `null`,
		"wrong shape":          `[1]`,
	} {
		if err := checkNodeConfig([]byte(body), "/data/trial-1"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func topicConfigs(name, cfg string, value *string, code int16) *kmsg.DescribeConfigsResponse {
	r := kmsg.NewPtrDescribeConfigsResponse()
	res := kmsg.NewDescribeConfigsResponseResource()
	res.ResourceType, res.ResourceName, res.ErrorCode = kmsg.ConfigResourceTypeTopic, name, code
	if cfg != "" {
		c := kmsg.NewDescribeConfigsResponseResourceConfig()
		c.Name, c.Value = cfg, value
		res.Configs = append(res.Configs, c)
	}
	r.Resources = append(r.Resources, res)
	return r
}

func TestTopicWriteCachingReadbackIsStrict(t *testing.T) {
	f, tr := "false", "true"
	if err := checkTopicWriteCaching(topicConfigs("events", "write.caching", &f, 0), "events"); err != nil {
		t.Fatal(err)
	}
	broker := topicConfigs("events", "write.caching", &f, 0)
	broker.Resources[0].ResourceType = kmsg.ConfigResourceTypeBroker
	topicOK := topicConfigs("events", "write.caching", &f, 0)
	topicOK.Resources[0].ResourceType = kmsg.ConfigResourceTypeTopic
	if err := checkTopicWriteCaching(topicOK, "events"); err != nil {
		t.Fatal(err)
	}
	for name, resp := range map[string]*kmsg.DescribeConfigsResponse{
		"broker resource of the same name": broker,
		"topic override on":                topicConfigs("events", "write.caching", &tr, 0),
		"no value":                         topicConfigs("events", "write.caching", nil, 0),
		"not reported":                     topicConfigs("events", "flush.ms", &f, 0),
		"no configs":                       topicConfigs("events", "", nil, 0),
		"broker error":                     topicConfigs("events", "write.caching", &f, 3),
		"another topic only":               topicConfigs("other", "write.caching", &f, 0),
		"no resources":                     kmsg.NewPtrDescribeConfigsResponse(),
	} {
		if err := checkTopicWriteCaching(resp, "events"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestEvidenceIsKeptPerTrialAndNeverOverwritten(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("BENCH_EVIDENCE_DIR", dir)
	t.Setenv("BENCH_TRIAL_KEY", "trial1-redpanda-w8")
	if err := saveEvidence("open1-fresh", "node_config.json", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := saveEvidence("open2-reopen", "node_config.json", []byte(`{"a":2}`)); err != nil {
		t.Fatal(err)
	}
	for open, want := range map[string]string{"open1-fresh": `{"a":1}`, "open2-reopen": `{"a":2}`} {
		got, err := os.ReadFile(filepath.Join(dir, "trial1-redpanda-w8."+open+".node_config.json"))
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q %v", open, got, err)
		}
	}
	// Evidence is required: without a directory the trial cannot run, and a key is never reused.
	if err := saveEvidence("open1-fresh", "node_config.json", []byte(`{"a":3}`)); err == nil {
		t.Fatal("a write-once evidence file was overwritten")
	}
	t.Setenv("BENCH_EVIDENCE_DIR", "")
	if err := saveEvidence("x", "y", nil); err == nil {
		t.Fatal("evidence was dropped silently when BENCH_EVIDENCE_DIR was unset")
	}
}

func TestRawFieldShowsWhatTheBrokerSent(t *testing.T) {
	if got := rawField([]byte(`{"write_caching_default":"disabled"}`), "write_caching_default"); got != `"disabled"` {
		t.Fatalf("%s", got)
	}
	if got := rawField([]byte(`{"write_caching_default":false}`), "write_caching_default"); got != "false" {
		t.Fatalf("%s", got)
	}
	if rawField([]byte(`{}`), "write_caching_default") != "absent" || rawField([]byte(`x`), "k") != "unreadable" {
		t.Fatal("absent or unreadable fields must say so")
	}
}

func metaTopic(name *string, code int16, partitions ...kmsg.MetadataResponseTopicPartition) kmsg.MetadataResponseTopic {
	t := kmsg.NewMetadataResponseTopic()
	t.Topic, t.ErrorCode, t.Partitions = name, code, partitions
	return t
}

func metaPartition(number int32, leader int32, replicas ...int32) kmsg.MetadataResponseTopicPartition {
	p := kmsg.NewMetadataResponseTopicPartition()
	p.Partition, p.Leader, p.Replicas = number, leader, replicas
	return p
}

func TestTopicReadinessNeedsTheNamedTopicOnePartitionOneReplicaAndALeader(t *testing.T) {
	events, other := "events", "other"
	if !topicReady([]kmsg.MetadataResponseTopic{metaTopic(&events, 0, metaPartition(0, 0, 0))}, "events") {
		t.Fatal("a ready topic was refused")
	}
	for name, topics := range map[string][]kmsg.MetadataResponseTopic{
		"another topic":    {metaTopic(&other, 0, metaPartition(0, 0, 0))},
		"unnamed":          {metaTopic(nil, 0, metaPartition(0, 0, 0))},
		"topic error":      {metaTopic(&events, 3, metaPartition(0, 0, 0))},
		"two partitions":   {metaTopic(&events, 0, metaPartition(0, 0, 0), metaPartition(1, 0, 0))},
		"no partition":     {metaTopic(&events, 0)},
		"partition 1 only": {metaTopic(&events, 0, metaPartition(1, 0, 0))},
		"two replicas":     {metaTopic(&events, 0, metaPartition(0, 0, 0, 1))},
		"no replica":       {metaTopic(&events, 0, metaPartition(0, 0))},
		"no leader":        {metaTopic(&events, 0, metaPartition(0, -1, 0))},
		"none":             nil,
	} {
		if topicReady(topics, "events") {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestClusterConfigReadbackIsStrict(t *testing.T) {
	for name, body := range map[string]string{
		"bool false":      `{"write_caching_default": false, "x": 1}`,
		"string false":    `{"write_caching_default": "false"}`,
		"string disabled": `{"write_caching_default": "disabled"}`,
	} {
		if err := checkClusterConfig([]byte(body)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, body := range map[string]string{
		"bool true":   `{"write_caching_default": true}`,
		"string true": `{"write_caching_default": "true"}`,
		"unknown":     `{"write_caching_default": "maybe"}`,
		"empty":       `{"write_caching_default": ""}`,
		"number":      `{"write_caching_default": 0}`,
		"null":        `{"write_caching_default": null}`,
		"absent":      `{"x": 1}`,
		"bad":         `{`,
	} {
		if err := checkClusterConfig([]byte(body)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestListenerIsTiedToTheTrialBroker(t *testing.T) {
	id := func(s string) *string { return &s }
	for name, kafka := range map[string]*string{"bare": id("abc-123"), "prefixed": id("redpanda.abc-123")} {
		if err := checkClusterIdentity([]byte(`{"cluster_uuid": "abc-123"}`), kafka); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := checkClusterIdentity([]byte(`{"uuid": "abc-123"}`), id("abc-123")); err != nil {
		t.Errorf("uuid key: %v", err)
	}
	for name, c := range map[string]struct {
		admin string
		kafka *string
	}{
		"stray broker":     {`{"cluster_uuid": "abc-123"}`, id("redpanda.other")},
		"suffix only":      {`{"cluster_uuid": "abc-123"}`, id("xabc-123")},
		"no kafka id":      {`{"cluster_uuid": "abc-123"}`, nil},
		"empty kafka id":   {`{"cluster_uuid": "abc-123"}`, id("")},
		"no admin uuid":    {`{}`, id("abc-123")},
		"admin not a map":  {`[1]`, id("abc-123")},
		"empty admin uuid": {`{"cluster_uuid": ""}`, id("")},
	} {
		if err := checkClusterIdentity([]byte(c.admin), c.kafka); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	b := func(node int32, host string, port int32) kmsg.MetadataResponseBroker {
		x := kmsg.NewMetadataResponseBroker()
		x.NodeID, x.Host, x.Port = node, host, port
		return x
	}
	if err := checkBrokers([]kmsg.MetadataResponseBroker{b(0, "127.0.0.1", 31001)}, "127.0.0.1", 31001); err != nil {
		t.Fatal(err)
	}
	for name, bs := range map[string][]kmsg.MetadataResponseBroker{
		"other port": {b(0, "127.0.0.1", 9092)},
		"other host": {b(0, "10.0.0.1", 31001)},
		"other node": {b(1, "127.0.0.1", 31001)},
		"none":       nil,
	} {
		if err := checkBrokers(bs, "127.0.0.1", 31001); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestReadyStatus(t *testing.T) {
	if ok, err := checkReady([]byte(`{"status":"ready"}`)); !ok || err != nil {
		t.Fatalf("ready: %v %v", ok, err)
	}
	if ok, err := checkReady([]byte(`{"status":"booting"}`)); ok || err != nil {
		t.Fatalf("booting: %v %v", ok, err)
	}
	if _, err := checkReady([]byte(`x`)); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestClientDoesNotRetryOrDeduplicate(t *testing.T) {
	if n := len(clientOptions("127.0.0.1:1")); n != 4 {
		t.Fatalf("%d client options, want seeds, acks=all, no idempotence, no retries", n)
	}
}
