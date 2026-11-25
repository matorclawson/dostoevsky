package main

import (
	"strings"
	"testing"
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
	good := `{"developer_mode": false, "data_directory": {"path": "/data/trial-1/data"}}`
	if err := checkNodeConfig([]byte(good), "/data/trial-1"); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"developer":   `{"developer_mode": true, "data_directory": "/data/trial-1/data"}`,
		"absent":      `{"data_directory": "/data/trial-1/data"}`,
		"string":      `{"developer_mode": "false", "data_directory": "/data/trial-1/data"}`,
		"other dir":   `{"developer_mode": false, "data_directory": "/data/trial-2/data"}`,
		"not json":    `nope`,
		"empty":       `{}`,
		"null":        `null`,
		"wrong shape": `[1]`,
	} {
		if err := checkNodeConfig([]byte(body), "/data/trial-1"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestClusterConfigReadbackIsStrict(t *testing.T) {
	if err := checkClusterConfig([]byte(`{"write_caching_default": false, "x": 1}`)); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"on":     `{"write_caching_default": true}`,
		"absent": `{"x": 1}`,
		"string": `{"write_caching_default": "false"}`,
		"bad":    `{`,
	} {
		if err := checkClusterConfig([]byte(body)); err == nil {
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
