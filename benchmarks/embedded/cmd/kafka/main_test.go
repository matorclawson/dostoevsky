package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBrokerConfigIsPrivateAndDurable(t *testing.T) {
	conf := brokerConfig("/data/trial-1", 39092, 39093)
	for _, want := range []string{
		"listeners=PLAINTEXT://127.0.0.1:39092,CONTROLLER://127.0.0.1:39093\n",
		"advertised.listeners=PLAINTEXT://127.0.0.1:39092\n",
		"controller.quorum.bootstrap.servers=127.0.0.1:39093\n",
		"log.dirs=/data/trial-1/kafka-logs\n",
		"log.flush.interval.messages=1\n",
		"default.replication.factor=1\n",
		"min.insync.replicas=1\n",
		"process.roles=broker,controller\n",
		"auto.create.topics.enable=false\n",
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("server.properties lacks %q", want)
		}
	}
	if strings.Contains(conf, "0.0.0.0") || strings.Contains(conf, ":9092") {
		t.Error("a fixed or wildcard listener is configured")
	}
}

func TestClientDoesNotRetryOrDeduplicate(t *testing.T) {
	// franz-go's options are opaque; at least check we pass the set the ruling needs.
	if n := len(clientOptions("127.0.0.1:1")); n != 4 {
		t.Fatalf("%d client options, want seeds, acks=all, no idempotence, no retries (and no auto topic creation)", n)
	}
}

func TestCleanShutdownNeedsKafkasOwnMarker(t *testing.T) {
	dir := t.TempDir()
	if err := cleanShutdown(dir); err == nil || !strings.Contains(err.Error(), ".kafka_cleanshutdown") {
		t.Fatalf("a log directory without the marker was accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".kafka_cleanshutdown"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cleanShutdown(dir); err != nil {
		t.Fatal(err)
	}
}
