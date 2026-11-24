package main

import (
	"strings"
	"testing"
)

func TestNodeIsPrivateToTheTrial(t *testing.T) {
	env := strings.Join(serverEnv("/data/trial-1", "bench42@localhost", 35672, 35673, 34369), "\n")
	for _, want := range []string{
		"RABBITMQ_NODENAME=bench42@localhost",
		"RABBITMQ_NODE_IP_ADDRESS=127.0.0.1",
		"RABBITMQ_NODE_PORT=35672",
		"RABBITMQ_DIST_PORT=35673",
		"ERL_EPMD_PORT=34369",
		"ERL_EPMD_ADDRESS=127.0.0.1",
		"RABBITMQ_MNESIA_BASE=/data/trial-1/mnesia",
		"HOME=/data/trial-1/home",
	} {
		if !strings.Contains(env, want) {
			t.Errorf("environment lacks %q:\n%s", want, env)
		}
	}
	if strings.Contains(env, "=5672\n") || strings.Contains(env, "=4369") {
		t.Error("a default shared port is used")
	}
}

func TestMemoryIsBounded(t *testing.T) {
	if !strings.Contains(config, "vm_memory_high_watermark.absolute = 1GiB") {
		t.Fatal("the 1 GiB watermark is missing from the broker config")
	}
}
