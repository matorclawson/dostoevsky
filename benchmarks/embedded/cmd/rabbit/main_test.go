package main

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

func TestNextDeliveryNeverWaitsPastItsBounds(t *testing.T) {
	hung := make(chan amqp.Delivery) // a broker that never answers
	start := time.Now()
	if _, err := nextDelivery(context.Background(), hung, 50*time.Millisecond); err == nil || !strings.Contains(err.Error(), "no delivery within") {
		t.Fatalf("hung channel: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the bounded wait took far longer than its bound")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := nextDelivery(ctx, hung, time.Hour); done <- err }()
	cancel() // the memory guard trips
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "verification stopped") {
			t.Fatalf("cancelled wait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled context did not end the wait; a goroutine would be left blocked")
	}

	closed := make(chan amqp.Delivery)
	close(closed)
	if _, err := nextDelivery(context.Background(), closed, time.Second); err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("closed channel: %v", err)
	}

	ready := make(chan amqp.Delivery, 1)
	ready <- amqp.Delivery{Body: []byte("x")}
	if m, err := nextDelivery(context.Background(), ready, time.Second); err != nil || string(m.Body) != "x" {
		t.Fatalf("delivery: %v %v", m, err)
	}
}
