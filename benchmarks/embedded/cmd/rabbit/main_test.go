package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNodeIsPrivateToTheTrial(t *testing.T) {
	env := strings.Join(serverEnv("/data/trial-1", "bench42@localhost", 35672, 35673, 34369), "\n")
	for _, want := range []string{
		"RABBITMQ_NODENAME=bench42@localhost",
		"RABBITMQ_USE_LONGNAME=false",
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

func TestBrokerConfigBoundsMemoryAndNamesTheCluster(t *testing.T) {
	conf := brokerConfig("bench42@localhost")
	for _, want := range []string{"vm_memory_high_watermark.absolute = 1GiB\n", "cluster_name = bench42@localhost\n"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("broker config lacks %q:\n%s", want, conf)
		}
	}
}

func TestIdentityCheckIsStrict(t *testing.T) {
	if err := checkIdentity(amqp.Table{"cluster_name": "bench42@localhost"}, "bench42@localhost"); err != nil {
		t.Fatal(err)
	}
	for _, props := range []amqp.Table{
		{"cluster_name": "bench42@zeo-internal-production-03"}, // the run 2 smoke finding
		{"cluster_name": "bench43@localhost"},
		{"cluster_name": 7},
		{},
		nil,
	} {
		if err := checkIdentity(props, "bench42@localhost"); err == nil {
			t.Errorf("identity %v was accepted", props)
		}
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

// A send blocked on a peer that never reads must end when the tracker closes the
// raw connection, not when the library next looks at its context.
func TestClosingTrackedConnectionsUnblocksAStuckSend(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close() // accepted and never read
		}
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tr := &connTracker{}
	tr.add(c)
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1<<20)
		for { // fills the socket buffers, then blocks
			if _, err := c.Write(buf); err != nil {
				done <- err
				return
			}
		}
	}()
	time.Sleep(300 * time.Millisecond) // let the writer block
	select {
	case err := <-done:
		t.Fatalf("the write ended on its own: %v", err)
	default:
	}
	tr.closeAll()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a write error after the connection was closed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closing the raw connection did not unblock the stuck send")
	}
	if tr.count() != 0 {
		t.Fatalf("%d connections still tracked", tr.count())
	}
}

// A dial attempt owns its result: a failed attempt leaves nothing tracked, so a
// late attempt can never replace the accepted connection.
func TestFailedDialLeavesNothingTracked(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close() // accepts, never speaks AMQP
		}
	}()
	tr := &connTracker{}
	start := time.Now()
	if conn, err := dialBroker(context.Background(), l.Addr().String(), 200*time.Millisecond, tr); err == nil {
		conn.Close()
		t.Fatal("a server that never speaks AMQP was accepted")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the attempt ignored its handshake deadline")
	}
	if tr.count() != 0 {
		t.Fatalf("a failed attempt left %d connections tracked", tr.count())
	}
	// A cancelled context ends the attempt without a connection.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := dialBroker(ctx, l.Addr().String(), time.Second, tr); err == nil || tr.count() != 0 {
		t.Fatalf("cancelled dial: %v, %d tracked", err, tr.count())
	}
}

// A connection registered after closeAll (a socket that connected just before the
// guard tripped) is closed at once and never tracked.
func TestLateRegistrationIsClosedAtOnce(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	tr := &connTracker{}
	tr.closeAll()
	if tr.add(a) {
		t.Fatal("a connection was tracked after closeAll")
	}
	if tr.count() != 0 {
		t.Fatalf("%d tracked", tr.count())
	}
	done := make(chan error, 1)
	go func() { _, err := a.Write([]byte("x")); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the late connection is still open")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the late connection was not closed")
	}
}

func TestDialAfterTheTrialWasCancelledReturnsAtOnce(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err == nil {
			defer c.Close()
			time.Sleep(5 * time.Second)
		}
	}()
	tr := &connTracker{}
	tr.closeAll() // the guard has already tripped
	start := time.Now()
	if _, err := dialBroker(context.Background(), l.Addr().String(), 10*time.Second, tr); err == nil {
		t.Fatal("dialed through a closed tracker")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("a socket that connected after the trip waited out its handshake deadline")
	}
	if tr.count() != 0 {
		t.Fatalf("%d tracked", tr.count())
	}
}
