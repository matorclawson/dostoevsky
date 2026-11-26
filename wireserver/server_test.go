package wireserver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/wire"
)

const token = "0123456789abcdef0123456789abcdef"

func openNode(t testing.TB) *d.Node {
	node, err := d.Open(t.TempDir(), d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"*": 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	return node
}

func start(t testing.TB, store Store, set func(*Config)) (*Server, string) {
	cfg := Config{Token: token}
	if set != nil {
		set(&cfg)
	}
	srv, err := New(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-done; !errors.Is(err, ErrServerClosed) {
			t.Errorf("serve: %v", err)
		}
	})
	return srv, ln.Addr().String()
}

type client struct {
	t  testing.TB
	nc net.Conn
}

func connect(t testing.TB, addr string) client {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	if err := nc.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return client{t, nc}
}

func (c client) hello(version uint16, tok string) (wire.Welcome, error) {
	b, err := wire.AppendHello(nil, wire.Hello{MaxVersion: version, Token: []byte(tok)})
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.nc.Write(b); err != nil {
		return wire.Welcome{}, err
	}
	reply := make([]byte, wire.WelcomeSize)
	if _, err := io.ReadFull(c.nc, reply); err != nil {
		return wire.Welcome{}, err
	}
	return wire.ParseWelcome(reply)
}

func dial(t testing.TB, addr string) client {
	c := connect(t, addr)
	w, err := c.hello(1, token)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Accept(1); err != nil {
		t.Fatal(err)
	}
	return c
}

func (c client) write(b []byte) {
	if _, err := c.nc.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

func (c client) send(q wire.Request) {
	b, err := wire.AppendRequest(nil, q)
	if err != nil {
		c.t.Fatal(err)
	}
	c.write(b)
}

func (c client) receive() (wire.Reply, error) {
	header := make([]byte, wire.HeaderSize)
	if _, err := io.ReadFull(c.nc, header); err != nil {
		return wire.Reply{}, err
	}
	h, err := wire.ParseHeader(header)
	if err != nil {
		return wire.Reply{}, err
	}
	body := make([]byte, h.Length)
	if _, err := io.ReadFull(c.nc, body); err != nil {
		return wire.Reply{}, err
	}
	return wire.DecodeReply(h, body)
}

func (c client) reply() wire.Reply {
	p, err := c.receive()
	if err != nil {
		c.t.Fatal(err)
	}
	return p
}

func (c client) call(q wire.Request) wire.Reply {
	c.send(q)
	p := c.reply()
	if p.RequestID != q.RequestID {
		c.t.Fatalf("reply to %d, sent %d", p.RequestID, q.RequestID)
	}
	return p
}

// ended requires that the server closes without sending anything more.
func (c client) ended() {
	n, err := c.nc.Read(make([]byte, 1))
	var timeout net.Error
	if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
		c.t.Fatalf("connection not closed: %d bytes, %v", n, err)
	}
}

func publish(id uint32, topic, messageID, value string) wire.Request {
	return wire.Request{Type: wire.Publish, RequestID: id, Topic: topic, MessageID: messageID, Value: []byte(value)}
}

func read(id uint32, topic string, offset uint64, waitMS uint16) wire.Request {
	return wire.Request{Type: wire.Read, RequestID: id, Topic: topic, Offset: offset, WaitMS: waitMS}
}

func frame(typ byte, flags byte, id uint32, body []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	b = binary.LittleEndian.AppendUint32(append(b, typ, flags), id)
	return append(b, body...)
}

func TestRoundTrip(t *testing.T) {
	_, addr := start(t, openNode(t), nil)
	c := dial(t, addr)
	if p := c.call(wire.Request{Type: wire.Ping, RequestID: 1}); p.Type != wire.PingOK {
		t.Fatal(p)
	}
	if p := c.call(publish(2, "jobs", "", "one")); p.Type != wire.PublishOK || p.Offset != 0 {
		t.Fatal(p)
	}
	if p := c.call(read(3, "jobs", 0, 0)); p.Type != wire.ReadOK || !p.Found || string(p.Value) != "one" || p.Time == 0 {
		t.Fatal(p)
	}
	position := wire.Request{Type: wire.Position, RequestID: 4, Topic: "jobs", Group: "g"}
	if p := c.call(position); p.Type != wire.PositionOK || p.Offset != 0 || len(p.Value) != 0 {
		t.Fatal(p)
	}
	commit := wire.Request{Type: wire.Commit, RequestID: 5, Topic: "jobs", Group: "g", Value: []byte("state")}
	if p := c.call(commit); p.Type != wire.CommitOK {
		t.Fatal(p)
	}
	if p := c.call(commit); p.Type != wire.Error || p.Code != wire.Conflict {
		t.Fatal(p)
	}
	if p := c.call(position); p.Offset != 1 || string(p.Value) != "state" {
		t.Fatal(p)
	}
	if p := c.call(read(6, "jobs", 1, 0)); p.Type != wire.ReadOK || p.Found {
		t.Fatal(p)
	}
}

// gated holds the first write until released and records the size of each batch.
type gated struct {
	*d.Node
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	batches []int
}

func gate(node *d.Node) *gated {
	return &gated{Node: node, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gated) Publish(ctx context.Context, topic string, m d.Message) (d.Record, error) {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.Node.Publish(ctx, topic, m)
}

func (g *gated) PublishBatch(ctx context.Context, items []d.Publication) ([]d.Record, error) {
	g.mu.Lock()
	g.batches = append(g.batches, len(items))
	g.mu.Unlock()
	return g.Node.PublishBatch(ctx, items)
}

func TestQueuedPublishesShareABatchInOrderAndOneConflictFailsAlone(t *testing.T) {
	g := gate(openNode(t))
	_, addr := start(t, g, nil)
	c := dial(t, addr)
	c.send(publish(1, "jobs", "x", "first"))
	<-g.entered
	c.send(publish(2, "jobs", "y", "second"))
	c.send(publish(3, "jobs", "x", "different"))
	c.send(publish(4, "jobs", "z", "third"))
	// A ping is answered by the reader, so its reply shows the frames before it were admitted.
	c.send(wire.Request{Type: wire.Ping, RequestID: 5})
	if p := c.reply(); p.Type != wire.PingOK {
		t.Fatal(p)
	}
	close(g.release)
	got := map[uint32]wire.Reply{}
	for range 4 {
		p := c.reply()
		got[p.RequestID] = p
	}
	for id, offset := range map[uint32]uint64{1: 0, 2: 1, 4: 2} {
		if p := got[id]; p.Type != wire.PublishOK || p.Offset != offset {
			t.Fatalf("request %d: %+v", id, p)
		}
	}
	if p := got[3]; p.Type != wire.Error || p.Code != wire.Conflict {
		t.Fatal(p)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.batches) != 1 || g.batches[0] != 3 {
		t.Fatalf("batches %v", g.batches)
	}
}

func TestPipelinedWritesTakeEffectInOrderBeyondTheWindow(t *testing.T) {
	for _, window := range []int{1, 256} {
		_, addr := start(t, openNode(t), func(c *Config) { c.Window = window })
		c := dial(t, addr)
		const n = 100
		var b []byte
		for i := range n {
			var err error
			b, err = wire.AppendRequest(b, publish(uint32(2*i+1), "jobs", "", "v"))
			if err != nil {
				t.Fatal(err)
			}
			b, err = wire.AppendRequest(b, wire.Request{Type: wire.Commit, RequestID: uint32(2*i + 2), Topic: "jobs", Group: "g", Offset: uint64(i)})
			if err != nil {
				t.Fatal(err)
			}
		}
		c.write(b)
		for range 2 * n {
			p := c.reply()
			if p.Type == wire.Error || (p.Type == wire.PublishOK && p.Offset != uint64(p.RequestID/2)) {
				t.Fatalf("window %d: %+v", window, p)
			}
		}
	}
}

func TestADisconnectDoesNotStopAnAdmittedPublish(t *testing.T) {
	g := gate(openNode(t))
	_, addr := start(t, g, nil)
	c := dial(t, addr)
	c.send(publish(1, "jobs", "", "kept"))
	<-g.entered
	c.nc.Close()
	close(g.release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := g.Node.Read(ctx, d.Partition{Topic: "jobs"}, 0, true)
	if err != nil || string(r.Value) != "kept" {
		t.Fatal(r, err)
	}
}

func TestHandshakeRefusals(t *testing.T) {
	_, addr := start(t, openNode(t), nil)
	for _, v := range []struct {
		version uint16
		token   string
		status  wire.Code
	}{
		{1, "wrong-token-wrong-token", wire.Unauthorized},
		{1, "short", wire.Unauthorized},
		{0, token, wire.UnsupportedVersion},
		{0, "wrong-token-wrong-token", wire.Unauthorized},
	} {
		c := connect(t, addr)
		w, err := c.hello(v.version, v.token)
		if err != nil || w != (wire.Welcome{Status: v.status}) {
			t.Fatalf("%+v: %+v %v", v, w, err)
		}
		c.ended()
	}
	c := connect(t, addr)
	c.write([]byte("GET / HTTP"))
	c.ended()
	// A newer client is answered with version 1.
	if w, err := connect(t, addr).hello(9, token); err != nil || w.Status != wire.OK || w.Version != 1 {
		t.Fatal(w, err)
	}
}

func TestRequestErrorsKeepTheConnection(t *testing.T) {
	_, addr := start(t, openNode(t), nil)
	c := dial(t, addr)
	// POSITION of the topic "a b", group "g".
	c.write(frame(byte(wire.Position), 0, 7, []byte("\x03a b\x00\x00\x01g")))
	if p := c.reply(); p.Type != wire.Error || p.RequestID != 7 || p.Code != wire.Invalid {
		t.Fatal(p)
	}
	if p := c.call(read(8, "jobs", 0, 0)); p.Type != wire.ReadOK {
		t.Fatal(p)
	}
	wrong := read(9, "jobs", 0, 0)
	wrong.Partition = 5
	if p := c.call(wrong); p.Type != wire.Error || p.Code != wire.Invalid {
		t.Fatal(p)
	}
	if p := c.call(wire.Request{Type: wire.Ping, RequestID: 9}); p.Type != wire.PingOK {
		t.Fatal("an answered id may be used again", p)
	}
}

func TestConnectionErrors(t *testing.T) {
	_, addr := start(t, openNode(t), nil)
	waiting, err := wire.AppendRequest(nil, read(1, "jobs", 0, 30000))
	if err != nil {
		t.Fatal(err)
	}
	tooLong := binary.LittleEndian.AppendUint32(nil, wire.MaxBody+1)
	tooLong = append(tooLong, byte(wire.Publish), 0, 1, 0, 0, 0)
	for _, v := range []struct {
		name  string
		bytes []byte
		code  wire.Code
	}{
		{"flags", frame(byte(wire.Ping), 1, 1, nil), wire.Protocol},
		{"zero id", frame(byte(wire.Ping), 0, 0, nil), wire.Protocol},
		{"unknown type", frame(6, 0, 1, nil), wire.Protocol},
		{"reply type", frame(byte(wire.PingOK), 0, 1, nil), wire.Protocol},
		{"bytes left", frame(byte(wire.Ping), 0, 1, []byte{0}), wire.Protocol},
		{"overrun", frame(byte(wire.Position), 0, 1, []byte{9, 'a'}), wire.Protocol},
		{"too long", tooLong, wire.Limit},
		{"unanswered id", append(waiting, frame(byte(wire.Ping), 0, 1, nil)...), wire.Protocol},
	} {
		c := dial(t, addr)
		c.write(v.bytes)
		p, err := c.receive()
		if err != nil || p.Type != wire.Error || p.RequestID != 0 || p.Code != v.code {
			t.Fatalf("%s: %+v %v", v.name, p, err)
		}
		c.ended()
	}
}

// A pipe has no buffer, so the server's write of a reply stays in progress until the whole reply is read.
func TestAnIDIsTakenUntilItsReplyIsWritten(t *testing.T) {
	srv, _ := start(t, openNode(t), nil)
	near, far := net.Pipe()
	defer near.Close()
	if err := near.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	srv.accept(far)
	c := client{t, near}
	if w, err := c.hello(1, token); err != nil || w.Status != wire.OK {
		t.Fatal(w, err)
	}
	ping := wire.Request{Type: wire.Ping, RequestID: 1}
	c.send(ping)
	first := make([]byte, wire.HeaderSize)
	if _, err := io.ReadFull(near, first[:1]); err != nil {
		t.Fatal(err)
	}
	c.send(ping)
	time.Sleep(reuseGrace + 500*time.Millisecond)
	if _, err := io.ReadFull(near, first[1:]); err != nil {
		t.Fatal(err)
	}
	if p := c.reply(); p.Type != wire.Error || p.RequestID != 0 || p.Code != wire.Protocol {
		t.Fatal(p)
	}
}

func TestAnAnsweredIDMayBeReusedAtOnce(t *testing.T) {
	_, addr := start(t, openNode(t), nil)
	c := dial(t, addr)
	for range 2000 {
		if p := c.call(wire.Request{Type: wire.Ping, RequestID: 1}); p.Type != wire.PingOK {
			t.Fatal(p)
		}
	}
}

func TestLongPoll(t *testing.T) {
	_, addr := start(t, openNode(t), nil)
	reader, writer := dial(t, addr), dial(t, addr)
	if p := reader.call(read(1, "jobs", 0, 50)); p.Type != wire.ReadOK || p.Found {
		t.Fatal(p)
	}
	reader.send(read(2, "jobs", 0, 30000))
	writer.call(publish(1, "jobs", "", "late"))
	if p := reader.reply(); !p.Found || string(p.Value) != "late" {
		t.Fatal(p)
	}
}

func TestAWaitingReadHoldsNoReplyBudget(t *testing.T) {
	_, addr := start(t, openNode(t), func(c *Config) { c.ReplyPool, c.ConnectionReplies = maxFrame, maxFrame })
	waiter, other := dial(t, addr), dial(t, addr)
	waiter.send(read(1, "quiet", 0, 30000))
	other.call(publish(1, "jobs", "", "v"))
	if p := other.call(read(2, "jobs", 0, 0)); !p.Found {
		t.Fatal(p)
	}
	if p := waiter.call(wire.Request{Type: wire.Ping, RequestID: 2}); p.Type != wire.PingOK {
		t.Fatal(p)
	}
}

func TestSmallestPoolsStillMakeProgress(t *testing.T) {
	_, addr := start(t, openNode(t), func(c *Config) {
		c.RequestPool, c.ReplyPool, c.ConnectionReplies, c.WindowBytes = maxFrame, maxFrame, maxFrame, maxFrame
	})
	value := string(bytes.Repeat([]byte{'x'}, 600<<10))
	var wg sync.WaitGroup
	for i := range 4 {
		c := dial(t, addr)
		topic := fmt.Sprintf("t%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 3 {
				b, err := wire.AppendRequest(nil, publish(uint32(j+1), topic, "", value))
				if err == nil {
					_, err = c.nc.Write(b)
				}
				var p wire.Reply
				if err == nil {
					p, err = c.receive()
				}
				if err == nil {
					b, err = wire.AppendRequest(nil, read(uint32(j+1), topic, uint64(j), 0))
				}
				if err == nil {
					_, err = c.nc.Write(b)
				}
				if err == nil {
					p, err = c.receive()
				}
				if err != nil || !p.Found || len(p.Value) != len(value) {
					t.Errorf("%s %d: %v %v", topic, j, p.Type, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestDeadlines(t *testing.T) {
	_, addr := start(t, openNode(t), func(c *Config) { c.AuthTimeout = 200 * time.Millisecond })
	connect(t, addr).ended()

	_, addr = start(t, openNode(t), func(c *Config) { c.IdleTimeout = 200 * time.Millisecond })
	c := dial(t, addr)
	// Not idle while a request is unanswered.
	if p := c.call(read(1, "jobs", 0, 600)); p.Type != wire.ReadOK || p.Found {
		t.Fatal(p)
	}
	c.ended()

	_, addr = start(t, openNode(t), func(c *Config) { c.FrameTimeout = 200 * time.Millisecond })
	c = dial(t, addr)
	c.write([]byte{1, 0, 0})
	c.ended()
}

func TestConnectionSlots(t *testing.T) {
	_, addr := start(t, openNode(t), func(c *Config) { c.MaxUnauthenticated = 1 })
	first := connect(t, addr)
	connect(t, addr).ended()
	if w, err := first.hello(1, token); err != nil || w.Status != wire.OK {
		t.Fatal(w, err)
	}

	_, addr = start(t, openNode(t), func(c *Config) { c.MaxConnections = 1 })
	dial(t, addr)
	connect(t, addr).ended()
}

func TestCloseReturnsWithWaitingReadsAndPartFrames(t *testing.T) {
	srv, addr := start(t, openNode(t), nil)
	waiting, part, unauthenticated := dial(t, addr), dial(t, addr), connect(t, addr)
	waiting.send(read(1, "jobs", 0, 30000))
	part.write([]byte{1, 0, 0})
	done := make(chan struct{})
	go func() {
		srv.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("close did not return")
	}
	unauthenticated.ended()
}

type address struct {
	net.Listener
	addr net.Addr
}

func (a address) Addr() net.Addr { return a.addr }
func (a address) Close() error   { return nil }

func TestPlaintextIsRefusedOffThisMachine(t *testing.T) {
	srv, err := New(openNode(t), Config{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	for _, ip := range []string{"0.0.0.0", "10.0.0.5", "::"} {
		if err := srv.Serve(address{addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 1}}); err == nil {
			t.Errorf("%s accepted in plaintext", ip)
		}
	}
}

func TestTLS(t *testing.T) {
	web := httptest.NewTLSServer(nil)
	defer web.Close()
	_, addr := start(t, openNode(t), func(c *Config) { c.TLS = &tls.Config{Certificates: web.TLS.Certificates} })
	roots := x509.NewCertPool()
	roots.AddCert(web.Certificate())
	nc, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	c := client{t, nc}
	if w, err := c.hello(1, token); err != nil || w.Status != wire.OK {
		t.Fatal(w, err)
	}
	if p := c.call(publish(1, "jobs", "", "v")); p.Type != wire.PublishOK {
		t.Fatal(p)
	}
	// A plaintext client gets no WELCOME.
	if w, err := connect(t, addr).hello(1, token); err == nil {
		t.Fatal(w)
	}
}

func TestConfigurationConstraints(t *testing.T) {
	for _, set := range []func(*Config){
		func(c *Config) { c.Token = "short" },
		func(c *Config) { c.Window = 1 << 16 },
		func(c *Config) { c.Window = -1 },
		func(c *Config) { c.WindowBytes = maxFrame - 1 },
		func(c *Config) { c.RequestPool = maxFrame - 1 },
		func(c *Config) { c.ReplyPool = maxFrame - 1 },
		func(c *Config) { c.ConnectionReplies = maxFrame - 1 },
		func(c *Config) { c.IdleTimeout = -time.Second },
	} {
		cfg := Config{Token: token}
		set(&cfg)
		if srv, err := New(memory{}, cfg); err == nil {
			srv.Close()
			t.Errorf("%+v accepted", cfg)
		}
	}
}

func TestErrorCodes(t *testing.T) {
	for err, want := range map[error]wire.Code{
		d.ErrInvalid: wire.Invalid, d.ErrOwner: wire.WrongOwner, d.ErrConflict: wire.Conflict, d.ErrExpired: wire.Expired,
		d.ErrLimit: wire.Limit, d.ErrClosed: wire.Closed, context.DeadlineExceeded: wire.Timeout,
		context.Canceled: wire.Canceled, io.ErrShortWrite: wire.StorageFailure,
	} {
		if got := code(fmt.Errorf("wrapped: %w", err)); got != want {
			t.Errorf("%v: %v, want %v", err, got, want)
		}
	}
}
