package wireserver

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/wire"
)

// memory is a store without a disk, so fuzzing exercises the server and not the journal.
type memory struct{}

func (memory) Publish(_ context.Context, topic string, m d.Message) (d.Record, error) {
	return d.Record{Partition: d.Partition{Topic: topic}, Message: m}, nil
}

func (memory) PublishBatch(_ context.Context, items []d.Publication) ([]d.Record, error) {
	return make([]d.Record, len(items)), nil
}

func (memory) Read(_ context.Context, p d.Partition, offset int64, _ bool) (d.Record, error) {
	if offset == 0 {
		return d.Record{Partition: p, Message: d.Message{Value: []byte("v")}, Time: time.Unix(1, 0)}, nil
	}
	return d.Record{}, d.ErrEmpty
}

func (memory) Await(ctx context.Context, _ d.Partition, offset int64) error {
	if offset == 0 {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (memory) Position(context.Context, d.Partition, string) (d.Position, error) {
	return d.Position{}, nil
}

func (memory) Commit(context.Context, d.Partition, string, int64, []byte) error { return nil }

func fuzzServer(f *testing.F) *Server {
	srv, err := New(memory{}, Config{Token: token, Window: 4, MaxConnections: 1 << 20, AuthTimeout: time.Second, FrameTimeout: time.Second})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { srv.Close() })
	return srv
}

// feed sends data to the server over a pipe, draining whatever it answers, and then hangs up.
func feed(t *testing.T, srv *Server, before, data []byte) {
	near, far := net.Pipe()
	defer near.Close()
	srv.accept(far)
	_ = near.SetDeadline(time.Now().Add(5 * time.Second))
	if len(before) > 0 {
		if _, err := near.Write(before); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(near, make([]byte, wire.WelcomeSize)); err != nil {
			t.Fatal(err)
		}
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_, _ = io.Copy(io.Discard, near)
	}()
	_ = near.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, _ = near.Write(data)
	near.Close()
	<-drained
}

func FuzzFrames(f *testing.F) {
	for _, q := range []wire.Request{
		publish(1, "jobs", "id", "value"),
		read(2, "jobs", 0, 0),
		read(3, "jobs", 1, 30000),
		{Type: wire.Position, RequestID: 4, Topic: "jobs", Group: "g"},
		{Type: wire.Commit, RequestID: 5, Topic: "jobs", Group: "g", Value: []byte("s")},
		{Type: wire.Ping, RequestID: 6},
	} {
		b, err := wire.AppendRequest(nil, q)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		f.Add(append(append([]byte(nil), b...), b...))
	}
	hello, err := wire.AppendHello(nil, wire.Hello{MaxVersion: 1, Token: []byte(token)})
	if err != nil {
		f.Fatal(err)
	}
	srv := fuzzServer(f)
	f.Fuzz(func(t *testing.T, data []byte) { feed(t, srv, hello, data) })
}

func FuzzHandshake(f *testing.F) {
	hello, err := wire.AppendHello(nil, wire.Hello{MaxVersion: 1, Token: []byte(token)})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(hello)
	f.Add([]byte("DSTV\x01\x00\x00\x00\xff\xff"))
	srv := fuzzServer(f)
	f.Fuzz(func(t *testing.T, data []byte) { feed(t, srv, nil, data) })
}
