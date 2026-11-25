package main

import (
	"fmt"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"time"
)

type store struct {
	s      *server.Server
	conn   *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
}

func (s store) Publish(data []byte) (uint64, error) {
	ack, e := s.js.Publish(run.Context(), "events", data)
	if e != nil {
		return 0, e
	}
	return ack.Sequence, nil
}
func (s store) Read(i uint64) ([]byte, error) {
	r, e := s.stream.GetMsg(run.Context(), i)
	if e != nil {
		return nil, e
	}
	return r.Data, nil
}
func (s store) Close() { s.conn.Close(); s.s.Shutdown(); s.s.WaitForShutdown() }
func main() {
	run.Main("jetstream", func(dir string, create bool) (run.Store, error) {
		s, e := server.NewServer(&server.Options{DontListen: true, NoSigs: true, JetStream: true, StoreDir: dir, SyncAlways: true})
		if e != nil {
			return nil, e
		}
		s.Start()
		if !s.ReadyForConnections(10 * time.Second) {
			return nil, fmt.Errorf("server startup timeout")
		}
		nc, e := nats.Connect("", nats.InProcessServer(s))
		if e != nil {
			return nil, e
		}
		js, e := jetstream.New(nc)
		if e != nil {
			return nil, e
		}
		var stream jetstream.Stream
		if create {
			stream, e = js.CreateStream(run.Context(), jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events"}, Storage: jetstream.FileStorage, Replicas: 1})
		} else {
			stream, e = js.Stream(run.Context(), "EVENTS")
		}
		return store{s, nc, js, stream}, e
	})
}
