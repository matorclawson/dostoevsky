package main

import (
	"context"
	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
	"github.com/rodriveracom/dostoevsky/internal/tsprobe"
)

type store struct{ node *d.Node }

func (s store) Publish(data []byte) (uint64, error) {
	r, e := s.node.Publish(context.Background(), "events", d.Message{Value: data})
	return uint64(r.Offset), e
}
func (s store) Read(i uint64) ([]byte, error) {
	r, e := s.node.Read(context.Background(), d.Partition{Topic: "events"}, int64(i), false)
	return r.Value, e
}

// Retain lets the sustained mode bound the journal: records below keepFrom may be dropped.
func (s store) Retain(keepFrom uint64) error {
	return s.node.Retain(context.Background(), map[d.Partition]int64{{Topic: "events"}: int64(keepFrom)})
}
func (s store) Close() {
	if e := s.node.Close(); e != nil {
		panic(e)
	}
}
func main() {
	defer tsprobe.Profile()()
	run.Main("dostoevsky", func(dir string, _ bool) (run.Store, error) {
		n, e := d.Open(dir, d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
		return store{n}, e
	})
}
