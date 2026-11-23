package main

import (
	"context"
	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/benchmarks/embedded/internal/run"
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
func (s store) Close() {
	if e := s.node.Close(); e != nil {
		panic(e)
	}
}
func main() {
	run.Main("dostoevsky", func(dir string, _ bool) (run.Store, error) {
		n, e := d.Open(dir, d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
		return store{n}, e
	})
}
