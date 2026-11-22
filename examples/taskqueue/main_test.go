package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	d "github.com/rodriveracom/dostoevsky"
)

type lostAck struct{ d.Store }

func (lostAck) Commit(context.Context, d.Partition, string, int64, []byte) error {
	return errors.New("interrupted")
}
func TestRestart(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		task, topic string
		attempt     int
	}{{"sha256", "results", 1}, {"retry-demo", "results", 2}, {"exhaust-demo", "dead-letters", 3}, {"invalid", "dead-letters", 1}} {
		t.Run(tc.task, func(t *testing.T) {
			dir := t.TempDir()
			n, e := d.Open(dir, configuration())
			if e != nil {
				t.Fatal(e)
			}
			j := job{"id", tc.task, "hello", 1}
			r, e := enqueue(ctx, n, j)
			if e != nil {
				t.Fatal(e)
			}
			duplicate, e := enqueue(ctx, n, j)
			if e != nil || duplicate.Offset != r.Offset {
				t.Fatal(duplicate, e)
			}
			if e = handle(ctx, lostAck{n}, r); e == nil {
				t.Fatal("missing simulated interruption")
			}
			n.Close()
			n, e = d.Open(dir, configuration())
			if e != nil {
				t.Fatal(e)
			}
			defer n.Close()
			for i := 0; i < 2; i++ {
				if e = drain(ctx, n); e != nil {
					t.Fatal(e)
				}
			}
			pos, e := n.Position(ctx, r.Partition, group)
			if e != nil || pos.Next != int64(tc.attempt) {
				t.Fatal(pos, e)
			}
			p := d.Partition{Topic: tc.topic, Number: r.Number}
			out, e := n.Read(ctx, p, 0, false)
			if e != nil {
				t.Fatal(e)
			}
			var got outcome
			if e = json.Unmarshal(out.Value, &got); e != nil || got.Job.Attempt != tc.attempt {
				t.Fatal(got, e)
			}
			if _, e = n.Read(ctx, p, 1, false); e != d.ErrEmpty {
				t.Fatal("duplicate outcome", e)
			}
		})
	}
}
