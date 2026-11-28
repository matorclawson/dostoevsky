package dostoevsky

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAwait(t *testing.T) {
	n, err := Open(t.TempDir(), Config{Node: "a", Nodes: []string{"a"}, Topics: map[string]int{"jobs": 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	p := Partition{Topic: "jobs"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := n.Await(ctx, p, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(n.streams) != 0 {
		t.Fatal("an abandoned wait left a stream behind")
	}
	bg := context.Background()
	got := make(chan error, 1)
	go func() { got <- n.Await(bg, p, 0) }()
	if _, err := n.Publish(bg, "jobs", Message{Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	if err := n.Await(bg, p, -1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	go func() { got <- n.Await(bg, p, 1) }()
	n.Close()
	if err := <-got; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
