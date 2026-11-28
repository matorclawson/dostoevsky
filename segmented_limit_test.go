package dostoevsky

import (
	"context"
	"errors"
	"testing"
)

func TestSegmentedRefusesRecordsPastTheIndexLimit(t *testing.T) {
	original := segmentPosLimit
	defer func() { segmentPosLimit = original }()
	cfg := Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
	n, err := OpenSegmented(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx := context.Background()
	m := Message{ID: "one", Value: []byte("x")}
	segmentPosLimit = 1
	if _, err = n.Publish(ctx, "events", m); !errors.Is(err, ErrLimit) {
		t.Fatalf("publish past the limit: %v, want ErrLimit", err)
	}
	segmentPosLimit = original
	r, err := n.Publish(ctx, "events", m)
	if err != nil || r.Offset != 0 {
		t.Fatalf("after the refusal: offset %d, %v; want 0 and no error", r.Offset, err)
	}
	// A commit stores no position and is still accepted.
	segmentPosLimit = 1
	if err = n.Commit(ctx, r.Partition, "g", 0, []byte("s")); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
