package dostoevsky

import (
	"hash/maphash"
	"testing"
)

func TestFingerprintHitChecksExactProducerID(t *testing.T) {
	cfg := config()
	cfg.Topics = map[string]int{"events": 1}
	n := openTest(t, t.TempDir(), cfg)
	publish := func(id string) Record {
		t.Helper()
		r, err := n.Publish(ctx, "events", Message{ID: id, Value: []byte("same payload")})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	publish("first")
	wanted := publish("wanted")
	other := publish("other")
	// Force two colliding chains without weakening the production hash.
	idx := n.streams[Partition{"events", 0}].tail.ids
	idx.add(maphash.String(producerIDSeed, "wanted"), other.Offset)
	idx.add(maphash.String(producerIDSeed, "absent"), other.Offset)
	if got := publish("wanted"); got.Offset != wanted.Offset || got.ID != "wanted" {
		t.Fatal(got)
	}
	items := []Publication{{"events", Message{ID: "absent", Value: []byte("same payload")}}}
	for i := 0; i < 2; i++ {
		got, err := n.PublishBatch(ctx, items)
		if err != nil || got[0].Offset != 3 || got[0].ID != "absent" {
			t.Fatal(got, err)
		}
	}
}
