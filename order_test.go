package dostoevsky

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rodriveracom/dostoevsky/internal/seam"
)

// Retention, backup and migration walk the node's partitions and groups, which are held in maps. With fixed inputs two
// runs must still write the same bytes, so the state here has many partitions and many groups.
func TestWithFixedInputsRewritesStoreTheSameBytes(t *testing.T) {
	cfg := Config{Node: "a", Nodes: []string{"a"}, Topics: map[string]int{"first": 4, "second": 4}}
	ctx := context.Background()
	fill := func(t *testing.T, n *Node) []Partition {
		used := map[Partition]bool{}
		for _, topic := range []string{"first", "second"} {
			for i := 0; i < 32; i++ {
				r, err := n.Publish(ctx, topic, Message{Key: fmt.Sprintf("key-%d", i), Value: []byte("v"), ID: fmt.Sprintf("%s-%d", topic, i)})
				if err != nil {
					t.Fatal(err)
				}
				used[r.Partition] = true
			}
		}
		parts := partitionsInOrder(used)
		if len(parts) < 4 {
			t.Fatalf("only %d partitions hold records", len(parts))
		}
		for _, p := range parts {
			for g := 0; g < 6; g++ {
				if err := n.CommitBatch(ctx, p, fmt.Sprintf("group-%d", g), 0, 1, []byte("state")); err != nil {
					t.Fatal(err)
				}
			}
		}
		return parts
	}
	for name, open := range map[string]func(string, Config) (*Node, error){"legacy": Open, "segmented": OpenSegmented} {
		t.Run(name, func(t *testing.T) {
			run := func() map[string]map[string][]byte {
				seam.Fix(1_700_000_000_000_000_000, 1_000_000, 7)
				defer seam.Restore()
				root := t.TempDir()
				n, err := open(filepath.Join(root, "store"), cfg)
				if err != nil {
					t.Fatal(err)
				}
				parts := fill(t, n)
				if err := n.Backup(ctx, filepath.Join(root, "backup")); err != nil {
					t.Fatal(err)
				}
				out := map[string]map[string][]byte{}
				if name == "legacy" {
					if err := n.MigrateSegmented(ctx, filepath.Join(root, "migrated")); err != nil {
						t.Fatal(err)
					}
				}
				floors := map[Partition]int64{}
				for _, p := range parts {
					floors[p] = 1
				}
				if err := n.Retain(ctx, floors); err != nil {
					t.Fatal(err)
				}
				if err := n.Close(); err != nil {
					t.Fatal(err)
				}
				out["store after retention"] = storedFiles(t, filepath.Join(root, "store"))
				out["backup"] = storedFiles(t, filepath.Join(root, "backup"))
				if name == "legacy" {
					out["migrated"] = storedFiles(t, filepath.Join(root, "migrated"))
				}
				return out
			}
			first := run()
			// Two further runs: one equal pair could be chance.
			for attempt := 0; attempt < 2; attempt++ {
				second := run()
				for what, files := range first {
					if len(files) == 0 || len(files) != len(second[what]) {
						t.Fatalf("%s: %d and %d files", what, len(files), len(second[what]))
					}
					for file, data := range files {
						if !bytes.Equal(data, second[what][file]) {
							t.Errorf("%s: %s differs between runs", what, file)
						}
					}
				}
			}
		})
	}
}
