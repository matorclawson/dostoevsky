// Command fixture creates an ID-bearing legacy store and a segmented copy for comparison.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	d "github.com/rodriveracom/dostoevsky"
)

func main() {
	parent := flag.String("dir", "", "new fixture directory")
	count := flag.Int("n", 1000000, "records")
	flag.Parse()
	if *parent == "" || *count < 1 {
		panic("invalid fixture arguments")
	}
	if err := os.Mkdir(*parent, 0700); err != nil {
		panic(err)
	}
	n, err := d.Open(filepath.Join(*parent, "legacy"), d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	for i := 0; i < *count; {
		batch := make([]d.Publication, 0, min(256, *count-i))
		for len(batch) < cap(batch) {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(i))
			hash := sha256.Sum256(b[:])
			batch = append(batch, d.Publication{Topic: "events", Message: d.Message{ID: hex.EncodeToString(hash[:16]), Value: b[:]}})
			i++
		}
		rs, e := n.PublishBatch(ctx, batch)
		if e != nil {
			panic(e)
		}
		if rs[len(rs)-1].Offset != int64(i-1) {
			panic("wrong offset")
		}
	}
	if err = n.MigrateSegmented(ctx, filepath.Join(*parent, "segmented")); err != nil {
		panic(err)
	}
	if err = n.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("seeded and migrated %d identified records\n", *count)
}
