// Wordcount prints durable cumulative counts for command-line words.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	d "github.com/zeroemployeeorg/dostoevsky"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: wordcount DATA_DIRECTORY [WORD ...]")
	}
	node, err := d.Open(os.Args[1], d.Config{Node: "app", Nodes: []string{"app"}, Topics: map[string]int{"words": 1}})
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()
	ctx := context.Background()
	for _, word := range os.Args[2:] {
		if _, err = node.Publish(ctx, "words", d.Message{Key: word, Value: []byte(word)}); err != nil {
			log.Fatal(err)
		}
	}
	p := d.Partition{Topic: "words", Number: 0}
	for {
		pos, err := node.Position(ctx, p, "counts")
		if err != nil {
			log.Fatal(err)
		}
		counts := map[string]int{}
		if pos.Next > 0 {
			if err = json.Unmarshal(pos.State, &counts); err != nil {
				log.Fatal(err)
			}
		}
		r, err := node.Read(ctx, p, pos.Next, false)
		if errors.Is(err, d.ErrEmpty) {
			summary, err := json.Marshal(counts)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(summary))
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		counts[r.Key]++
		state, err := json.Marshal(counts)
		if err != nil {
			log.Fatal(err)
		}
		if err = node.Commit(ctx, p, "counts", r.Offset, state); err != nil {
			log.Fatal(err)
		}
	}
}
