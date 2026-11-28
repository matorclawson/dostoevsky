// Command catalog tests the separate cost of many distinct partitions in a checkpoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

func main() {
	dir := flag.String("dir", "", "disposable data directory")
	seed := flag.Bool("seed", false, "create the fixture and checkpoint the catalog")
	count := flag.Int("topics", 100000, "number of distinct one-record topics")
	flag.Parse()
	if *dir == "" || *count < 2 {
		panic("supply a directory and at least two topics")
	}
	name := func(i int) string { return "topic-" + strconv.Itoa(i) }
	ctx := context.Background()
	start := time.Now()
	n, err := d.OpenSegmented(*dir, d.Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"*": 1}})
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	rss, _ := os.ReadFile("/proc/self/status")
	if *seed {
		for i := 0; i < *count; {
			batch := make([]d.Publication, 0, min(256, *count-i))
			for len(batch) < cap(batch) {
				s := name(i)
				batch = append(batch, d.Publication{Topic: s, Message: d.Message{Value: []byte(s)}})
				i++
			}
			if _, err = n.PublishBatch(ctx, batch); err != nil {
				panic(err)
			}
		}
		for i := 0; i < *count; i += 1000 {
			if err = n.Commit(ctx, d.Partition{Topic: name(i)}, "worker", 0, []byte(name(i))); err != nil {
				panic(err)
			}
		}
		if err = n.Retain(ctx, map[d.Partition]int64{{Topic: name(0)}: 1}); err != nil {
			panic(err)
		}
	} else {
		for i := 0; i < *count; i += 1000 {
			p := d.Partition{Topic: name(i)}
			pos, err := n.Position(ctx, p, "worker")
			if err != nil || pos.Next != 1 || string(pos.State) != p.Topic {
				panic("wrong checkpoint")
			}
			if i > 0 {
				r, err := n.Read(ctx, p, 0, false)
				if err != nil || string(r.Value) != p.Topic {
					panic("wrong record")
				}
			}
		}
		if _, err = n.Read(ctx, d.Partition{Topic: name(0)}, 0, false); err != d.ErrExpired {
			panic("lost retention floor")
		}
	}
	for _, line := range strings.Split(string(rss), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fmt.Print(line, " ")
		}
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("topics=%d seed=%t open_ms=%.3f process_s=%.3f live_heap_MiB=%.2f\n", *count, *seed, elapsed.Seconds()*1000, time.Since(start).Seconds(), float64(m.HeapAlloc)/(1<<20))
	if err = n.Close(); err != nil {
		panic(err)
	}
}
