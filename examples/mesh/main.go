// Mesh runs one application instance. Each instance owns a partition and can publish to either.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	d "github.com/rodriveracom/dostoevsky"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

func main() {
	id := flag.String("node", "a", "a or b")
	dir := flag.String("dir", "", "persistent directory")
	flag.Parse()
	ports := map[string]string{"a": "127.0.0.1:8711", "b": "127.0.0.1:8712"}
	if *dir == "" || ports[*id] == "" {
		log.Fatal("provide -dir and -node a|b")
	}
	n, e := d.Open(*dir, d.Config{Node: *id, Nodes: []string{"a", "b"}, Topics: map[string]int{"words": 2}})
	if e != nil {
		log.Fatal(e)
	}
	defer n.Close()
	peers := map[string]string{"a": "http://" + ports["a"], "b": "http://" + ports["b"]}
	m, e := d.NewMesh(n, peers, os.Getenv("DOST_TOKEN"), nil)
	if e != nil {
		log.Fatal(e)
	}
	ln, e := net.Listen("tcp", ports[*id])
	if e != nil {
		log.Fatal(e)
	}
	server := &http.Server{Handler: m, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second}
	go func() {
		if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
		}
	}()
	defer server.Close()
	fmt.Println("ready; enter one word per line")
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := m.Publish(ctx, "words", d.Message{Key: s.Text(), Value: []byte(s.Text())})
		cancel()
		if err != nil {
			log.Print(err)
			continue
		}
		fmt.Printf("owner=%s partition=%d offset=%d\n", []string{"a", "b"}[r.Number], r.Number, r.Offset)
	}
	if err := s.Err(); err != nil {
		log.Print(err)
	}
}
