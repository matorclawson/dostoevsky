// Command taskqueue demonstrates a durable producer, partition workers and result inspection.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	d "github.com/zeroemployeeorg/dostoevsky"
)

const partitions = 4
const maxAttempts = 3
const group = "task-workers"

type job struct {
	ID      string `json:"id"`
	Task    string `json:"task"`
	Input   string `json:"input"`
	Attempt int    `json:"attempt"`
}
type outcome struct {
	Job   job    `json:"job"`
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

func configuration() d.Config {
	return d.Config{Node: "app", Nodes: []string{"app"}, Topics: map[string]int{"jobs": partitions, "results": partitions, "dead-letters": partitions}}
}

// The job ID selects a partition. The attempt ID deduplicates producer retries.
func enqueue(ctx context.Context, s d.Publisher, j job) (d.Record, error) {
	if len(j.ID) == 0 || len(j.ID) > 128 || len(j.Input) > 32768 {
		return d.Record{}, d.ErrInvalid
	}
	b, e := json.Marshal(j)
	if e != nil {
		return d.Record{}, e
	}
	return s.Publish(ctx, "jobs", d.Message{Key: j.ID, ID: fmt.Sprintf("%s/%d", j.ID, j.Attempt), Value: b})
}

func execute(j job) (value string, retryable bool, err error) {
	switch j.Task {
	case "sha256":
		return fmt.Sprintf("%x", sha256.Sum256([]byte(j.Input))), false, nil
	case "retry-demo":
		if j.Attempt == 1 {
			return "", true, errors.New("simulated transient failure")
		}
		return strings.ToUpper(j.Input), false, nil
	case "exhaust-demo":
		return "", true, errors.New("simulated persistent failure")
	default:
		return "", false, fmt.Errorf("unknown task %q", j.Task)
	}
}
func handle(ctx context.Context, s d.Store, r d.Record) error {
	var j job
	if e := json.Unmarshal(r.Value, &j); e != nil {
		return e
	}
	value, retryable, taskErr := execute(j)
	if retryable && j.Attempt < maxAttempts {
		j.Attempt++
		if _, e := enqueue(ctx, s, j); e != nil {
			return e
		}
	} else {
		topic := "results"
		result := outcome{Job: j, Value: value}
		if taskErr != nil {
			topic = "dead-letters"
			result.Error = taskErr.Error()
		}
		data, e := json.Marshal(result)
		if e != nil {
			return e
		}
		if _, e = s.Publish(ctx, topic, d.Message{Key: j.ID, ID: j.ID, Value: data}); e != nil {
			return e
		}
	}
	// A crash before this commit replays the input. The preceding publish uses a stable ID.
	return s.Commit(ctx, r.Partition, group, r.Offset, nil)
}
func worker(ctx context.Context, s d.Store, p d.Partition) error {
	pos, e := s.Position(ctx, p, group)
	if e != nil {
		return e
	}
	for {
		r, e := s.Read(ctx, p, pos.Next, false)
		if errors.Is(e, d.ErrEmpty) {
			return nil
		}
		if e != nil {
			return e
		}
		if e = handle(ctx, s, r); e != nil {
			return e
		}
		pos.Next++
	}
}
func drain(ctx context.Context, s d.Store) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first error
	var once sync.Once
	for i := 0; i < partitions; i++ {
		wg.Add(1)
		go func(number int) {
			defer wg.Done()
			if e := worker(ctx, s, d.Partition{Topic: "jobs", Number: number}); e != nil {
				once.Do(func() { first = e; cancel() })
			}
		}(i)
	}
	wg.Wait()
	return first
}
func run(ctx context.Context, args []string) error {
	usage := errors.New("usage: taskqueue DIR enqueue ID TASK INPUT | drain | inspect")
	if len(args) < 2 {
		return usage
	}
	n, e := d.Open(args[0], configuration())
	if e != nil {
		return e
	}
	defer n.Close()
	switch args[1] {
	case "enqueue":
		if len(args) != 5 {
			return usage
		}
		r, e := enqueue(ctx, n, job{args[2], args[3], args[4], 1})
		if e == nil {
			fmt.Printf("queued %s partition=%d offset=%d\n", args[2], r.Number, r.Offset)
		}
		return e
	case "drain":
		return drain(ctx, n)
	case "inspect":
		for _, topic := range []string{"results", "dead-letters"} {
			for i := 0; i < partitions; i++ {
				for off := int64(0); ; off++ {
					r, e := n.Read(ctx, d.Partition{Topic: topic, Number: i}, off, false)
					if errors.Is(e, d.ErrEmpty) {
						break
					}
					if e != nil {
						return e
					}
					fmt.Printf("%s %s\n", topic, r.Value)
				}
			}
		}
		return nil
	default:
		return usage
	}
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if e := run(ctx, os.Args[1:]); e != nil {
		log.Fatal(e)
	}
}
