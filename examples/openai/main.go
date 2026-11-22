// Command openai records a live model or agent event stream and replays it locally.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return errors.New("usage: openai text|agent|replay JOURNAL_DIR")
	}
	mode := args[0]
	node, e := d.Open(args[1], d.Config{Node: "local", Nodes: []string{"local"}, Topics: map[string]int{"events": 1}})
	if e != nil {
		return e
	}
	defer node.Close()
	p := d.Partition{Topic: "events"}
	if mode == "replay" {
		for offset := int64(0); ; offset++ {
			r, e := node.Read(ctx, p, offset, false)
			if errors.Is(e, d.ErrEmpty) {
				return nil
			}
			if e != nil {
				return e
			}
			fmt.Println(string(r.Value))
		}
	}
	if mode != "text" && mode != "agent" {
		return errors.New("unknown mode")
	}
	if _, e = node.Read(ctx, p, 0, false); e != d.ErrEmpty {
		if e != nil {
			return e
		}
		return errors.New("journal contains a previous run: use replay or choose a new directory")
	}
	key, model := os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_MODEL")
	if key == "" || model == "" {
		return errors.New("set OPENAI_API_KEY and OPENAI_MODEL")
	}
	publish := func(data []byte) error {
		r, e := node.Publish(ctx, p.Topic, d.Message{Key: mode, Value: data})
		if e == nil {
			fmt.Printf("%d %s\n", r.Offset, data)
		}
		return e
	}
	return capture(ctx, &http.Client{Timeout: 5 * time.Minute}, "https://api.openai.com/v1", mode, model, key, publish)
}
func capture(ctx context.Context, c *http.Client, base, mode, model, key string, publish func([]byte) error) error {
	path := "/responses"
	terminal := "response.completed"
	body := map[string]any{"stream": true, "model": model, "input": "Explain in two sentences how an event consumer resumes from a checkpoint."}
	if mode == "agent" {
		path = "/agents/sessions"
		terminal = "agent.session.turn.completed"
		body = map[string]any{"stream": true, "agent": map[string]string{"model": model, "instructions": "Use the sandbox to calculate the result and summarize it."}, "environment": map[string]string{"type": "openai_hosted"}, "input": "Use Python to compute the median of these synthetic delivery latencies in milliseconds: 8, 13, 5, 21, 3. Report the result."}
	}
	b, e := json.Marshal(body)
	if e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(b))
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if mode == "agent" {
		req.Header.Set("OpenAI-Beta", "agents=v1")
	}
	res, e := c.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("OpenAI returned %s (request %s)", res.Status, res.Header.Get("x-request-id"))
	}
	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 4096), d.MaxValue+64)
	var data []byte
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			if field, ok := strings.CutPrefix(line, "data:"); ok {
				if len(data) > 0 {
					data = append(data, '\n')
				}
				field = strings.TrimPrefix(field, " ")
				if len(data)+len(field) > d.MaxValue {
					return d.ErrLimit
				}
				data = append(data, field...)
			}
			continue
		}
		if len(data) == 0 {
			continue
		}
		if string(data) == "[DONE]" {
			break
		}
		var event struct{ Type string }
		if e = json.Unmarshal(data, &event); e != nil {
			return e
		}
		if event.Type == "" {
			return errors.New("event missing type")
		}
		if e = publish(data); e != nil {
			return e
		}
		switch event.Type {
		case terminal:
			return nil
		case "error", "response.failed", "response.incomplete", "agent.session.turn.failed", "agent.session.turn.cancelled", "agent.session.failed":
			return fmt.Errorf("stream ended with %s", event.Type)
		}
		data = nil
	}
	if e = scanner.Err(); e != nil {
		return e
	}
	return io.ErrUnexpectedEOF
}
