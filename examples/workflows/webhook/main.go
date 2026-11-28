// Durable webhook intake: an event is acknowledged only after it is committed,
// and a redelivery of the same event does not start it again.
//
// Mirrors the webhook callback pattern in github.com/restatedev/examples (MIT),
// go/patterns-use-cases/src/webhookcallbacks.
package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "payment-event"
const queue = "payment-events"

type event struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Invoice string `json:"invoice"`
}

func register(s *workflow.Store) error {
	return s.Register(name, 1, []workflow.Step{{Kind: "activity", Name: "apply", Attempts: 20}})
}

// intake stores the fields it uses in one spelling, so a provider that resends
// the event with different spacing or extra fields is still recognized.
func intake(s *workflow.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var e event
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&e); err != nil || e.ID == "" || e.Invoice == "" {
			http.Error(w, "bad event", http.StatusBadRequest)
			return
		}
		input, err := json.Marshal(e)
		if err == nil {
			_, err = s.StartOnce(name+"/"+e.ID, name, 1, queue, input, nil)
		}
		switch {
		case errors.Is(err, workflow.ErrStartConflict):
			http.Error(w, "event ID reused with other content", http.StatusConflict)
		case err != nil:
			// Not acknowledged: the provider redelivers.
			http.Error(w, "not stored", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}
}

// handle applies an event. apply must be idempotent for its key.
func handle(task *workflow.Task, apply func(key string, e event) error) ([]byte, string) {
	var e event
	if err := json.Unmarshal(task.Input, &e); err != nil {
		return nil, err.Error()
	}
	if err := apply(task.Run, e); err != nil {
		return nil, err.Error()
	}
	return task.Input, ""
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: webhook DATA-DIRECTORY LISTEN-ADDRESS")
	}
	s, err := workflow.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = register(s); err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Fatal(http.ListenAndServe(os.Args[2], intake(s)))
	}()
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		if task == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		out, failure := handle(task, func(key string, e event) error {
			log.Println(key, e.Type, e.Invoice)
			return nil
		})
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			log.Print(err)
		}
	}
}
