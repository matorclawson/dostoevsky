// A trip booking saga: book a flight, a car and a hotel; if one is refused,
// cancel what was booked, in reverse order.
//
// Mirrors the saga pattern in github.com/restatedev/examples (MIT),
// go/patterns-use-cases/src/sagas. The module cannot branch on failure, so a
// refusal is data: forward steps record it and compensation steps act on it. A
// run that was rolled back therefore ends completed. If a step uses up its
// attempts (crashes and expired leases count), the run fails and no later
// compensation runs.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "trip"
const queue = "trips"
const attempts = 100

// errRefused is a final answer from a provider. Other errors are retried.
var errRefused = errors.New("refused")

type trip struct {
	Booked  []string `json:"booked"`
	Refused string   `json:"refused"`
}

// provider calls must be idempotent for their key: they run again if the process
// dies before Finish is recorded.
type provider interface {
	Book(key, item string) error
	Cancel(key, item string) error
}

func register(s *workflow.Store) error {
	var steps []workflow.Step
	for _, step := range []string{"book-flight", "book-car", "book-hotel", "cancel-hotel", "cancel-car", "cancel-flight"} {
		steps = append(steps, workflow.Step{Kind: "activity", Name: step, Attempts: attempts, BackoffMillis: 100})
	}
	return s.Register(name, 1, steps)
}

func start(s *workflow.Store, id string) error {
	_, err := s.StartOnce(name+"/"+id, name, 1, queue, []byte(`{}`), nil)
	return err
}

func handle(task *workflow.Task, p provider) ([]byte, string) {
	var t trip
	if err := json.Unmarshal(task.Input, &t); err != nil {
		return nil, err.Error()
	}
	action, item, _ := strings.Cut(task.Activity, "-")
	key := task.Run + "/" + item
	switch {
	case action == "book" && t.Refused == "":
		err := p.Book(key, item)
		switch {
		case errors.Is(err, errRefused):
			t.Refused = item
		case err != nil:
			return nil, err.Error()
		default:
			t.Booked = append(t.Booked, item)
		}
	case action == "cancel" && t.Refused != "" && slices.Contains(t.Booked, item):
		if err := p.Cancel(key, item); err != nil {
			return nil, err.Error()
		}
	}
	out, err := json.Marshal(t)
	if err != nil {
		return nil, err.Error()
	}
	return out, ""
}

type printer struct{}

func (printer) Book(_, item string) error {
	if item == "hotel" {
		fmt.Println("refused", item)
		return errRefused
	}
	fmt.Println("booked", item)
	return nil
}

func (printer) Cancel(_, item string) error {
	fmt.Println("canceled", item)
	return nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: saga DATA-DIRECTORY")
	}
	s, err := workflow.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = register(s); err != nil {
		log.Fatal(err)
	}
	if err = start(s, "1"); err != nil {
		log.Fatal(err)
	}
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		if task == nil {
			return
		}
		out, failure := handle(task, printer{})
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			log.Fatal(err)
		}
	}
}
