// A durable task queue: submitting the same task ID twice starts it once, workers
// lease tasks, and the result can be read back later.
//
// Mirrors the immediate half of the message queue pattern in
// github.com/restatedev/examples (MIT), go/patterns-use-cases/src/queue. Delayed
// sends are not shown: the module has no delayed start.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "task"
const queue = "tasks"

func register(s *workflow.Store) error {
	return s.Register(name, 1, []workflow.Step{{Kind: "activity", Name: "work", Attempts: 20}})
}

// submit reports whether this call created the task. Reusing an ID with another
// payload returns workflow.ErrStartConflict.
func submit(s *workflow.Store, id, payload string) (bool, error) {
	input, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	return s.StartOnce(name+"/"+id, name, 1, queue, input, nil)
}

// handle runs a task. record must be idempotent for its key: it runs again if
// the process dies before Finish is recorded.
func handle(task *workflow.Task, record func(key, value string) error) ([]byte, string) {
	var payload string
	if err := json.Unmarshal(task.Input, &payload); err != nil {
		return nil, err.Error()
	}
	value := strings.ToUpper(payload)
	if err := record(task.Run, value); err != nil {
		return nil, err.Error()
	}
	out, err := json.Marshal(value)
	if err != nil {
		return nil, err.Error()
	}
	return out, ""
}

// result returns the finished task's output, or false while it is not finished.
func result(s *workflow.Store, id string) (string, bool, error) {
	events, err := s.History(name+"/"+id, 0, 1000)
	if err != nil {
		return "", false, err
	}
	for _, event := range events {
		if event.Kind == "WorkflowCompleted" {
			var value string
			err = json.Unmarshal([]byte(event.Data), &value)
			return value, err == nil, err
		}
	}
	return "", false, nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: queue DATA-DIRECTORY")
	}
	s, err := workflow.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = register(s); err != nil {
		log.Fatal(err)
	}
	for range 2 {
		created, err := submit(s, "42", "heavy work")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("created:", created)
	}
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		if task == nil {
			break
		}
		out, failure := handle(task, func(string, string) error { return nil })
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			log.Fatal(err)
		}
	}
	value, done, err := result(s, "42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(done, value)
}
