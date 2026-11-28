// Durable one-shot reservation: each product can be reserved once, and a repeated
// request with the same reservation ID gets the same answer.
//
// Mirrors the durable RPC pattern in github.com/restatedev/examples (MIT),
// go/patterns-use-cases/src/durablerpc. Restate keys a virtual object by product;
// here the run ID is the key and StartOnce is the single decision. This covers a
// claim that is never released, not general per-key state.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "reserve"
const queue = "reservations"

func register(s *workflow.Store) error {
	return s.Register(name, 1, []workflow.Step{{Kind: "activity", Name: "notify-warehouse", Attempts: 20}})
}

// reserve reports whether reservation holds product. The answer is durable when
// it returns.
func reserve(s *workflow.Store, product, reservation string) (bool, error) {
	input, err := json.Marshal(reservation)
	if err != nil {
		return false, err
	}
	_, err = s.StartOnce(name+"/"+product, name, 1, queue, input, nil)
	if errors.Is(err, workflow.ErrStartConflict) {
		return false, nil
	}
	return err == nil, err
}

// handle does the follow-up work of a reservation. notify must be idempotent for
// its key: it runs again if the process dies before Finish is recorded.
func handle(task *workflow.Task, notify func(key string, reservation []byte) error) ([]byte, string) {
	if err := notify(task.Run, task.Input); err != nil {
		return nil, err.Error()
	}
	return task.Input, ""
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: durablerpc DATA-DIRECTORY")
	}
	s, err := workflow.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = register(s); err != nil {
		log.Fatal(err)
	}
	for _, reservation := range []string{"r-1", "r-2", "r-1"} {
		ok, err := reserve(s, "product-7", reservation)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(reservation, ok)
	}
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil {
			log.Fatal(err)
		}
		if task == nil {
			return
		}
		out, failure := handle(task, func(key string, reservation []byte) error {
			fmt.Println("warehouse told:", key, string(reservation))
			return nil
		})
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			log.Fatal(err)
		}
	}
}
