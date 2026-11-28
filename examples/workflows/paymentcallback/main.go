// A payment that finishes by callback: charge, wait durably for the provider's
// callback, then settle.
//
// Mirrors the payment signal pattern in github.com/restatedev/examples (MIT),
// python/patterns-use-cases/signalspayments, for its asynchronous path only. A
// callback that arrives before the run waits for it is refused, so the endpoint
// must return an error and the provider must redeliver. The wait has no
// deadline. The callback value replaces the value carried between steps.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/workflow"
)

const name = "payment"
const queue = "payments"

// waitStep is the position of the signal step in the definition.
const waitStep = 1

// provider calls must be idempotent for their key.
type provider interface {
	Charge(key string) error
	Settle(key, status string) error
}

func register(s *workflow.Store) error {
	return s.Register(name, 1, []workflow.Step{
		{Kind: "activity", Name: "charge", Attempts: 20},
		{Kind: "signal", Name: "callback"},
		{Kind: "activity", Name: "settle", Attempts: 20},
	})
}

func start(s *workflow.Store, id string) error {
	_, err := s.StartOnce(name+"/"+id, name, 1, queue, []byte(`{}`), nil)
	return err
}

// deliver records a callback. It returns nil when this status is already
// recorded, so a redelivery after a lost reply is acknowledged. Any error means
// the provider should deliver again later.
func deliver(s *workflow.Store, id, status string) error {
	value, err := json.Marshal(status)
	if err != nil {
		return err
	}
	run := name + "/" + id
	err = s.Signal(run, waitStep, "callback", value)
	if !errors.Is(err, d.ErrConflict) {
		return err
	}
	events, historyErr := s.History(run, 0, 1000)
	if historyErr != nil {
		return historyErr
	}
	for _, event := range events {
		if event.Kind == "SignalReceived" && event.Step == waitStep && event.Data == string(value) {
			return nil
		}
	}
	return err
}

func handle(task *workflow.Task, p provider) ([]byte, string) {
	switch task.Activity {
	case "charge":
		if err := p.Charge(task.Run); err != nil {
			return nil, err.Error()
		}
	case "settle":
		var status string
		if err := json.Unmarshal(task.Input, &status); err != nil {
			return nil, err.Error()
		}
		if err := p.Settle(task.Run, status); err != nil {
			return nil, err.Error()
		}
	}
	return task.Input, ""
}

type printer struct{}

func (printer) Charge(key string) error {
	fmt.Println("charge sent", key)
	return nil
}

func (printer) Settle(key, status string) error {
	fmt.Println("settled", key, status)
	return nil
}

func drain(s *workflow.Store) error {
	for {
		task, err := s.Poll(queue, time.Minute)
		if err != nil || task == nil {
			return err
		}
		out, failure := handle(task, printer{})
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			return err
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: paymentcallback DATA-DIRECTORY")
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
	if err = drain(s); err != nil {
		log.Fatal(err)
	}
	if err = deliver(s, "1", "succeeded"); err != nil {
		log.Fatal(err)
	}
	if err = drain(s); err != nil {
		log.Fatal(err)
	}
}
