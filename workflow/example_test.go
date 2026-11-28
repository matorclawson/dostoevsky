package workflow_test

import (
	"fmt"
	"os"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

func ExampleStore() {
	dir, err := os.MkdirTemp("", "workflow-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	store, err := workflow.Open(dir)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	err = store.Register("receipt", 1, []workflow.Step{{Kind: "activity", Name: "record-payment"}})
	if err != nil {
		panic(err)
	}
	created, err := store.StartOnce("order-17", "receipt", 1, "billing", []byte(`{"amount":42}`), nil)
	if err != nil {
		panic(err)
	}
	task, err := store.Poll("billing", time.Minute)
	if err != nil {
		panic(err)
	}
	if err = store.Finish(task.Run, task.Token, []byte("true"), ""); err != nil {
		panic(err)
	}
	events, err := store.History("order-17", 0, 10)
	if err != nil {
		panic(err)
	}
	fmt.Println(created, task.Activity, events[len(events)-1].Kind)
	// Output: true record-payment WorkflowCompleted
}
