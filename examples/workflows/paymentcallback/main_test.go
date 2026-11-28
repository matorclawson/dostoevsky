package main

import (
	"errors"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/workflow"
)

type ledger struct {
	charged map[string]bool
	settled map[string]string
}

func (l ledger) Charge(key string) error {
	l.charged[key] = true
	return nil
}

func (l ledger) Settle(key, status string) error {
	l.settled[key] = status
	return nil
}

func open(t *testing.T, dir string) *workflow.Store {
	t.Helper()
	s, err := workflow.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = register(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// lose runs one activity and dies before Finish, then completes the retry after
// a restart.
func lose(t *testing.T, dir, activity string, world ledger) {
	t.Helper()
	s := open(t, dir)
	task, err := s.Poll(queue, 50*time.Millisecond)
	if err != nil || task == nil || task.Activity != activity {
		t.Fatal(task, err)
	}
	if _, failure := handle(task, world); failure != "" {
		t.Fatal(failure)
	}
	if (activity == "charge" && !world.charged["payment/1"]) || (activity == "settle" && world.settled["payment/1"] == "") {
		t.Fatal("first effect", activity, world)
	}
	lost := task.Run
	s.Close()
	time.Sleep(100 * time.Millisecond)
	s = open(t, dir)
	defer s.Close()
	task, err = s.Poll(queue, time.Minute)
	if err != nil || task == nil || task.Run != lost || task.Activity != activity || task.Attempt != 2 {
		t.Fatal(task, err)
	}
	out, failure := handle(task, world)
	if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
		t.Fatal(err)
	}
}

func TestCrashAndResume(t *testing.T) {
	dir := t.TempDir()
	world := ledger{map[string]bool{}, map[string]string{}}
	s := open(t, dir)
	if err := start(s, "1"); err != nil {
		t.Fatal(err)
	}
	if err := deliver(s, "1", "succeeded"); !errors.Is(err, d.ErrConflict) {
		t.Fatal("early callback", err)
	}
	s.Close()

	lose(t, dir, "charge", world)

	s = open(t, dir)
	if task, err := s.Poll(queue, time.Minute); err != nil || task != nil {
		t.Fatal("not waiting", task, err)
	}
	if err := deliver(s, "1", "succeeded"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = open(t, dir)
	if err := deliver(s, "1", "succeeded"); err != nil {
		t.Fatal("redelivery", err)
	}
	if err := deliver(s, "1", "failed"); !errors.Is(err, d.ErrConflict) {
		t.Fatal("contradicting callback", err)
	}
	s.Close()

	lose(t, dir, "settle", world)

	s = open(t, dir)
	defer s.Close()
	if err := deliver(s, "1", "succeeded"); err != nil {
		t.Fatal("redelivery after completion", err)
	}
	if len(world.charged) != 1 || len(world.settled) != 1 || world.settled["payment/1"] != "succeeded" {
		t.Fatal(world)
	}
}
