package main

import (
	"errors"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

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

func TestCrashAndResume(t *testing.T) {
	dir := t.TempDir()
	recorded := map[string]string{}
	record := func(key, value string) error {
		recorded[key] = value
		return nil
	}

	s := open(t, dir)
	if created, err := submit(s, "1", "a"); err != nil || !created {
		t.Fatal(created, err)
	}
	s.Close()

	s = open(t, dir)
	if created, err := submit(s, "1", "a"); err != nil || created {
		t.Fatal(created, err)
	}
	if _, err := submit(s, "1", "b"); !errors.Is(err, workflow.ErrStartConflict) {
		t.Fatal(err)
	}

	// Die after the effect and before Finish.
	lost, err := s.Poll(queue, 50*time.Millisecond)
	if err != nil || lost == nil {
		t.Fatal(lost, err)
	}
	if _, failure := handle(lost, record); failure != "" || recorded["task/1"] != "A" {
		t.Fatal("first effect", failure, recorded)
	}
	s.Close()
	time.Sleep(100 * time.Millisecond)

	s = open(t, dir)
	task, err := s.Poll(queue, time.Minute)
	if err != nil || task == nil || task.Run != lost.Run || task.Step != lost.Step || task.Attempt != 2 {
		t.Fatal(task, err)
	}
	out, failure := handle(task, record)
	if err = s.Finish(lost.Run, lost.Token, out, ""); !errors.Is(err, workflow.ErrStale) {
		t.Fatal("expired lease accepted", err)
	}
	if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = open(t, dir)
	defer s.Close()
	value, done, err := result(s, "1")
	if err != nil || !done || value != "A" {
		t.Fatal(value, done, err)
	}
	if err = s.Finish(task.Run, task.Token, out, ""); !errors.Is(err, workflow.ErrStale) {
		t.Fatal("repeated finish accepted", err)
	}
	if len(recorded) != 1 || recorded["task/1"] != "A" {
		t.Fatal(recorded)
	}
}
