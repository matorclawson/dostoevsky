package main

import (
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

func expect(t *testing.T, s *workflow.Store, reservation string, want bool) {
	t.Helper()
	got, err := reserve(s, "p", reservation)
	if err != nil || got != want {
		t.Fatal(reservation, got, err)
	}
}

func TestCrashAndResume(t *testing.T) {
	dir := t.TempDir()
	told := map[string]string{}
	notify := func(key string, reservation []byte) error {
		told[key] = string(reservation)
		return nil
	}

	s := open(t, dir)
	expect(t, s, "first", true)
	s.Close()

	// The decision survives a restart, whether or not the caller saw the reply.
	s = open(t, dir)
	expect(t, s, "second", false)
	expect(t, s, "first", true)

	// Die after the effect and before Finish.
	task, err := s.Poll(queue, 50*time.Millisecond)
	if err != nil || task == nil {
		t.Fatal(task, err)
	}
	handle(task, notify)
	s.Close()
	time.Sleep(100 * time.Millisecond)

	s = open(t, dir)
	task, err = s.Poll(queue, time.Minute)
	if err != nil || task == nil || task.Attempt != 2 {
		t.Fatal(task, err)
	}
	out, failure := handle(task, notify)
	if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s = open(t, dir)
	defer s.Close()
	expect(t, s, "first", true)
	expect(t, s, "second", false)
	if task, err = s.Poll(queue, time.Minute); err != nil || task != nil {
		t.Fatal(task, err)
	}
	if len(told) != 1 || told["reserve/p"] != `"first"` {
		t.Fatal(told)
	}
}
