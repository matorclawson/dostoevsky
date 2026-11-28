package main

import (
	"strings"
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

// Every step dies once after its effects and before Finish.
func TestCrashAndResume(t *testing.T) {
	dir := t.TempDir()
	text := strings.Repeat("é", 2000)
	world := memory{map[string][]byte{"doc": []byte(text)}, map[string]chunk{}}

	s := open(t, dir)
	if err := onUpload(s, "1", "doc"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	for _, activity := range []string{"download", "embed", "upsert"} {
		s = open(t, dir)
		task, err := s.Poll(queue, 50*time.Millisecond)
		if err != nil || task == nil || task.Activity != activity {
			t.Fatal(task, err)
		}
		if _, failure := handle(task, world); failure != "" {
			t.Fatal(failure)
		}
		s.Close()
		time.Sleep(100 * time.Millisecond)

		s = open(t, dir)
		task, err = s.Poll(queue, time.Minute)
		if err != nil || task == nil || task.Activity != activity || task.Attempt != 2 {
			t.Fatal(task, err)
		}
		out, failure := handle(task, world)
		if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	s = open(t, dir)
	defer s.Close()
	if err := onUpload(s, "1", "doc"); err != nil {
		t.Fatal(err)
	}
	if task, err := s.Poll(queue, time.Minute); err != nil || task != nil {
		t.Fatal(task, err)
	}
	// 2000 runes in chunks of 1000 overlapping by 200: 0-1000, 800-1800, 1600-2000.
	if len(world.vectors) != 3 || world.vectors["doc#2"].Text != strings.Repeat("é", 400) {
		t.Fatal(len(world.vectors))
	}
}
