package workflow

import (
	"errors"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

func TestSignalReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	steps := []Step{{Kind: "activity", Name: "prepare"}, {Kind: "signal", Name: "ok"}, {Kind: "signal", Name: "ok"}, {Kind: "activity", Name: "send"}}
	if err = s.Register("work", 1, steps); err != nil {
		t.Fatal(err)
	}
	if _, err = s.StartOnce("id", "work", 1, "q", []byte("0"), nil); err != nil {
		t.Fatal(err)
	}
	if err = s.Signal("id", 1, "ok", []byte("1")); !errors.Is(err, d.ErrConflict) {
		t.Fatal(err)
	}
	task, err := s.Poll("q", time.Minute)
	if err != nil || task == nil {
		t.Fatal(task, err)
	}
	if err = s.Finish("id", task.Token, []byte("1"), ""); err != nil {
		t.Fatal(err)
	}
	if task, err = s.Poll("q", time.Minute); err != nil || task != nil {
		t.Fatal(task, err)
	}
	if err = s.Signal("id", 1, "ok", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Signal("id", 1, "ok", []byte("2")); !errors.Is(err, d.ErrConflict) {
		t.Fatal(err)
	}
	if err = s.Signal("id", 2, "ok", []byte("3")); err != nil {
		t.Fatal(err)
	}
	task, err = s.Poll("q", time.Minute)
	if err != nil || task == nil || string(task.Input) != "3" {
		t.Fatal(task, err)
	}
}
