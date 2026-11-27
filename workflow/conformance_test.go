package workflow_test

import (
	"bytes"
	"encoding/json"
	engine "github.com/rodriveracom/dostoevsky/workflow"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Run unchanged (apart from the import path) against the Ephemeral SQLite implementation.
func TestActivityConformance(t *testing.T) {
	for _, scenario := range []string{"lost-start", "lost-lease-last", "lost-lease-retry", "lost-finish", "lost-failure", "competing-workers"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			open := func() *engine.Store {
				s, e := engine.Open(path)
				if e != nil {
					t.Fatal(e)
				}
				return s
			}
			s := open()
			defer func() { s.Close() }()
			attempts := 0
			if scenario == "lost-lease-retry" || scenario == "lost-failure" {
				attempts = 2
			}
			if e := s.Register("work", 1, []engine.Step{{Kind: "activity", Name: "send", Attempts: attempts}}); e != nil {
				t.Fatal(e)
			}
			start := func(input string) (bool, error) {
				return s.StartOnce("run", "work", 1, "q", json.RawMessage(input), nil)
			}
			if ok, e := start("{ }"); !ok || e != nil {
				t.Fatal(ok, e)
			}
			reopen := func() { s.Close(); s = open() }
			hist := func(want ...string) {
				ev, e := s.History("run", 0, 20)
				if e != nil {
					t.Fatal(e)
				}
				var got []string
				var last int64
				for _, v := range ev {
					if v.Sequence <= last {
						t.Fatal("unordered history")
					}
					last = v.Sequence
					got = append(got, v.Kind)
				}
				if !reflect.DeepEqual(want, got) {
					t.Fatal(want, got)
				}
			}
			poll := func(lease time.Duration) *engine.Task {
				task, e := s.Poll("q", lease)
				if e != nil {
					t.Fatal(e)
				}
				return task
			}
			switch scenario {
			case "lost-start":
				reopen()
				if ok, e := start("{ }"); ok || e != nil {
					t.Fatal(ok, e)
				}
				if ok, e := start("{}"); ok || e != engine.ErrStartConflict {
					t.Fatal(ok, e)
				}
				hist("WorkflowStarted", "ActivityScheduled")
			case "lost-lease-last", "lost-lease-retry":
				if task := poll(300 * time.Millisecond); task == nil || task.Attempt != 1 {
					t.Fatal(task)
				}
				reopen()
				if task := poll(time.Second); task != nil {
					t.Fatal("granted live lease twice", task)
				}
				time.Sleep(310 * time.Millisecond)
				task := poll(time.Second)
				if attempts == 0 {
					if task != nil {
						t.Fatal(task)
					}
					hist("WorkflowStarted", "ActivityScheduled", "ActivityStarted", "ActivityTimedOut", "WorkflowFailed")
				} else if task == nil || task.Attempt != 2 {
					t.Fatal(task)
				}
			case "lost-finish", "lost-failure":
				task := poll(time.Minute)
				if task == nil {
					t.Fatal("missing lease")
				}
				failure := ""
				if attempts == 2 {
					failure = "busy"
				}
				if e := s.Finish("run", task.Token, json.RawMessage("42"), failure); e != nil {
					t.Fatal(e)
				}
				reopen()
				if e := s.Finish("run", task.Token, json.RawMessage("42"), failure); e == nil || e.Error() != "stale activity lease" {
					t.Fatal(e)
				}
				if failure == "" {
					hist("WorkflowStarted", "ActivityScheduled", "ActivityStarted", "ActivityCompleted", "WorkflowCompleted")
					ev, e := s.History("run", taskSequence(t, s), 1)
					if e != nil || len(ev) != 1 || ev[0].Data != "42" {
						t.Fatal(ev, e)
					}
				} else {
					hist("WorkflowStarted", "ActivityScheduled", "ActivityStarted", "ActivityFailed", "RetryScheduled")
				}
			case "competing-workers":
				var wg sync.WaitGroup
				tasks := make(chan *engine.Task, 32)
				begin := make(chan struct{})
				for i := 0; i < 32; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-begin
						task, e := s.Poll("q", time.Minute)
						if e != nil {
							t.Error(e)
						}
						if task != nil {
							tasks <- task
						}
					}()
				}
				close(begin)
				wg.Wait()
				close(tasks)
				if len(tasks) != 1 {
					t.Fatal("grants", len(tasks))
				}
				for task := range tasks {
					if !bytes.Equal(task.Input, []byte("{ }")) {
						t.Fatal(task)
					}
				}
			}
		})
	}
}
func taskSequence(t *testing.T, s *engine.Store) int64 {
	t.Helper()
	ev, e := s.History("run", 0, 20)
	if e != nil {
		t.Fatal(e)
	}
	for _, v := range ev {
		if v.Kind == "ActivityStarted" {
			return v.Sequence
		}
	}
	t.Fatal("missing start")
	return 0
}
