package main

import (
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

// ledger is the outside world. It survives our restarts and deduplicates by key.
type ledger struct {
	refuse string
	held   map[string]bool
}

func (l *ledger) Book(key, item string) error {
	if item == l.refuse {
		return errRefused
	}
	// A key that was canceled stays canceled.
	if _, seen := l.held[key]; !seen {
		l.held[key] = true
	}
	return nil
}

func (l *ledger) Cancel(key, _ string) error {
	l.held[key] = false
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

// Each case restarts after every step, and in one of the six steps dies after
// the effect and before Finish.
func TestCrashAndResume(t *testing.T) {
	for _, c := range []struct {
		refuse string
		want   map[string]bool
	}{
		{"", map[string]bool{"trip/1/flight": true, "trip/1/car": true, "trip/1/hotel": true}},
		{"hotel", map[string]bool{"trip/1/flight": false, "trip/1/car": false}},
		{"car", map[string]bool{"trip/1/flight": false}},
		{"flight", map[string]bool{}},
	} {
		for crash := range 6 {
			t.Run(fmt.Sprintf("refuse=%s/crash=%d", c.refuse, crash), func(t *testing.T) {
				dir := t.TempDir()
				world := &ledger{refuse: c.refuse, held: map[string]bool{}}
				s := open(t, dir)
				if err := start(s, "1"); err != nil {
					t.Fatal(err)
				}
				s.Close()
				finished, died := 0, false
				for finished < 6 {
					s = open(t, dir)
					lease := time.Minute
					dying := finished == crash && !died
					if dying {
						lease = 50 * time.Millisecond
					}
					task, err := s.Poll(queue, lease)
					if err != nil || task == nil {
						t.Fatal(finished, task, err)
					}
					out, failure := handle(task, world)
					if failure != "" {
						t.Fatal(failure)
					}
					if dying {
						died = true
						s.Close()
						// Outlive the lease and the backoff that follows it.
						time.Sleep(100 * time.Millisecond)
						s = open(t, dir)
						if task, err = s.Poll(queue, time.Minute); err != nil || task != nil {
							t.Fatal("expected the expiry only", task, err)
						}
						s.Close()
						time.Sleep(150 * time.Millisecond)
						continue
					}
					if err = s.Finish(task.Run, task.Token, out, failure); err != nil {
						t.Fatal(err)
					}
					finished++
					s.Close()
				}
				s = open(t, dir)
				defer s.Close()
				events, err := s.History("trip/1", 0, 1000)
				if err != nil || len(events) == 0 || events[len(events)-1].Kind != "WorkflowCompleted" {
					t.Fatal(events, err)
				}
				if !maps.Equal(world.held, c.want) {
					t.Fatal(world.held)
				}
			})
		}
	}
}
