package main

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/workflow"
)

// ledger is the outside world. It survives our restarts and deduplicates by key.
type ledger struct {
	refuse string
	held   map[string]bool
	calls  []string
}

func (l *ledger) Book(key, item string) error {
	l.calls = append(l.calls, "book "+item)
	if item == l.refuse {
		return errRefused
	}
	// A key that was canceled stays canceled.
	if _, seen := l.held[key]; !seen {
		l.held[key] = true
	}
	return nil
}

func (l *ledger) Cancel(key, item string) error {
	l.calls = append(l.calls, "cancel "+item)
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
// the effect and before Finish. calls is the outside call each step makes, so a
// rollback must cancel in reverse order of booking.
func TestCrashAndResume(t *testing.T) {
	for _, c := range []struct {
		refuse string
		want   map[string]bool
		calls  [6]string
	}{
		{"", map[string]bool{"trip/1/flight": true, "trip/1/car": true, "trip/1/hotel": true}, [6]string{"book flight", "book car", "book hotel"}},
		{"hotel", map[string]bool{"trip/1/flight": false, "trip/1/car": false}, [6]string{"book flight", "book car", "book hotel", "", "cancel car", "cancel flight"}},
		{"car", map[string]bool{"trip/1/flight": false}, [6]string{"book flight", "book car", "", "", "", "cancel flight"}},
		{"flight", map[string]bool{}, [6]string{"book flight"}},
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
				finished, died, lost := 0, false, ""
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
					if died && finished == crash && (task.Activity != lost || task.Attempt != 2) {
						t.Fatal("retry is another activity", task, lost)
					}
					before := len(world.calls)
					out, failure := handle(task, world)
					if failure != "" {
						t.Fatal(failure)
					}
					made := strings.Join(world.calls[before:], ",")
					if made != c.calls[finished] {
						t.Fatalf("step %d called %q, want %q", finished, made, c.calls[finished])
					}
					if dying {
						died, lost = true, task.Activity
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
