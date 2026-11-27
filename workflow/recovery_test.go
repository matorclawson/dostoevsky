//go:build unix

package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

func setup(t *testing.T, path string) *Store {
	t.Helper()
	s, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Register("work", 1, []Step{{Kind: "activity", Name: "send", Attempts: 2}}); e != nil {
		t.Fatal(e)
	}
	return s
}
func TestAbruptExit(t *testing.T) {
	if dir := os.Getenv("WORKFLOW_KILL_STORE"); dir != "" {
		s := setup(t, dir)
		op := os.Getenv("WORKFLOW_KILL_OP")
		kill := func() {
			if e := syscall.Kill(os.Getpid(), syscall.SIGKILL); e != nil {
				panic(e)
			}
		}
		// Hook the boundary while the writer is idle.
		if os.Getenv("WORKFLOW_KILL_AT") == "after-sync" {
			s.afterSync = kill
		} else {
			s.appendGroup = func(context.Context, []d.Publication) ([]d.Record, error) { kill(); panic("kill returned") }
		}
		switch op {
		case "start":
			s.StartOnce("run", "work", 1, "q", []byte("0"), nil)
		case "poll":
			s.Poll("q", time.Hour)
		case "finish", "failure":
			failure := ""
			if op == "failure" {
				failure = "busy"
			}
			s.Finish("run", os.Getenv("WORKFLOW_TOKEN"), []byte("1"), failure)
		case "expire":
			s.Poll("q", time.Hour)
		}
		t.Fatal("kill point missed")
	}
	for _, at := range []string{"before-write", "after-sync"} {
		for _, op := range []string{"start", "poll", "finish", "failure", "expire"} {
			t.Run(at+"/"+op, func(t *testing.T) {
				dir := t.TempDir()
				s := setup(t, dir)
				token := ""
				if op != "start" {
					_, e := s.StartOnce("run", "work", 1, "q", []byte("0"), nil)
					if e != nil {
						t.Fatal(e)
					}
				}
				if op == "finish" || op == "failure" || op == "expire" {
					duration := time.Hour
					if op == "expire" {
						duration = time.Millisecond
					}
					task, e := s.Poll("q", duration)
					if e != nil || task == nil {
						t.Fatal(task, e)
					}
					token = task.Token
				}
				if e := s.Close(); e != nil {
					t.Fatal(e)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestAbruptExit$")
				cmd.Env = append(os.Environ(), "WORKFLOW_KILL_STORE="+dir, "WORKFLOW_KILL_AT="+at, "WORKFLOW_KILL_OP="+op, "WORKFLOW_TOKEN="+token)
				out, e := cmd.CombinedOutput()
				var ex *exec.ExitError
				if !errors.As(e, &ex) || ex.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
					t.Fatal(e, string(out))
				}
				s = setup(t, dir)
				defer s.Close()
				after := at == "after-sync"
				switch op {
				case "start":
					made, e := s.StartOnce("run", "work", 1, "q", []byte("0"), nil)
					if e != nil || made == after {
						t.Fatal(made, e)
					}
				case "poll":
					task, e := s.Poll("q", time.Minute)
					if e != nil || (task == nil) != after {
						t.Fatal(task, e)
					}
				case "finish", "failure":
					failure := ""
					if op == "failure" {
						failure = "busy"
					}
					e := s.Finish("run", token, []byte("1"), failure)
					if (after && !errors.Is(e, ErrStale)) || (!after && e != nil) {
						t.Fatal(e)
					}
				case "expire":
					ev, e := s.History("run", 0, 20)
					if e != nil {
						t.Fatal(e)
					}
					n := 0
					for _, v := range ev {
						if v.Kind == "ActivityTimedOut" {
							n++
						}
					}
					if (n == 1) != after {
						t.Fatal(ev)
					}
				}
			})
		}
	}
}

func TestGroupedConflicts(t *testing.T) {
	s := setup(t, t.TempDir())
	defer s.Close()
	// Hold the writer inside an earlier sync so all four commands enter one group.
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	realAppend := s.appendGroup
	sizes := make(chan int, 5)
	s.appendGroup = func(c context.Context, p []d.Publication) ([]d.Record, error) {
		once.Do(func() { close(entered); <-release })
		sizes <- len(p)
		return realAppend(c, p)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, e := s.StartOnce("race", "work", 1, "q", []byte("0"), nil)
		if e != nil {
			t.Error(e)
		}
	}()
	<-entered
	tasks := make(chan *Task, 2)
	for i := 0; i < 2; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			v, e := s.Poll("q", time.Hour)
			if e != nil {
				t.Error(e)
			}
			tasks <- v
		}()
		go func(i int) {
			defer wg.Done()
			_, e := s.StartOnce(fmt.Sprint(i), "work", 1, "other", []byte("0"), nil)
			if e != nil {
				t.Error(e)
			}
		}(i)
	}
	deadline := time.Now().Add(time.Second)
	for len(s.calls) != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	if len(sizes) != 2 || <-sizes != 1 || <-sizes != 3 {
		t.Fatal("unexpected append groups")
	}
	a, b := <-tasks, <-tasks
	if (a == nil) == (b == nil) {
		t.Fatal(a, b)
	}
}
func TestFenceAfterUncertainWrite(t *testing.T) {
	dir := t.TempDir()
	s := setup(t, dir)
	injected := errors.New("uncertain sync")
	s.appendGroup = func(context.Context, []d.Publication) ([]d.Record, error) { return nil, injected }
	made, e := s.StartOnce("r", "work", 1, "q", []byte("0"), nil)
	if made || e != injected {
		t.Fatal(made, e)
	}
	if ev, e := s.History("r", 0, 10); ev != nil || e != injected {
		t.Fatal(ev, e)
	}
	s.Close()
	s = setup(t, dir)
	defer s.Close()
	if made, e = s.StartOnce("r", "work", 1, "q", []byte("0"), nil); !made || e != nil {
		t.Fatal(made, e)
	}
}

// Replay byte images of a group interrupted after a valid first transition.
// Length-preserving damage is intentionally fail-closed in the current store.
func TestWorkflowGroupCrashImages(t *testing.T) {
	for _, damage := range []bool{false, true} {
		t.Run(fmt.Sprint("damaged=", damage), func(t *testing.T) {
			dir := t.TempDir()
			s := setup(t, dir)
			realAppend := s.appendGroup
			var captured []d.Publication
			s.appendGroup = func(c context.Context, pubs []d.Publication) ([]d.Record, error) {
				captured = append(captured, pubs...)
				return realAppend(c, pubs)
			}
			for _, id := range []string{"one", "two"} {
				if _, e := s.StartOnce(id, "work", 1, "q", []byte("0"), nil); e != nil {
					t.Fatal(e)
				}
			}
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			// Build a separate store, then use the actual storage batch encoder for both records.
			replica := t.TempDir()
			ss := setup(t, replica)
			if e := ss.Close(); e != nil {
				t.Fatal(e)
			}
			n, e := d.OpenSegmented(replica, d.Config{Node: "workflow", Nodes: []string{"workflow"}, Topics: map[string]int{topic: 1}})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = n.PublishBatch(context.Background(), captured); e != nil {
				t.Fatal(e)
			}
			if e = n.Close(); e != nil {
				t.Fatal(e)
			}
			files, e := filepath.Glob(filepath.Join(replica, "segments", "seg-*.log"))
			if e != nil || len(files) != 1 {
				t.Fatal(files, e)
			}
			b, e := os.ReadFile(files[0])
			if e != nil {
				t.Fatal(e)
			}
			if damage {
				b[len(b)-1] ^= 1
			} else {
				b = b[:len(b)-1]
			}
			if e = os.WriteFile(files[0], b, 0600); e != nil {
				t.Fatal(e)
			}
			ss, e = Open(replica)
			if damage {
				if e == nil {
					ss.Close()
					t.Fatal("damaged complete frame accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer ss.Close()
			for _, id := range []string{"one", "two"} {
				made, e := ss.StartOnce(id, "work", 1, "q", []byte("0"), nil)
				if e != nil || made != (id == "two") {
					t.Fatal(id, made, e)
				}
			}
		})
	}
}
