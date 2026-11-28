//go:build unix

package dostoevsky

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestWorkflowWitnessKillAtRollover(t *testing.T) {
	put := func(l *WorkflowLog, s string) error {
		_, e := l.PublishBatch(context.Background(), []Publication{{Topic: "transitions", Message: Message{Value: []byte(s)}}})
		return e
	}
	if root := os.Getenv("DOST_WITNESS_CHILD"); root != "" {
		l, e := OpenWorkflowLog(root)
		if e != nil {
			t.Fatal(e)
		}
		l.n.rollBytes = 1
		l.persist = func(dir string, b []byte) error {
			if os.Getenv("DOST_WITNESS_PHASE") == "published" {
				if e := publishWitness(dir, b); e != nil {
					return e
				}
			}
			if e := syscall.Kill(os.Getpid(), syscall.SIGKILL); e != nil {
				return e
			}
			// SIGKILL need not stop this thread before its next instruction: wait for it,
			// so nothing past the kill point can run.
			for {
				time.Sleep(time.Second)
			}
		}
		if e = put(l, "lost-reply"); e != nil {
			t.Fatal(e)
		}
		t.Fatal("child survived")
	}
	for _, phase := range []string{"data-only", "published"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			l, e := OpenWorkflowLog(dir)
			if e != nil {
				t.Fatal(e)
			}
			if e = put(l, "ack"); e != nil {
				t.Fatal(e)
			}
			l.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestWorkflowWitnessKillAtRollover$")
			cmd.Env = append(os.Environ(), "DOST_WITNESS_CHILD="+dir, "DOST_WITNESS_PHASE="+phase)
			out, e := cmd.CombinedOutput()
			var ex *exec.ExitError
			if !errors.As(e, &ex) || ex.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("%s: %v", out, e)
			}
			l, e = OpenWorkflowLog(dir)
			if e != nil {
				t.Fatal(e)
			}
			defer l.Close()
			pg, e := l.ReadBatch(context.Background(), Partition{Topic: "transitions"}, 0, Limits{3, 128}, false)
			if e != nil {
				t.Fatal(e)
			}
			want := 1
			if phase == "published" {
				want = 2
			}
			if len(pg.Records) != want || string(pg.Records[0].Value) != "ack" || want == 2 && string(pg.Records[1].Value) != "lost-reply" {
				t.Fatal(pg)
			}
		})
	}
}
