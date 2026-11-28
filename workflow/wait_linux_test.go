package workflow

import (
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

func parallelWaitJobs(t *testing.T, count int, work func(int) error) {
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < count; i += 32 {
				if err := work(i); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
}

func TestWorkflowWaitingScale(t *testing.T) {
	root := os.Getenv("DOST_WORKFLOW_BENCH_DIR")
	if root == "" {
		t.Skip("set disposable benchmark directory")
	}
	const count = 100000
	const burst = 1024
	if dir := os.Getenv("DOST_WAIT_REOPEN"); dir != "" {
		before, rss := processSnapshot(t)
		at := time.Now()
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		elapsed := time.Since(at)
		after, end := processSnapshot(t)
		if len(s.runs) != count || len(s.queues) != 0 {
			t.Fatal(len(s.runs), len(s.queues))
		}
		for _, r := range s.runs {
			if !s.waiting(r, "approval", 0) {
				t.Fatal(r.start.Run)
			}
		}
		t.Logf("waiting=%d reopen_ms=%.2f retained_heap_bytes_per_run=%.1f rss_before_KiB=%d rss_after_KiB=%d", count, float64(elapsed)/1e6, float64(after-before)/count, rss, end)
		// All burst items arrive together. Queueing for the 32 senders is included.
		latency := make([]time.Duration, burst)
		signalCost := make([]time.Duration, burst)
		started := time.Now()
		readers := 1
		if setting := os.Getenv("DOST_WAIT_READERS"); setting != "" {
			readers, err = strconv.Atoi(setting)
			if err != nil || readers < 1 || readers > burst {
				t.Fatal("invalid DOST_WAIT_READERS")
			}
		}
		type delivery struct {
			id      int
			elapsed time.Duration
			err     error
		}
		deliveries := make(chan delivery, burst+readers)
		var pollers sync.WaitGroup
		for w := 0; w < readers; w++ {
			pollers.Add(1)
			go func(w int) {
				defer pollers.Done()
				deadline := started.Add(time.Minute)
				for n := w; n < burst; {
					if time.Now().After(deadline) {
						deliveries <- delivery{err: fmt.Errorf("lease timeout")}
						return
					}
					task, e := s.Poll("shared", time.Minute)
					if e != nil {
						deliveries <- delivery{err: e}
						return
					}
					if task == nil {
						time.Sleep(100 * time.Microsecond)
						continue
					}
					dt := time.Since(started)
					id, e := strconv.Atoi(task.Run)
					if e != nil || id < 0 || id%97 != 0 || id/97 >= burst || string(task.Input) != "1" {
						deliveries <- delivery{err: fmt.Errorf("unexpected delivery %v", task)}
						return
					}
					deliveries <- delivery{id: id / 97, elapsed: dt}
					n += readers
				}
			}(w)
		}
		parallelWaitJobs(t, burst, func(i int) error {
			at := time.Now()
			err := s.Signal(strconv.Itoa(i*97), 0, "approval", []byte("1"))
			signalCost[i] = time.Since(at)
			return err
		})
		pollers.Wait()
		close(deliveries)
		for v := range deliveries {
			if v.err != nil {
				t.Fatal(v.err)
			}
			if latency[v.id] != 0 {
				t.Fatal("duplicate", v.id)
			}
			latency[v.id] = v.elapsed
		}
		for i, v := range latency {
			if v == 0 {
				t.Fatal("missing", i)
			}
		}
		slices.Sort(signalCost)
		t.Logf("signal_call_to_durable_ack_p99_ms=%.2f", float64(signalCost[(99*burst+99)/100-1])/1e6)
		slices.Sort(latency)
		t.Logf("burst=%d producers=32 polling_workers=%d arrival_to_durable_lease_p50_ms=%.2f p99_ms=%.2f last_ms=%.2f", burst, readers, float64(latency[(burst-1)/2])/1e6, float64(latency[(99*burst+99)/100-1])/1e6, float64(latency[burst-1])/1e6)
		return
	}
	dir, err := os.MkdirTemp(root, "wait-scale-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Register("work", 1, []Step{{Kind: "signal", Name: "approval"}, {Kind: "activity", Name: "resume"}}); err != nil {
		t.Fatal(err)
	}
	parallelWaitJobs(t, count, func(i int) error {
		ok, e := s.StartOnce(strconv.Itoa(i), "work", 1, "shared", []byte("0"), nil)
		if !ok && e == nil {
			return fmt.Errorf("duplicate %d", i)
		}
		return e
	})
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkflowWaitingScale$", "-test.v")
	cmd.Env = append(os.Environ(), "DOST_WAIT_REOPEN="+dir)
	out, err := cmd.CombinedOutput()
	t.Log(string(out))
	if err != nil {
		t.Fatal(err)
	}
}
