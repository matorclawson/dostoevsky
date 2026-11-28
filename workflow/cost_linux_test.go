package workflow

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	d "github.com/rodriveracom/dostoevsky"
)

// Each client uses a private queue so it can time its run through completion.
func TestWorkflowCost(t *testing.T) {
	workflowCost(t, 3)
}

func TestWorkflowLongCost(t *testing.T) { workflowCost(t, 30) }

func workflowCost(t *testing.T, depth int) {
	root := os.Getenv("DOST_WORKFLOW_BENCH_DIR")
	if root == "" {
		t.Skip("set a disposable disk directory")
	}
	for _, workers := range []int{1, 8, 32} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			dir, e := os.MkdirTemp(root, "workflow-cost")
			if e != nil {
				t.Fatal(e)
			}
			defer os.RemoveAll(dir)
			s, e := Open(dir)
			if e != nil {
				t.Fatal(e)
			}
			steps := make([]Step, depth)
			for i := range steps {
				steps[i] = Step{Kind: "activity", Name: string(rune('a' + i%26))}
			}
			if e = s.Register("work", 1, steps); e != nil {
				t.Fatal(e)
			}
			const count = 512
			var groups, records int
			realAppend := s.appendGroup
			s.appendGroup = func(c context.Context, p []d.Publication) ([]d.Record, error) {
				groups++
				records += len(p)
				return realAppend(c, p)
			}
			disk := func() int64 {
				var total int64
				e := filepath.Walk(dir, func(_ string, info os.FileInfo, e error) error {
					if e != nil {
						return e
					}
					if info.Mode().IsRegular() {
						total += info.Size()
					}
					return nil
				})
				if e != nil {
					t.Fatal(e)
				}
				return total
			}
			cpu := func() int64 {
				var r syscall.Rusage
				if e := syscall.Getrusage(syscall.RUSAGE_SELF, &r); e != nil {
					t.Fatal(e)
				}
				return r.Utime.Nano() + r.Stime.Nano()
			}
			before := disk()
			lat := make([]time.Duration, count)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					q := fmt.Sprint(w)
					<-start
					for i := w; i < count; i += workers {
						at := time.Now()
						id := fmt.Sprint(i)
						created, e := s.StartOnce(id, "work", 1, q, []byte("0"), nil)
						if e != nil || !created {
							t.Errorf("start %s: %v", id, e)
							return
						}
						for k := range steps {
							task, e := s.Poll(q, time.Minute)
							if e != nil || task == nil || task.Run != id || task.Step != k {
								t.Errorf("poll %v %v", task, e)
								return
							}
							if e = s.Finish(id, task.Token, []byte("0"), ""); e != nil {
								t.Error(e)
								return
							}
						}
						lat[i] = time.Since(at)
					}
				}(w)
			}
			cpuBefore := cpu()
			begin := time.Now()
			close(start)
			wg.Wait()
			elapsed := time.Since(begin)
			perCPU := float64(cpu()-cpuBefore) / count / 1000
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			if t.Failed() {
				return
			}
			if records != (1+2*depth)*count {
				t.Fatalf("records %d", records)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestWorkflowReopenFootprint$", "-test.v")
			cmd.Env = append(os.Environ(), "DOST_WORKFLOW_REOPEN_CHILD="+dir)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: %v", out, err)
			}
			t.Log(string(out))
			slices.Sort(lat)
			t.Logf("workers=%d completed=%d runs/s=%.1f p99_ms=%.2f records/run=%d append_groups/run=%.3f logical_disk_bytes/run=%.1f cpu_us/run=%.1f", workers, count, count/elapsed.Seconds(), float64(lat[(count*99+99)/100-1])/1e6, records/count, float64(groups)/count, float64(disk()-before)/count, perCPU)
		})
	}
}

// A fresh process distinguishes retained history from allocations left by the seed workload.
func TestWorkflowReopenFootprint(t *testing.T) {
	root := os.Getenv("DOST_WORKFLOW_BENCH_DIR")
	if root == "" {
		t.Skip("set a disposable disk directory")
	}
	if path := os.Getenv("DOST_WORKFLOW_REOPEN_CHILD"); path != "" {

		before, rss := processSnapshot(t)
		at := time.Now()
		s, e := Open(path)
		if e != nil {
			t.Fatal(e)
		}
		elapsed := time.Since(at)
		after, end := processSnapshot(t)
		t.Logf("closed=%d warm_reopen_ms=%.2f retained_heap_delta_KiB=%.1f rss_before_KiB=%d rss_after_KiB=%d", len(s.runs), float64(elapsed)/1e6, float64(int64(after)-int64(before))/1024, rss, end)
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		return
	}
	dir, e := os.MkdirTemp(root, "workflow-memory")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	prior := 0
	for _, count := range []int{0, 256, 4096} {
		s := setupForMemory(t, dir)
		var wg sync.WaitGroup
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				q := fmt.Sprint(w)
				for i := prior + w; i < count; i += 16 {
					id := fmt.Sprint(i)
					if ok, e := s.StartOnce(id, "work", 1, q, []byte("0"), nil); e != nil || !ok {
						t.Error(id, e)
						return
					}
					task, e := s.Poll(q, time.Minute)
					if e != nil || task == nil || task.Run != id {
						t.Error(id, task, e)
						return
					}
					if e = s.Finish(id, task.Token, []byte("1"), ""); e != nil {
						t.Error(e)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		if t.Failed() {
			return
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestWorkflowReopenFootprint$", "-test.v")
		cmd.Env = append(os.Environ(), "DOST_WORKFLOW_REOPEN_CHILD="+dir)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("%s %v", out, e)
		}
		t.Log(string(out))
		prior = count
	}
}
func setupForMemory(t *testing.T, dir string) *Store {
	t.Helper()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Register("work", 1, []Step{{Kind: "activity", Name: "a"}}); e != nil {
		t.Fatal(e)
	}
	return s
}

func processSnapshot(t *testing.T) (uint64, int) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	b, e := os.ReadFile("/proc/self/status")
	if e != nil {
		t.Fatal(e)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			v, e := strconv.Atoi(strings.Fields(line)[1])
			if e != nil {
				t.Fatal(e)
			}
			return m.HeapAlloc, v
		}
	}
	t.Fatal("missing RSS")
	return 0, 0
}
