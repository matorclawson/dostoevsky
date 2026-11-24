package serverproc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSamplesATreeAndStops(t *testing.T) {
	if _, err := os.Stat("/proc/self/status"); err != nil {
		t.Skip("needs /proc")
	}
	// sh is the server; its child sleep is a descendant that must be counted.
	p, err := Start("sh", []string{"-c", "sleep 30 & wait"}, nil, filepath.Join(t.TempDir(), "log"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(tree(p.cmd.Process.Pid)) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := len(tree(p.cmd.Process.Pid)); n < 2 {
		t.Fatalf("process tree has %d processes, want the shell and its child", n)
	}
	time.Sleep(100 * time.Millisecond)
	if kib, err := p.PeakKiB(); err != nil || kib == 0 {
		t.Fatalf("no memory sampled: %d KiB, %v", kib, err)
	}
	if err := p.Stop(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if pids := tree(p.cmd.Process.Pid); len(pids) > 1 {
		t.Fatalf("children survived Stop: %v", pids)
	}
}

func TestWaitReadyReportsAnExitedServer(t *testing.T) {
	p, err := Start("sh", []string{"-c", "exit 3"}, nil, filepath.Join(t.TempDir(), "log"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady("tcp", "127.0.0.1:1", 3*time.Second); err == nil {
		t.Fatal("ready reported for a server that exited")
	}
}

// An unmeasured server must fail the trial, never report zero memory.
func TestPeakIsAnErrorWhenTheServerWasNeverMeasured(t *testing.T) {
	p := &Proc{rootPid: 4242, peak: map[int]uint64{}}
	if kib, err := p.PeakKiB(); err == nil {
		t.Fatalf("reported %d KiB for a server that was never measured", kib)
	}
	p.peak[4242] = 100
	p.peak[4243] = 20
	if kib, err := p.PeakKiB(); err != nil || kib != 120 {
		t.Fatalf("sum of per-process peaks = %d, %v; want 120", kib, err)
	}
}
