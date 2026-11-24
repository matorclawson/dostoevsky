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
	if p.PeakKiB() == 0 {
		t.Fatal("no memory sampled")
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
	if err := p.WaitReady("127.0.0.1:1", 3*time.Second); err == nil {
		t.Fatal("ready reported for a server that exited")
	}
}
