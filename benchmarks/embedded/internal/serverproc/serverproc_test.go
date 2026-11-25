package serverproc

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	if _, ok := vmHWM(os.Getpid()); !ok {
		t.Skip("needs procfs (Linux); run on the Linux gate host")
	}
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

func needProcfs(t *testing.T) {
	t.Helper()
	if _, ok := vmHWM(os.Getpid()); !ok {
		t.Skip("needs procfs (Linux); run on the Linux gate host")
	}
}

// A shell that traps SIGTERM and exits 143, as a JVM does after a clean shutdown.
const trapsTerm = `trap 'exit 143' TERM; sleep 30 & wait`

func startTrap(t *testing.T, script string) *Proc {
	t.Helper()
	p, err := Start("sh", []string{"-c", script}, nil, filepath.Join(t.TempDir(), "log"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // let the shell install its trap
	return p
}

func TestExit143AfterOurSIGTERMIsSuccessOnlyWhenShutdownIsConfirmedClean(t *testing.T) {
	needProcfs(t)
	if err := startTrap(t, trapsTerm).StopWith(10*time.Second, func() error { return nil }); err != nil {
		t.Fatalf("a confirmed clean exit 143 after our SIGTERM was rejected: %v", err)
	}
	err := startTrap(t, trapsTerm).StopWith(10*time.Second, func() error { return errors.New("no clean-shutdown marker") })
	if err == nil || !strings.Contains(err.Error(), "not confirmed clean") || !strings.Contains(err.Error(), "no clean-shutdown marker") {
		t.Fatalf("an unconfirmed exit 143 was accepted: %v", err)
	}
	// Plain Stop has no confirmation to offer, so 143 stays a failure.
	if err := startTrap(t, trapsTerm).Stop(10 * time.Second); err == nil || !strings.Contains(err.Error(), "143") {
		t.Fatalf("exit 143 without a clean-shutdown check was accepted: %v", err)
	}
}

func TestExit143WithoutOurSIGTERMFails(t *testing.T) {
	needProcfs(t)
	p, err := Start("sh", []string{"-c", "exit 143"}, nil, filepath.Join(t.TempDir(), "log"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // it has exited by itself
	err = p.StopWith(10*time.Second, func() error { return nil })
	if err == nil || !strings.Contains(err.Error(), "on its own") {
		t.Fatalf("an exit 143 that we did not cause was accepted: %v", err)
	}
}

func TestOtherExitStatusesStayFailures(t *testing.T) {
	needProcfs(t)
	err := startTrap(t, `trap 'exit 7' TERM; sleep 30 & wait`).StopWith(10*time.Second, func() error { return nil })
	if err == nil {
		t.Fatal("exit status 7 was accepted")
	}
}

func TestAServerThatIgnoresSIGTERMIsKilledAndFails(t *testing.T) {
	needProcfs(t)
	p := startTrap(t, `trap '' TERM; while :; do sleep 1; done`)
	err := p.StopWith(300*time.Millisecond, func() error { return nil })
	if err == nil || !strings.Contains(err.Error(), "was killed") {
		t.Fatalf("a killed server was reported as a clean stop: %v", err)
	}
}
