package serverproc

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// livePids lists the running members of the tree below root, measured the way this host
// measures memory (procfs on Linux, ps elsewhere).
func livePids(t *testing.T, root int) []int {
	t.Helper()
	m, err := snapshotTree(root, Source())
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for pid := range m {
		out = append(out, pid)
	}
	return out
}

func TestSamplesATreeAndStops(t *testing.T) {
	// sh is the server; its child sleep is a descendant that must be counted.
	p, err := Start("sh", []string{"-c", "sleep 30 & wait"}, nil, filepath.Join(t.TempDir(), "log"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(livePids(t, p.cmd.Process.Pid)) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := len(livePids(t, p.cmd.Process.Pid)); n < 2 {
		t.Fatalf("process tree has %d processes, want the shell and its child", n)
	}
	time.Sleep(100 * time.Millisecond)
	if kib, err := p.PeakKiB(); err != nil || kib == 0 {
		t.Fatalf("no memory sampled: %d KiB, %v", kib, err)
	}
	if err := p.Stop(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if pids := livePids(t, p.cmd.Process.Pid); len(pids) > 1 {
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

// needProcfs is kept as a guard that FAILS, never skips, when the host cannot measure memory.
func needProcfs(t *testing.T) {
	t.Helper()
	if err := checkMeasurable(); err != nil {
		t.Fatal(err)
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

// exitErrorOf runs a shell command and returns the *exec.ExitError it produces.
func exitErrorOf(t *testing.T, script string) error {
	t.Helper()
	err := exec.Command("sh", "-c", script).Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("%q did not produce an exit error: %v", script, err)
	}
	return err
}

// Judging the leader's exit, separately from cleanup.
func TestJudgeExit(t *testing.T) {
	exit143 := exitErrorOf(t, "exit 143")
	termDeath := exitErrorOf(t, "kill -TERM $$")
	ok := func() error { return nil }
	cases := []struct {
		name                       string
		err                        error
		exitedFirst, delivered, kl bool
		clean                      func() error
		wantErr                    string // "" means success
	}{
		{"143 after delivered SIGTERM and clean shutdown", exit143, false, true, false, ok, ""},
		{"143 when our SIGTERM was not delivered (ESRCH)", exit143, false, false, false, ok, "not delivered"},
		{"143 on its own", exit143, true, false, false, ok, "on its own"},
		{"143 unconfirmed", exit143, false, true, false, func() error { return errors.New("no marker") }, "not confirmed clean"},
		{"143 without a check", exit143, false, true, false, nil, "no clean-shutdown check"},
		{"SIGTERM death after delivery", termDeath, false, true, false, nil, ""},
		{"SIGTERM death without delivery", termDeath, false, false, false, nil, "signal: terminated"},
		// The leader died of the SIGTERM at the same moment the grace period expired and we killed
		// the group: the recorded kill must stand, whatever status the wait reported.
		{"a recorded kill is not cleared by a late SIGTERM status", errors.Join(errors.New("was killed"), termDeath), false, true, true, ok, "was killed"},
		{"a recorded kill is not cleared by a late 143", errors.Join(errors.New("was killed"), exit143), false, true, true, ok, "was killed"},
		{"a clean self-exit is not judged here", nil, true, false, false, nil, ""},
	}
	for _, c := range cases {
		err := judgeExit(c.err, c.exitedFirst, c.delivered, c.kl, c.clean)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: error %v, want one containing %q", c.name, err, c.wantErr)
		}
	}
}

// Cleanup, separately from judging: a child that outlives the leader is gone after
// StopWith, even though the leader had already exited by itself.
func TestStopCleansUpGroupMembersThatOutliveTheLeader(t *testing.T) {
	needProcfs(t)
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := fmt.Sprintf("sleep 60 & echo $! > '%s'; exit 0", pidFile)
	p, err := Start("sh", []string{"-c", script}, nil, filepath.Join(dir, "log"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var child int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(pidFile); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			fmt.Sscan(strings.TrimSpace(string(b)), &child)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if child == 0 {
		t.Fatal("the child never started")
	}
	time.Sleep(300 * time.Millisecond) // the leader has exited; the child lives on
	if err := syscall.Kill(child, 0); err != nil {
		t.Fatalf("the child is already gone before Stop: %v", err)
	}
	_ = p.StopWith(10*time.Second, nil) // the judgment of the leader is not what this test is about
	gone := false
	for i := 0; i < 100 && !gone; i++ {
		// A killed child can linger as a zombie until init reaps it; ESRCH or a zombie both mean it is dead.
		if err := syscall.Kill(child, 0); err != nil {
			gone = true
		} else if out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(child)).Output(); err != nil || strings.HasPrefix(strings.TrimSpace(string(out)), "Z") {
			// ps works on Linux and macOS alike: no such process, or a zombie, both mean dead.
			gone = true
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !gone {
		t.Fatal("a group member that outlived the leader survived Stop")
	}
}

func TestPSOutputIsParsedAndTheTreeWalked(t *testing.T) {
	out := "  1     0  100\n 10     1  2000\n 11    10  3000\n 12    11  40\n 99     1  7\n"
	parent, rss, err := parsePS(out)
	if err != nil {
		t.Fatal(err)
	}
	got := descendants(parent, 10)
	if len(got) != 3 {
		t.Fatalf("tree of 10 = %v, want 10, 11, 12", got)
	}
	for _, pid := range got {
		if pid == 99 || pid == 1 {
			t.Fatalf("unrelated process %d in the tree", pid)
		}
	}
	if rss[11] != 3000 {
		t.Fatalf("rss of 11 = %d", rss[11])
	}
	for _, bad := range []string{"", "1 2\n", "1 2 x\n", "a b c\n"} {
		if _, _, err := parsePS(bad); err == nil {
			t.Errorf("ps output %q was accepted", bad)
		}
	}
}

func TestVmHWMIsParsedFromAStatusFile(t *testing.T) {
	status := "Name:\tserver\nVmPeak:\t  9999 kB\nVmHWM:\t  12345 kB\nVmRSS:\t  100 kB\n"
	if kib, ok := parseVmHWM(status); !ok || kib != 12345 {
		t.Fatalf("VmHWM = %d, %v", kib, ok)
	}
	if _, ok := parseVmHWM("Name:\tx\n"); ok {
		t.Fatal("a status without VmHWM was accepted")
	}
}

// Both sources are exercised on every host: the real ps, and (where it exists) procfs.
func TestEachSourceMeasuresThisProcess(t *testing.T) {
	self := os.Getpid()
	m, err := snapshotTree(self, SourcePSSampled)
	if err != nil || m[self] == 0 {
		t.Fatalf("ps-sampled reading of this process: %v, %v", m, err)
	}
	if runtime.GOOS == "linux" {
		m, err := snapshotTree(self, SourceVmHWM)
		if err != nil || m[self] == 0 {
			t.Fatalf("VmHWM reading of this process: %v, %v", m, err)
		}
	}
	want := SourcePSSampled
	if runtime.GOOS == "linux" {
		want = SourceVmHWM
	}
	if Source() != want {
		t.Fatalf("Source() = %s on %s", Source(), runtime.GOOS)
	}
}
