// Package serverproc starts one server process for a benchmark trial and
// measures its memory. It is the same for every server engine, so every engine
// is measured the same way.
package serverproc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Proc is a running server and its sampler.
type Proc struct {
	cmd     *exec.Cmd
	rootPid int
	stop    chan struct{}
	done    chan struct{}
	exit    chan error

	mu   sync.Mutex
	peak map[int]uint64 // pid -> highest VmHWM seen, in KiB
}

// Start runs bin with args, in its own process group, with stdout and stderr
// appended to logPath, and starts sampling its process tree every interval.
func Start(bin string, args []string, env []string, logPath string, interval time.Duration) (*Proc, error) {
	// Memory is part of every server receipt, so a host that cannot report it fails the trial.
	if _, ok := vmHWM(os.Getpid()); !ok {
		return nil, errors.New("procfs is unavailable: server memory cannot be measured")
	}
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer log.Close()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	p := &Proc{cmd: cmd, rootPid: cmd.Process.Pid, stop: make(chan struct{}), done: make(chan struct{}), exit: make(chan error, 1), peak: map[int]uint64{}}
	go func() { p.exit <- cmd.Wait() }()
	go p.sample(interval)
	return p, nil
}

// FreePort returns a loopback TCP port that was free a moment ago. Another
// process could still take it before the server binds, which makes the server
// fail to start; the harness then reports a failed trial.
func FreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Pid is the launched server's process ID.
func (p *Proc) Pid() int { return p.cmd.Process.Pid }

// WaitReady polls addr on network ("tcp" or "unix") until it accepts a
// connection, the server exits or the timeout passes. The caller must also check
// that the process answering is the launched one (see Pid).
func (p *Proc) WaitReady(network, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.exit:
			p.exit <- err
			return fmt.Errorf("server exited before it was ready: %v", err)
		default:
		}
		if c, err := net.DialTimeout(network, addr, 200*time.Millisecond); err == nil {
			c.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("server not ready on %s after %v", addr, timeout)
}

// Stop asks the whole process group to terminate and kills it after grace.
func (p *Proc) Stop(grace time.Duration) error { return p.StopWith(grace, nil) }

// exitSIGTERM is the status a JVM reports after it trapped SIGTERM and finished a
// clean shutdown: 128 plus the signal number.
const exitSIGTERM = 128 + int(syscall.SIGTERM)

// StopWith is Stop for a server that traps SIGTERM and exits 143 after shutting
// down. Exit 143 counts as success only when all of these hold: the process was
// still running when this call signaled it, it exited within the grace period
// without being killed, and cleanShutdown (when given) confirms from the server's
// own state that the shutdown was clean. A SIGTERM death, as before, is success.
// Any other status, an exit 143 that nobody asked for, or a failed confirmation
// is an error.
func (p *Proc) StopWith(grace time.Duration, cleanShutdown func() error) error {
	pgid := p.cmd.Process.Pid
	var err error
	exitedFirst := false
	select {
	case err = <-p.exit:
		exitedFirst = true // it was already gone: nothing of ours stopped it
	default:
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	}
	killed := false
	if !exitedFirst {
		select {
		case err = <-p.exit:
		case <-time.After(grace):
			killed = true
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			err = <-p.exit
			err = errors.Join(errors.New("server did not stop within the grace period and was killed"), err)
		}
	}
	p.sampleOnce()
	close(p.stop)
	<-p.done
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			// A server stopped by our SIGTERM may report a signal; that is not a failure.
			if ws.Signaled() && ws.Signal() == syscall.SIGTERM && !exitedFirst {
				return nil
			}
			if ws.Exited() && ws.ExitStatus() == exitSIGTERM {
				switch {
				case exitedFirst:
					return fmt.Errorf("server exited with status %d on its own, before it was stopped: %w", exitSIGTERM, err)
				case killed:
					return err
				case cleanShutdown == nil:
					return fmt.Errorf("server exited with status %d after SIGTERM and no clean-shutdown check was given: %w", exitSIGTERM, err)
				}
				if cerr := cleanShutdown(); cerr != nil {
					return fmt.Errorf("server exited with status %d after SIGTERM but its shutdown is not confirmed clean: %w", exitSIGTERM, cerr)
				}
				return nil
			}
		}
	}
	return err
}

// PeakKiB returns the sum, over every process the server tree ever contained,
// of that process's highest sampled VmHWM. It is a sampled peak: a spike
// shorter than the sampling interval in a child process can be missed, the
// per-process peaks may occur at different times, and shared pages are counted
// once per process. It fails, rather than reporting zero, when the server
// process itself was never measured.
func (p *Proc) PeakKiB() (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peak[p.rootPid] == 0 {
		return 0, fmt.Errorf("server memory unavailable: no VmHWM was ever read for pid %d", p.rootPid)
	}
	var sum uint64
	for _, v := range p.peak {
		sum += v
	}
	return sum, nil
}

func (p *Proc) sample(interval time.Duration) {
	defer close(p.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		p.sampleOnce()
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
	}
}

func (p *Proc) sampleOnce() {
	for _, pid := range tree(p.cmd.Process.Pid) {
		if kib, ok := vmHWM(pid); ok {
			p.mu.Lock()
			if kib > p.peak[pid] {
				p.peak[pid] = kib
			}
			p.mu.Unlock()
		}
	}
}

// tree returns root and all of its descendants, read from /proc.
func tree(root int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return []int{root}
	}
	parent := map[int]int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		// pid (comm) state ppid ...: comm can contain spaces and parentheses.
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		fields := strings.Fields(s[i+1:])
		if len(fields) < 2 {
			continue
		}
		if ppid, err := strconv.Atoi(fields[1]); err == nil {
			parent[pid] = ppid
		}
	}
	out := []int{root}
	for changed := true; changed; {
		changed = false
		for pid, ppid := range parent {
			if pid == root {
				continue
			}
			for _, have := range out {
				if have == ppid && !contains(out, pid) {
					out = append(out, pid)
					changed = true
					break
				}
			}
		}
	}
	return out
}

func contains(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// vmHWM reads the peak resident set size of pid in KiB.
func vmHWM(pid int) (uint64, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			f := strings.Fields(rest)
			if len(f) >= 1 {
				if kib, err := strconv.ParseUint(f[0], 10, 64); err == nil {
					return kib, true
				}
			}
		}
	}
	return 0, false
}
