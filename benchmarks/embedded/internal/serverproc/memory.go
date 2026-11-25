package serverproc

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// The two ways server memory is measured. Receipts carry the name so a number is never
// read as the other kind:
//
//	VmHWM       Linux: the kernel's own peak resident size of each process, read from procfs.
//	ps-sampled  elsewhere (macOS, the ENG-UK dev track): the current resident size from
//	            `ps -o rss=` at each sample; the peak is the largest sample, so a spike
//	            shorter than the sampling interval can be missed. Not comparable with VmHWM.
const (
	SourceVmHWM     = "VmHWM"
	SourcePSSampled = "ps-sampled"
)

// Source names how this host measures memory.
func Source() string {
	if runtime.GOOS == "linux" {
		return SourceVmHWM
	}
	return SourcePSSampled
}

// checkMeasurable fails when this host cannot report a server's memory: memory is part of
// every server receipt, so such a host fails the trial.
func checkMeasurable() error {
	m, err := snapshotTree(os.Getpid(), Source())
	if err != nil {
		return fmt.Errorf("server memory cannot be measured (%s): %w", Source(), err)
	}
	if m[os.Getpid()] == 0 {
		return fmt.Errorf("server memory cannot be measured (%s): no reading for this process", Source())
	}
	return nil
}

// snapshotTree returns the memory in KiB of root and each of its descendants.
func snapshotTree(root int, source string) (map[int]uint64, error) {
	if source == SourceVmHWM {
		out := map[int]uint64{}
		for _, pid := range tree(root) {
			if kib, ok := vmHWM(pid); ok {
				out[pid] = kib
			}
		}
		if len(out) == 0 {
			return nil, errors.New("procfs is unavailable")
		}
		return out, nil
	}
	raw, err := exec.Command("ps", "-A", "-o", "pid=,ppid=,rss=").Output()
	if err != nil {
		return nil, fmt.Errorf("ps: %w", err)
	}
	parent, rss, err := parsePS(string(raw))
	if err != nil {
		return nil, err
	}
	out := map[int]uint64{}
	for _, pid := range descendants(parent, root) {
		if kib, ok := rss[pid]; ok {
			out[pid] = kib
		}
	}
	return out, nil
}

// parsePS reads the output of `ps -A -o pid=,ppid=,rss=`: three integers per line.
func parsePS(out string) (parent map[int]int, rss map[int]uint64, err error) {
	parent, rss = map[int]int{}, map[int]uint64{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 3 {
			return nil, nil, fmt.Errorf("unexpected ps line %q", line)
		}
		pid, e1 := strconv.Atoi(f[0])
		ppid, e2 := strconv.Atoi(f[1])
		kib, e3 := strconv.ParseUint(f[2], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil {
			return nil, nil, fmt.Errorf("unexpected ps line %q", line)
		}
		parent[pid], rss[pid] = ppid, kib
	}
	if len(parent) == 0 {
		return nil, nil, errors.New("ps listed no processes")
	}
	return parent, rss, nil
}

// descendants returns root and every process below it, given each process's parent.
func descendants(parent map[int]int, root int) []int {
	out := []int{root}
	seen := map[int]bool{root: true}
	for changed := true; changed; {
		changed = false
		for pid, ppid := range parent {
			if !seen[pid] && seen[ppid] {
				seen[pid] = true
				out = append(out, pid)
				changed = true
			}
		}
	}
	return out
}

// parseVmHWM reads the VmHWM line of a /proc/<pid>/status text, in KiB.
func parseVmHWM(status string) (uint64, bool) {
	for _, line := range strings.Split(status, "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			if f := strings.Fields(rest); len(f) >= 1 {
				if kib, err := strconv.ParseUint(f[0], 10, 64); err == nil {
					return kib, true
				}
			}
		}
	}
	return 0, false
}
