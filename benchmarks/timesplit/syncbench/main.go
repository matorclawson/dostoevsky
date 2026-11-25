// Command syncbench times append-then-sync alone, with no engine (PLAN-time-split T4). MEASUREMENT BRANCH ONLY.
// "filesync" is Go's File.Sync (what dostoevsky calls outside Linux); "fsync" is the plain system call, which on macOS
// does not flush the drive's cache and must never be shown beside the others without saying so. "preallocated" writes
// into space already filled with zeros and synced, so the file does not grow.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"syscall"
	"time"
)

type mode struct {
	name string
	flag int
	sync func(*os.File) error
}

func main() {
	n := flag.Int("n", 1000, "appends per cell")
	dir := flag.String("dir", os.TempDir(), "directory on the data disk")
	flag.Parse()
	modes := []mode{
		{"filesync", 0, func(f *os.File) error { return f.Sync() }},
		{"fsync", 0, func(f *os.File) error { return syscall.Fsync(int(f.Fd())) }},
	}
	modes = append(modes, platformModes()...)
	failed := false
	for round := 1; round <= 3; round++ {
		for _, size := range []int{512, 4096, 16384} {
			for _, m := range modes {
				for _, pre := range []bool{false, true} {
					line, err := cell(*dir, *n, size, m, pre)
					if err != nil {
						failed = true
						line = "FAILED " + err.Error()
					}
					fmt.Printf("syncbench round=%d os=%s mode=%s bytes=%d preallocated=%t %s\n", round, runtime.GOOS, m.name, size, pre, line)
				}
			}
		}
	}
	if failed {
		os.Exit(1)
	}
}

func cell(dir string, n, size int, m mode, pre bool) (string, error) {
	path := filepath.Join(dir, "syncbench.dat")
	defer os.Remove(path)
	if pre {
		f, err := os.Create(path)
		if err != nil {
			return "", err
		}
		_, err = f.Write(make([]byte, n*size))
		if err == nil {
			err = f.Sync()
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return "", err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|m.flag, 0o600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	took := make([]time.Duration, 0, n)
	start := time.Now()
	for i := range n {
		begin := time.Now()
		if _, err := f.WriteAt(data, int64(i)*int64(size)); err != nil {
			return "", err
		}
		if m.sync != nil {
			if err := m.sync(f); err != nil {
				return "", err
			}
		}
		took = append(took, time.Since(begin))
	}
	total := time.Since(start)
	slices.Sort(took)
	us := func(d time.Duration) float64 { return float64(d) / 1e3 }
	return fmt.Sprintf("n=%d per_s=%.0f p50_us=%.0f p90_us=%.0f p99_us=%.0f", n, float64(n)/total.Seconds(), us(took[n/2]), us(took[n*9/10]), us(took[n*99/100])), nil
}
