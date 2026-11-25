//go:build linux

package main

import (
	"os"
	"syscall"
)

func platformModes() []mode {
	return []mode{
		{"fdatasync", 0, func(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }},
		{"o_dsync", syscall.O_DSYNC, nil},
	}
}
