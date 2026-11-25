//go:build !linux

package main

// Outside Linux only filesync and fsync are measured.
func platformModes() []mode { return nil }
