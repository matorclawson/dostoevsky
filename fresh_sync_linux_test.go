package dostoevsky

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// Compare two Linux durability primitives on the requested volume before
// considering a change to the storage contract. The node uses WriteAt+fdatasync.
func TestFreshSyncModes(t *testing.T) {
	root := os.Getenv("DOST_SYNC_MODE_DIR")
	if root == "" {
		t.Skip("supply a disposable directory on the desired volume")
	}
	dir, err := os.MkdirTemp(root, "sync-modes")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	const count = 1024
	for trial := 0; trial < 7; trial++ {
		for j := 0; j < 2; j++ {
			dsync := (trial+j)%2 == 1
			flags := os.O_CREATE | os.O_EXCL | os.O_RDWR
			if dsync {
				flags |= syscall.O_DSYNC
			}
			path := filepath.Join(dir, fmt.Sprintf("%d-%d", trial, j))
			f, e := os.OpenFile(path, flags, 0600)
			if e != nil {
				t.Fatal(e)
			}
			if e = f.Sync(); e != nil {
				t.Fatal(e)
			}
			if e = syncDir(dir); e != nil {
				t.Fatal(e)
			}
			lat := make([]time.Duration, count)
			frame := make([]byte, 1072)
			start := time.Now()
			for i := 0; i < count; i++ {
				frame[0] = byte(i)
				at := time.Now()
				n, e := f.WriteAt(frame, int64(i*len(frame)))
				if e != nil || n != len(frame) {
					t.Fatal(n, e)
				}
				if !dsync {
					if e = syncJournalData(f); e != nil {
						t.Fatal(e)
					}
				}
				lat[i] = time.Since(at)
			}
			elapsed := time.Since(start)
			if e = f.Close(); e != nil {
				t.Fatal(e)
			}
			raw, e := os.ReadFile(path)
			if e != nil || len(raw) != count*len(frame) {
				t.Fatal(len(raw), e)
			}
			for i := 0; i < count; i++ {
				frame[0] = byte(i)
				if !bytes.Equal(frame, raw[i*len(frame):(i+1)*len(frame)]) {
					t.Fatal(i)
				}
			}
			slices.Sort(lat)
			t.Logf("pair=%d dsync=%t rate=%.1f p99_us=%.1f", trial+1, dsync, count/elapsed.Seconds(), float64(lat[(count*99+99)/100-1])/1000)
		}
	}
}
