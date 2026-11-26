package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// legacyAccepts emulates 0.2.x recover(): it walks complete frames and refuses any
// whose header is not a valid DOST0001 header. It reports the offset it refused at.
func legacyAccepts(t *testing.T, path string) (bool, int64) {
	t.Helper()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return legacyAcceptsBytes(b)
}

func legacyAcceptsBytes(b []byte) (bool, int64) {
	var at int64
	for at+20 <= int64(len(b)) {
		h := b[at : at+20]
		if !bytes.Equal(h[:8], []byte("DOST0001")) || binary.BigEndian.Uint32(h[16:]) != crc32.Checksum(h[:16], crcTable) {
			return false, at
		}
		at += 20 + int64(binary.BigEndian.Uint32(h[8:]))
	}
	return true, at
}

// withLegacyWriter makes this process write 0.2.x frames so a test can build a 0.2.x journal.
func withLegacyWriter(t *testing.T) func() {
	t.Helper()
	old := signature
	signature = signatureV1
	return func() { signature = old }
}

func TestFreshDirectoryIsRefusedByLegacyReaders(t *testing.T) {
	dir := t.TempDir()
	n := openTest(t, dir, config())
	publish(t, n, "a")
	n.Close()
	if ok, at := legacyAccepts(t, filepath.Join(dir, "journal.v1")); ok || at != 0 {
		t.Fatalf("a legacy reader would accept a 0.3.0 journal (refused at %d, accepted=%v)", at, ok)
	}
}

func TestOpeningALegacyJournalFencesIt(t *testing.T) {
	dir := t.TempDir()
	restore := withLegacyWriter(t)
	n := openTest(t, dir, config())
	publish(t, n, "old-1")
	publish(t, n, "old-2")
	n.Close()
	restore()
	path := filepath.Join(dir, "journal.v1")
	if ok, _ := legacyAccepts(t, path); !ok {
		t.Fatal("test fixture is not a legacy journal")
	}
	// Open refuses it, names the backup step and changes nothing.
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Open(dir, config()); !errors.Is(e, ErrUpgradeRequired) || !strings.Contains(e.Error(), "back up") {
		t.Fatalf("a legacy journal was not refused with the backup step: %v", e)
	}
	unchanged(t, path, before)
	// Upgrading keeps a copy, then fences.
	kept, e := UpgradeJournal(dir)
	if e != nil || kept == "" {
		t.Fatal(kept, e)
	}
	unchanged(t, kept, before)
	if again, e := UpgradeJournal(dir); e != nil || again != "" {
		t.Fatalf("a second upgrade did something: %q %v", again, e)
	}
	m, e := Open(dir, config())
	if e != nil {
		t.Fatal(e)
	}
	m.Close()
	if ok, at := legacyAccepts(t, path); ok || at == 0 {
		t.Fatalf("legacy reader would still accept the directory (accepted=%v, refused at %d)", ok, at)
	}
	// Every record written before the fence is intact and the node keeps working.
	m = openTest(t, dir, config())
	for off, id := range []string{"old-1", "old-2"} {
		if r, e := m.Read(ctx, p, int64(off), false); e != nil || r.ID != id {
			t.Fatal(off, r, e)
		}
	}
	if r := publish(t, m, "new"); r.Offset != 2 {
		t.Fatal(r)
	}
}

func TestRetainedDirectoryIsRefusedAfterTwoSwaps(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	for i := 0; i < 2; i++ {
		if e := n.Retain(ctx, map[Partition]int64{p: int64(1 + i)}); e != nil {
			t.Fatal(e)
		}
		publish(t, n, fmt.Sprint("more-", i))
	}
	n.Close()
	if ok, at := legacyAccepts(t, filepath.Join(dir, "journal.v1")); ok || at != 0 {
		t.Fatalf("legacy reader would accept a retained journal (accepted=%v, refused at %d)", ok, at)
	}
	if es, _ := os.ReadDir(dir); len(es) != 2 {
		t.Fatal("unexpected files:", es)
	}
}

// TestFenceFixture builds directories for scripts/fence-check.sh, which opens them with an
// unmodified 0.2.3 binary. It does nothing unless DOST_FENCE_DIR is set.
func TestFenceFixture(t *testing.T) {
	root := os.Getenv("DOST_FENCE_DIR")
	if root == "" {
		t.Skip("fixture for scripts/fence-check.sh")
	}
	c := Config{"app", []string{"app"}, map[string]int{"words": 1}}
	// retained: a fresh 0.3.0 directory swapped twice.
	n, e := Open(filepath.Join(root, "retained"), c)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		for j := 0; j < 3; j++ {
			if _, e = n.Publish(ctx, "words", Message{ID: fmt.Sprint(i, "-", j), Key: "k"}); e != nil {
				t.Fatal(e)
			}
		}
		if e = n.Retain(ctx, map[Partition]int64{{"words", 0}: int64(i + 1)}); e != nil {
			t.Fatal(e)
		}
	}
	n.Close()
	// legacy: a directory written by 0.2.3 (path given), opened once by this build.
	if dir := os.Getenv("DOST_FENCE_LEGACY"); dir != "" {
		if _, e := UpgradeJournal(dir); e != nil {
			t.Fatal(e)
		}
		m, e := Open(dir, c)
		if e != nil {
			t.Fatal(e)
		}
		m.Close()
	}
}

// A stale descriptor to a retired journal must always read a header 0.2.x rejects,
// after each swap and after Close, never an empty file.
func TestRetiredInodeIsFencedNotEmpty(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	path := filepath.Join(dir, "journal.v1")
	stale1, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer stale1.Close()
	read := func(f *os.File, label string) {
		t.Helper()
		info, e := f.Stat()
		if e != nil || info.Size() == 0 {
			t.Fatalf("%s: retired file empty or unreadable (%v)", label, e)
		}
		buf := make([]byte, info.Size())
		if _, e = f.ReadAt(buf, 0); e != nil {
			t.Fatal(label, e)
		}
		if ok, at := legacyAcceptsBytes(buf); ok || at != 0 {
			t.Fatalf("%s: a 0.2.x reader would accept the retired file (accepted=%v at %d)", label, ok, at)
		}
	}
	if e = n.Retain(ctx, map[Partition]int64{p: 1}); e != nil {
		t.Fatal(e)
	}
	read(stale1, "after first swap")
	stale2, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer stale2.Close()
	publish(t, n, "x")
	if e = n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
		t.Fatal(e)
	}
	read(stale1, "after second swap")
	read(stale2, "after second swap")
	n.Close()
	read(stale1, "after Close")
	read(stale2, "after Close")
}

// A V2 header whose payload is torn is not a fence: replay never saw a complete frame.
func TestTornV2FrameDoesNotCountAsFence(t *testing.T) {
	dir := t.TempDir()
	restore := withLegacyWriter(t)
	n := openTest(t, dir, config())
	publish(t, n, "old")
	n.Close()
	restore()
	path := filepath.Join(dir, "journal.v1")
	frame, e := encodeFrame(entry{Fence: 1})
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write(frame[:len(frame)-2]); e != nil { // valid header, payload cut short
		t.Fatal(e)
	}
	f.Close()
	if ok, at := legacyAccepts(t, path); ok || at == 0 {
		t.Fatal("fixture should already show a V2 header", ok, at)
	}
	// An incomplete DOST0002 frame is no upgrade: Open still asks for one and leaves even the torn tail alone.
	torn, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Open(dir, config()); !errors.Is(e, ErrUpgradeRequired) {
		t.Fatal("a torn DOST0002 frame counted as an upgrade:", e)
	}
	unchanged(t, path, torn)
	if _, e = UpgradeJournal(dir); e != nil {
		t.Fatal(e)
	}
	m, e := Open(dir, config())
	if e != nil {
		t.Fatal("reopen after the upgrade:", e)
	}
	m.Close()
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	// The torn tail is gone and a complete fence frame was appended after the legacy frames.
	ok, at := legacyAcceptsBytes(b)
	if ok || at == 0 || !bytes.Equal(b[at:at+8], signature) {
		t.Fatalf("journal not fenced on disk (accepted=%v at %d)", ok, at)
	}
	if int64(len(b)) != at+int64(len(frame)) {
		t.Fatalf("expected exactly one complete fence frame after the legacy frames: %d bytes, fence at %d, frame %d", len(b), at, len(frame))
	}
	r, e := openTest(t, dir, config()).Read(ctx, p, 0, false)
	if e != nil || r.ID != "old" {
		t.Fatal(r, e)
	}
}
