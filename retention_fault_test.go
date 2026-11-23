package dostoevsky

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

var errInjected = errors.New("injected fault")

// seedRetention publishes a, b, c, d and leaves group g at Next 2 with state "s2".
func seedRetention(t *testing.T, dir string) *Node {
	t.Helper()
	n := openTest(t, dir, config())
	for _, id := range []string{"a", "b", "c", "d"} {
		publish(t, n, id)
	}
	if e := n.Commit(ctx, p, "g", 0, []byte("s1")); e != nil {
		t.Fatal(e)
	}
	if e := n.Commit(ctx, p, "g", 1, []byte("s2")); e != nil {
		t.Fatal(e)
	}
	return n
}

func withFault(t *testing.T, hook func(stage string) error) {
	t.Helper()
	old := retentionFault
	retentionFault = hook
	t.Cleanup(func() { retentionFault = old })
}

func leftovers(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	es, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range es {
		if strings.HasPrefix(f.Name(), ".retention-") {
			out = append(out, f.Name())
		}
	}
	return out
}

func snapshotDir(t *testing.T, from string) string {
	t.Helper()
	to := t.TempDir()
	es, e := os.ReadDir(from)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range es {
		b, e := os.ReadFile(filepath.Join(from, f.Name()))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(to, f.Name()), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	return to
}

func wantOldHistory(t *testing.T, n *Node, label string) {
	t.Helper()
	for off, id := range []string{"a", "b", "c", "d"} {
		if r, e := n.Read(ctx, p, int64(off), false); e != nil || r.ID != id {
			t.Fatalf("%s: offset %d: %v %v", label, off, r, e)
		}
	}
	if pos, e := n.Position(ctx, p, "g"); e != nil || pos.Next != 2 || string(pos.State) != "s2" {
		t.Fatalf("%s: checkpoint %v %v", label, pos, e)
	}
}

func wantRetained(t *testing.T, n *Node, label string) {
	t.Helper()
	for off := int64(0); off < 2; off++ {
		if _, e := n.Read(ctx, p, off, false); e != ErrExpired {
			t.Fatalf("%s: offset %d: %v", label, off, e)
		}
	}
	for off, id := range map[int64]string{2: "c", 3: "d"} {
		if r, e := n.Read(ctx, p, off, false); e != nil || r.ID != id {
			t.Fatalf("%s: offset %d: %v %v", label, off, r, e)
		}
	}
	if pos, e := n.Position(ctx, p, "g"); e != nil || pos.Next != 2 || string(pos.State) != "s2" {
		t.Fatalf("%s: checkpoint %v %v", label, pos, e)
	}
}

// A failure before the rename leaves the old journal in place and the node usable.
func TestRetentionFaultBeforeRename(t *testing.T) {
	cases := []struct {
		name  string
		stage string
		nth   int
	}{
		{"first temp write", "write", 1},
		{"later temp write", "write", 4},
		{"temp file sync", "sync", 1},
		{"rename", "rename", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			n := seedRetention(t, dir)
			calls := 0
			withFault(t, func(stage string) error {
				if stage == c.stage {
					if calls++; calls == c.nth {
						return errInjected
					}
				}
				return nil
			})
			if e := n.Retain(ctx, map[Partition]int64{p: 2}); !errors.Is(e, errInjected) {
				t.Fatal("fault not reported:", e)
			}
			if l := leftovers(t, dir); len(l) != 0 {
				t.Fatal("temp file left behind:", l)
			}
			wantOldHistory(t, n, "same process")
			if r := publish(t, n, "e"); r.Offset != 4 {
				t.Fatal(r)
			}
			n.Close()
			m := openTest(t, dir, config())
			if r, e := m.Read(ctx, p, 4, false); e != nil || r.ID != "e" {
				t.Fatal(r, e)
			}
			// The same request succeeds once the fault clears.
			withFault(t, func(string) error { return nil })
			if e := m.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
				t.Fatal(e)
			}
			if _, e := m.Read(ctx, p, 1, false); e != ErrExpired {
				t.Fatal(e)
			}
		})
	}
}

// A failure of the final directory sync happens after the rename: the retention
// took effect even though Retain reports an error.
func TestRetentionFaultDirectorySync(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	withFault(t, func(stage string) error {
		if stage == "dirsync" {
			return errInjected
		}
		return nil
	})
	if e := n.Retain(ctx, map[Partition]int64{p: 2}); !errors.Is(e, errInjected) {
		t.Fatal(e)
	}
	wantRetained(t, n, "same process")
	// The directory entry may not be durable: no further write may be acknowledged.
	if _, e := n.Publish(ctx, "words", Message{ID: "late"}); !errors.Is(e, errInjected) {
		t.Fatal("write acknowledged after a failed directory sync:", e)
	}
	if e := n.Commit(ctx, p, "g", 2, nil); !errors.Is(e, errInjected) {
		t.Fatal("commit acknowledged after a failed directory sync:", e)
	}
	if e := n.Retain(ctx, map[Partition]int64{p: 2}); !errors.Is(e, errInjected) {
		t.Fatal("retain ran on a failed journal:", e)
	}
	n.Close()
	withFault(t, func(string) error { return nil })
	m := openTest(t, dir, config())
	wantRetained(t, m, "after reopen")
	if r := publish(t, m, "late"); r.Offset != 4 {
		t.Fatal(r)
	}
}

// Copy the data directory at each durable step, as a crash would leave it, and open the copy.
func TestRetentionCrashPoints(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	snaps := map[string]string{}
	writes := 0
	withFault(t, func(stage string) error {
		label := stage
		if stage == "write" {
			writes++
			label = "write-" + string(rune('0'+writes))
		}
		snaps[label] = snapshotDir(t, dir)
		return nil
	})
	if e := n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
		t.Fatal(e)
	}
	for _, need := range []string{"write-1", "sync", "rename", "dirsync"} {
		if snaps[need] == "" {
			t.Fatalf("crash point %s not exercised (%v)", need, len(snaps))
		}
	}
	for label, snap := range snaps {
		m, e := Open(snap, config())
		if e != nil {
			t.Fatalf("crash at %s: reopen failed: %v", label, e)
		}
		if label == "dirsync" {
			wantRetained(t, m, "crash at "+label)
		} else {
			wantOldHistory(t, m, "crash at "+label)
		}
		if r, e := m.Publish(ctx, "words", Message{ID: "post", Key: "k"}); e != nil || r.Offset != 4 {
			t.Fatalf("crash at %s: publish after recovery %v %v", label, r, e)
		}
		m.Close()
	}
}

func TestRetentionGroupsFloorsAndIDs(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	before := filepath.Join(t.TempDir(), "before")
	if e := n.Backup(ctx, before); e != nil {
		t.Fatal(e)
	}
	// A group that never committed starts at offset 0, below the floor.
	if e := n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
		t.Fatal(e)
	}
	if pos, e := n.Position(ctx, p, "late"); e != nil || pos.Next != 0 {
		t.Fatal(pos, e)
	}
	if _, e := n.Read(ctx, p, 0, false); e != ErrExpired {
		t.Fatal("new group below the floor must get ErrExpired:", e)
	}
	// Repeating the same floor changes nothing; a lower floor is invalid.
	for i := 0; i < 3; i++ {
		if e := n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
			t.Fatal("repeat", i, e)
		}
	}
	wantRetained(t, n, "after repeats")
	if e := n.Retain(ctx, map[Partition]int64{p: 1}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	// A retry of an expired ID is a new record; a retry of a retained ID returns the original.
	if r := publish(t, n, "c"); r.Offset != 2 {
		t.Fatal("retained ID duplicated:", r)
	}
	if r := publish(t, n, "a"); r.Offset != 4 {
		t.Fatal("expired ID did not become a new record:", r)
	}
	// Retain everything: the partition is empty but offsets continue.
	if e := n.Commit(ctx, p, "g", 2, []byte("s3")); e != nil {
		t.Fatal(e)
	}
	if e := n.Commit(ctx, p, "g", 3, []byte("s4")); e != nil {
		t.Fatal(e)
	}
	if e := n.Commit(ctx, p, "g", 4, []byte("s5")); e != nil {
		t.Fatal(e)
	}
	if e := n.Retain(ctx, map[Partition]int64{p: 5}); e != nil {
		t.Fatal(e)
	}
	if _, e := n.Read(ctx, p, 4, false); e != ErrExpired {
		t.Fatal(e)
	}
	if _, e := n.Read(ctx, p, 5, false); e != ErrEmpty {
		t.Fatal(e)
	}
	n.Close()
	m := openTest(t, dir, config())
	if r := publish(t, m, "after-empty"); r.Offset != 5 {
		t.Fatal("offsets did not continue:", r)
	}
	if pos, e := m.Position(ctx, p, "g"); e != nil || pos.Next != 5 || string(pos.State) != "s5" {
		t.Fatal(pos, e)
	}
	// A backup taken before retention still holds the whole history.
	old := openTest(t, before, config())
	wantOldHistory(t, old, "backup before retention")
	// A backup taken after retention honours the floor and the checkpoints.
	after := filepath.Join(t.TempDir(), "after")
	if e := m.Backup(ctx, after); e != nil {
		t.Fatal(e)
	}
	restored := openTest(t, after, config())
	if _, e := restored.Read(ctx, p, 4, false); e != ErrExpired {
		t.Fatal(e)
	}
	if r, e := restored.Read(ctx, p, 5, false); e != nil || r.ID != "after-empty" {
		t.Fatal(r, e)
	}
}

// A process that opened journal.v1 before the rename holds the old inode. It must not
// be able to lock it afterwards, or it could acknowledge writes into an orphan.
func TestRetentionKeepsOldInodeLocked(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	stale, e := os.Open(filepath.Join(dir, "journal.v1"))
	if e != nil {
		t.Fatal(e)
	}
	defer stale.Close()
	if e = syscall.Flock(int(stale.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e == nil {
		t.Fatal("journal not locked before retention")
	}
	if e = n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
		t.Fatal(e)
	}
	if e = syscall.Flock(int(stale.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e == nil {
		t.Fatal("old inode lockable after retention: a 0.2.x process could write into it")
	}
	if info, _ := stale.Stat(); info.Size() != 0 {
		t.Fatal("old inode space not released:", info.Size())
	}
	// A second retention replaces the held inode without leaking descriptors or locks.
	if e = n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
		t.Fatal(e)
	}
	n.Close()
	if e = syscall.Flock(int(stale.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		t.Fatal("lock still held after Close:", e)
	}
}

// Many groups with large states must not make Retain fail on the frame size limit.
func TestRetentionManyLargeGroups(t *testing.T) {
	dir := t.TempDir()
	n := openTest(t, dir, config())
	publish(t, n, "a")
	publish(t, n, "b")
	state := bytes.Repeat([]byte("x"), 900<<10)
	const groups = 6 // about 7 MB of state, far over one 4 MiB frame
	for i := 0; i < groups; i++ {
		if e := n.Commit(ctx, p, fmt.Sprintf("g%d", i), 0, state); e != nil {
			t.Fatal(e)
		}
		if e := n.Commit(ctx, p, fmt.Sprintf("g%d", i), 1, state); e != nil {
			t.Fatal(e)
		}
	}
	if e := n.Retain(ctx, map[Partition]int64{p: 1}); e != nil {
		t.Fatal("retain with many large group states:", e)
	}
	check := func(m *Node, label string) {
		for i := 0; i < groups; i++ {
			pos, e := m.Position(ctx, p, fmt.Sprintf("g%d", i))
			if e != nil || pos.Next != 2 || !bytes.Equal(pos.State, state) {
				t.Fatalf("%s: group g%d: next %d, err %v", label, i, pos.Next, e)
			}
		}
		if r, e := m.Read(ctx, p, 1, false); e != nil || r.ID != "b" {
			t.Fatal(label, r, e)
		}
		if _, e := m.Read(ctx, p, 0, false); e != ErrExpired {
			t.Fatal(label, e)
		}
	}
	check(n, "same process")
	n.Close()
	check(openTest(t, dir, config()), "after reopen")
}

// Retain, publish, Retain, publish and commit on one open node, then reopen: every
// acknowledgement must survive. The second Retain used to rename onto the first
// retention's temp path and leave journal.v1 stale.
func TestRetainTwiceThenReopenKeepsEverything(t *testing.T) {
	dir := t.TempDir()
	n := seedRetention(t, dir)
	if e := n.Retain(ctx, map[Partition]int64{p: 1}); e != nil {
		t.Fatal(e)
	}
	publish(t, n, "e")
	if e := n.Commit(ctx, p, "g", 2, []byte("s3")); e != nil {
		t.Fatal(e)
	}
	if e := n.Retain(ctx, map[Partition]int64{p: 2}); e != nil {
		t.Fatal(e)
	}
	publish(t, n, "f")
	if e := n.Commit(ctx, p, "g", 3, []byte("s4")); e != nil {
		t.Fatal(e)
	}
	n.Close()
	var names []string
	es, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range es {
		names = append(names, f.Name())
	}
	if len(names) != 2 || names[0] != "journal.lock" || names[1] != "journal.v1" {
		t.Fatal("directory must hold only journal.lock and journal.v1:", names)
	}
	if info, e := os.Stat(filepath.Join(dir, "journal.v1")); e != nil || info.Size() == 0 {
		t.Fatal("journal.v1 empty or missing:", info, e)
	}
	m := openTest(t, dir, config())
	for off, id := range map[int64]string{2: "c", 3: "d", 4: "e", 5: "f"} {
		if r, e := m.Read(ctx, p, off, false); e != nil || r.ID != id {
			t.Fatalf("offset %d: %v %v", off, r, e)
		}
	}
	if _, e := m.Read(ctx, p, 1, false); e != ErrExpired {
		t.Fatal(e)
	}
	if pos, e := m.Position(ctx, p, "g"); e != nil || pos.Next != 4 || string(pos.State) != "s4" {
		t.Fatal(pos, e)
	}
	if r := publish(t, m, "g2"); r.Offset != 6 {
		t.Fatal(r)
	}
}
