package dostoevsky

import (
	"errors"
	"os"
	"testing"
)

// countSyncs replaces both sync hooks and reports full and data sync calls.
func countSyncs(t *testing.T) (full, data *int) {
	t.Helper()
	oldFull, oldData := syncFile, syncData
	full, data = new(int), new(int)
	syncFile = func(f *os.File) error { *full++; return oldFull(f) }
	syncData = func(f *os.File) error { *data++; return oldData(f) }
	t.Cleanup(func() { syncFile, syncData = oldFull, oldData })
	return full, data
}

// A fresh journal is created with full syncs; only later appends to the
// established file may use the data sync.
func TestFreshJournalUsesFullSyncUntilEstablished(t *testing.T) {
	full, data := countSyncs(t)
	dir := t.TempDir()
	n := openTest(t, dir, config())
	if *full == 0 || *data != 0 {
		t.Fatalf("creation used %d full and %d data syncs", *full, *data)
	}
	if _, e := n.Publish(ctx, p.Topic, Message{ID: "a", Value: []byte("a")}); e != nil {
		t.Fatal(e)
	}
	if *data != 1 {
		t.Fatalf("append after creation used %d data syncs, want 1", *data)
	}
	if e := n.Close(); e != nil {
		t.Fatal(e)
	}
	// A reopened journal is fully synced by recovery before any append.
	*full, *data = 0, 0
	n = openTest(t, dir, config())
	if *full == 0 || *data != 0 {
		t.Fatalf("reopen used %d full and %d data syncs", *full, *data)
	}
	if _, e := n.Publish(ctx, p.Topic, Message{ID: "b", Value: []byte("b")}); e != nil || *data != 1 {
		t.Fatal(e, *data)
	}
}

// A sync failing after a successful write acknowledges nobody, advances no
// index and fails the journal closed.
func TestQueuedSyncFailureAfterWrite(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	if _, e := n.Publish(ctx, p.Topic, Message{ID: "seed", Value: []byte("s")}); e != nil {
		t.Fatal(e)
	}
	old := syncData
	syncData = func(*os.File) error { return errors.New("injected sync failure") }
	t.Cleanup(func() { syncData = old })
	a, b := pending("a", "a"), pending("b", "b")
	n.writeQueued([]*queuedPublish{a, b})
	if a.err == nil || b.err == nil || n.j.failed == nil {
		t.Fatal("sync failure was acknowledged", a.err, b.err)
	}
	if got := n.streams[p].count(); got != 1 {
		t.Fatalf("index advanced to %d records", got)
	}
	if _, e := n.Publish(ctx, p.Topic, Message{ID: "c", Value: []byte("c")}); e == nil {
		t.Fatal("failed journal accepted a write")
	}
}
