package dostoevsky

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBackupRecovery(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	publish(t, n, "one")
	if err := n.Commit(ctx, p, "g", 0, []byte("state")); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "backup")
	if err := n.Backup(ctx, dst); err != nil {
		t.Fatal(err)
	}
	publish(t, n, "two")
	restored := openTest(t, dst, config())
	pos, err := restored.Position(ctx, p, "g")
	if err != nil || pos.Next != 1 || string(pos.State) != "state" {
		t.Fatal(pos, err)
	}
	if _, err = restored.Read(ctx, p, 1, false); err != ErrEmpty {
		t.Fatal(err)
	}
	if err = n.Backup(ctx, dst); err == nil {
		t.Fatal("existing directory overwritten")
	}
}

type countdown struct {
	context.Context
	left int
}

func (c *countdown) Err() error {
	if c.left <= 0 {
		return context.Canceled
	}
	c.left--
	return nil
}

func TestBackupFailureLeavesNoDestination(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	big := bytes.Repeat([]byte("v"), 100<<10)
	for i := 0; i < 4; i++ { // about 400 KiB, several 128 KiB copy chunks
		if _, e := n.Publish(ctx, "words", Message{ID: fmt.Sprint(i), Value: big}); e != nil {
			t.Fatal(e)
		}
	}
	parent := t.TempDir()
	dst := filepath.Join(parent, "backup")
	if e := n.Backup(&countdown{Context: ctx, left: 3}, dst); !errors.Is(e, context.Canceled) {
		t.Fatal("interrupted backup:", e)
	}
	es, _ := os.ReadDir(parent)
	if len(es) != 0 {
		t.Fatalf("interrupted backup left %d entries, first %s", len(es), es[0].Name())
	}
	if e := n.Backup(ctx, dst); e != nil {
		t.Fatal("retry after failure:", e)
	}
}

func TestBackupSyncsParentDirectory(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	publish(t, n, "one")
	parent := t.TempDir()
	var synced []string
	old := syncDir
	syncDir = func(path string) error { synced = append(synced, path); return old(path) }
	defer func() { syncDir = old }()
	if e := n.Backup(ctx, filepath.Join(parent, "backup")); e != nil {
		t.Fatal(e)
	}
	if len(synced) < 2 || synced[len(synced)-1] != parent {
		t.Fatalf("directory syncs %v: the parent %s must be synced last, after the rename", synced, parent)
	}
	if _, e := os.Stat(filepath.Join(parent, "backup", "journal.v1")); e != nil {
		t.Fatal(e)
	}
}

func TestBackupInterruptedLeavesNoJournal(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	big := bytes.Repeat([]byte("v"), 100<<10)
	for i := 0; i < 4; i++ {
		if _, e := n.Publish(ctx, "words", Message{ID: fmt.Sprint(i), Value: big}); e != nil {
			t.Fatal(e)
		}
	}
	dst := filepath.Join(t.TempDir(), "backup")
	if e := n.Backup(&countdown{Context: ctx, left: 3}, dst); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(dst, "journal.v1")); e == nil {
		t.Fatal("an interrupted backup left a journal.v1")
	}
}

func TestBackupDirectoryEntryNotDurable(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	publish(t, n, "one")
	parent := t.TempDir()
	dst := filepath.Join(parent, "backup")
	old := syncDir
	syncDir = func(path string) error {
		if path == parent {
			return errors.New("injected parent sync failure")
		}
		return old(path)
	}
	defer func() { syncDir = old }()
	e := n.Backup(ctx, dst)
	if !errors.Is(e, ErrBackupNotDurable) {
		t.Fatal("want ErrBackupNotDurable, got", e)
	}
	syncDir = old
	restored := openTest(t, dst, config())
	if r, e := restored.Read(ctx, p, 0, false); e != nil || r.ID != "one" {
		t.Fatal("backup reported as complete is not:", r, e)
	}
}

func TestBackupRefusesExistingDirectoryEvenIfEmpty(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	publish(t, n, "one")
	dst := filepath.Join(t.TempDir(), "backup")
	if e := os.Mkdir(dst, 0700); e != nil {
		t.Fatal(e)
	}
	if e := n.Backup(ctx, dst); !errors.Is(e, os.ErrExist) {
		t.Fatal(e)
	}
	if _, e := os.Stat(dst); e != nil {
		t.Fatal("a refused backup removed a directory it did not create:", e)
	}
}

// A crash during Backup leaves .journal.tmp and no journal.v1. Open must not turn that
// into a silent empty node.
func TestOpenRefusesIncompleteBackup(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, ".journal.tmp"), []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	n, e := Open(dir, config())
	if !errors.Is(e, ErrIncompleteBackup) {
		if e == nil {
			n.Close()
		}
		t.Fatal("want ErrIncompleteBackup, got", e)
	}
	if _, e := os.Stat(filepath.Join(dir, "journal.v1")); e == nil {
		t.Fatal("refused open created a journal")
	}
	// A directory with a journal is not affected by a stray temp file.
	good := t.TempDir()
	m := openTest(t, good, config())
	m.Close()
	if e := os.WriteFile(filepath.Join(good, ".journal.tmp"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	openTest(t, good, config())
}
