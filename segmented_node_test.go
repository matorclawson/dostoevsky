package dostoevsky

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentedMigrationBackupAndContinuation(t *testing.T) {
	src := openTest(t, t.TempDir(), config())
	var records []Record
	for i := 0; i < 8; i++ {
		records = append(records, publish(t, src, fmt.Sprint(i)))
	}
	if e := src.CommitBatch(ctx, p, "worker", 0, 5, []byte("cursor")); e != nil {
		t.Fatal(e)
	}
	if e := src.Retain(ctx, map[Partition]int64{p: 3}); e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(t.TempDir(), "migrated")
	if e := src.MigrateSegmented(ctx, dir); e != nil {
		t.Fatal(e)
	}
	n := openTest(t, dir, config())
	n.rollBytes = 1
	if n.segments == nil {
		t.Fatal("legacy directory")
	}
	for i := 8; i < 16; i++ {
		records = append(records, publish(t, n, fmt.Sprint(i)))
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n = openTest(t, dir, config())
	check := func(node *Node) {
		page, e := node.ReadBatch(ctx, p, 3, Limits{100, 10000}, false)
		if e != nil || page.Next != 16 || len(page.Records) != 13 {
			t.Fatalf("page %v %v", page, e)
		}
		for i, r := range page.Records {
			want := records[i+3]
			if r.Offset != want.Offset || r.ID != want.ID || r.Key != want.Key || !bytes.Equal(r.Value, want.Value) || !r.Time.Equal(want.Time) {
				t.Fatal(r, want)
			}
		}
		pos, e := node.Position(ctx, p, "worker")
		if e != nil || pos.Next != 5 || string(pos.State) != "cursor" {
			t.Fatal(pos, e)
		}
		if _, e = node.Read(ctx, p, 2, false); !errors.Is(e, ErrExpired) {
			t.Fatal(e)
		}
		if r := publish(t, node, "3"); r.Offset != 3 {
			t.Fatal(r)
		}
	}
	check(n)
	if err := n.VerifySegments(ctx); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "export")
	if e := n.Backup(ctx, backup); e != nil {
		t.Fatal(e)
	}
	restored := openTest(t, backup, config())
	check(restored)
	if restored.segments != nil {
		t.Fatal("expected backward-compatible backup")
	}
	if e := n.CommitBatch(ctx, p, "worker", 5, 16, nil); e != nil {
		t.Fatal(e)
	}
	if e := n.Retain(ctx, map[Partition]int64{p: 16}); e != nil {
		t.Fatal(e)
	}
	if len(n.segments.ids) != 0 {
		t.Fatal(n.segments.ids)
	}
	if e := n.Close(); e != nil {
		t.Fatal(e)
	}
	n = openTest(t, dir, config())
	if r := publish(t, n, "3"); r.Offset != 16 {
		t.Fatal(r)
	}
}

func TestSegmentedMissingFileAndRepairGuard(t *testing.T) {
	for _, which := range []string{"first", "middle", "active", "range"} {
		t.Run(which, func(t *testing.T) {
			dir := t.TempDir()
			n, e := OpenSegmented(dir, config())
			if e != nil {
				t.Fatal(e)
			}
			n.rollBytes = 1
			for i := 0; i < 3; i++ {
				publish(t, n, fmt.Sprint(i))
			}
			path := map[string]string{"first": segName(1), "middle": segName(2), "active": segName(3), "range": "RANGE"}[which]
			if e = n.Close(); e != nil {
				t.Fatal(e)
			}
			if _, e = RepairJournal(dir, RepairOptions{}); !errors.Is(e, ErrSegmentedJournal) {
				t.Fatal(e)
			}
			if e = os.Remove(filepath.Join(dir, "segments", path)); e != nil {
				t.Fatal(e)
			}
			if _, e = Open(dir, config()); !errors.Is(e, ErrCorruptLog) {
				t.Fatal(e)
			}
		})
	}
}

func TestSegmentedTornTailAndSealedDamage(t *testing.T) {
	dir := t.TempDir()
	n, e := OpenSegmented(dir, config())
	if e != nil {
		t.Fatal(e)
	}
	n.rollBytes = 1
	publish(t, n, "one")
	publish(t, n, "two")
	loc := n.segments.active.dataStart
	active := n.segments.active.id
	if e = n.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(dir, "segments", segName(active)), os.O_APPEND|os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write([]byte{1, 2, 3}); e != nil {
		t.Fatal(e)
	}
	f.Close()
	n = openTest(t, dir, config())
	if n.truncated.Bytes != 3 {
		t.Fatal(n.truncated)
	}
	if r := publish(t, n, "two"); r.Offset != 1 {
		t.Fatal(r)
	}
	n.Close()
	f, e = os.OpenFile(filepath.Join(dir, "segments", segName(1)), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	var b [1]byte
	if _, e = f.ReadAt(b[:], loc); e != nil {
		t.Fatal(e)
	}
	b[0] ^= 1
	if _, e = f.WriteAt(b[:], loc); e != nil {
		t.Fatal(e)
	}
	f.Close()
	n = openTest(t, dir, config())
	if e = n.VerifySegments(ctx); !errors.Is(e, ErrCorruptLog) {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "backup")
	if e = n.Backup(ctx, dest); e == nil {
		t.Fatal("export accepted corrupt history")
	}
	if _, e = os.Stat(dest); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
}

func TestSegmentedIdleCatalogAndWake(t *testing.T) {
	dir := t.TempDir()
	n, e := OpenSegmented(dir, config())
	if e != nil {
		t.Fatal(e)
	}
	publish(t, n, "first")
	n.Close()
	n = openTest(t, dir, config())
	if len(n.streams) != 0 {
		t.Fatal("idle partitions allocated reader state")
	}
	if _, e = n.ReadBatch(ctx, p, 0, Limits{1, 1}, false); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	timeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		page, e := n.ReadBatch(timeout, p, 1, Limits{1, 100}, true)
		if e == nil && (len(page.Records) != 1 || page.Records[0].ID != "second") {
			e = ErrInvalid
		}
		result <- e
	}()
	publish(t, n, "second")
	if e = <-result; e != nil {
		t.Fatal(e)
	}
	if e = n.Retain(ctx, map[Partition]int64{p: 1}); e != nil {
		t.Fatal(e)
	}
	if _, e = n.ReadBatch(ctx, p, 0, Limits{1, 100}, true); !errors.Is(e, ErrExpired) {
		t.Fatal(e)
	}
}

func TestSegmentedRollCountsOnlyTail(t *testing.T) {
	n, e := OpenSegmented(t.TempDir(), config())
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	publish(t, n, "first")
	if e = n.CommitBatch(ctx, p, "worker", 0, 1, bytes.Repeat([]byte{1}, 4096)); e != nil {
		t.Fatal(e)
	}
	if e = n.Retain(ctx, map[Partition]int64{p: 1}); e != nil {
		t.Fatal(e)
	}
	id := n.segments.active.id
	n.rollBytes = 2048
	for i := 0; i < 4; i++ {
		publish(t, n, fmt.Sprint(i))
	}
	if n.segments.active.id != id {
		t.Fatal("large checkpoint caused premature rollover")
	}
}
