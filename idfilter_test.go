package dostoevsky

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIDFilterNoFalseNegatives(t *testing.T) {
	var ids idTable
	for i := 0; i < 100000; i++ {
		ids.add(idHash(uint32(i%3), []byte(fmt.Sprint(i))), uint64(i))
	}
	f := makeIDFilter(&ids)
	ids.each(func(h, _ uint64) {
		if !f.contains(mixFilterHash(h)) {
			t.Fatalf("forgot hash %x", h)
		}
	})
	if makeIDFilter(&idTable{}).contains(1) {
		t.Fatal("empty filter matched")
	}
}

// Corrupt and missing derived files must never let an acknowledged ID through as new.
func TestIDFilterRecoveryConflictAndRetention(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "damaged", "swapped", "false-positive"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			n, e := OpenSegmented(dir, config())
			if e != nil {
				t.Fatal(e)
			}
			n.rollBytes = 1
			for i := 0; i < 4; i++ {
				publish(t, n, fmt.Sprint(i))
			}
			if e = n.Close(); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(dir, "segments", filterName(1))
			switch kind {
			case "missing":
				e = os.Remove(path)
			case "damaged":
				e = os.WriteFile(path, []byte("invalid"), 0600)
			case "swapped":
				var b []byte
				b, e = os.ReadFile(filepath.Join(dir, "segments", filterName(2)))
				if e == nil {
					e = os.WriteFile(path, b, 0600)
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			n = openTest(t, dir, config())
			if kind == "false-positive" {
				n.segments.filters.put(1, idFilter{255, 255, 255, 255})
			}
			r := publish(t, n, "0")
			if r.Offset != 0 {
				t.Fatal(r)
			}
			_, e = n.Publish(ctx, p.Topic, Message{ID: "0", Key: "key", Value: []byte("different")})
			if !errors.Is(e, ErrConflict) {
				t.Fatal(e)
			}
			if r = publish(t, n, "new"); r.Offset != 4 {
				t.Fatal(r)
			}
			if e = n.Retain(ctx, map[Partition]int64{p: 5}); e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(path); !os.IsNotExist(e) {
				t.Fatal("retired hint remains", e)
			}
			if r = publish(t, n, "0"); r.Offset != 5 {
				t.Fatal(r)
			}
		})
	}
}

func TestPrepareIDFilters(t *testing.T) {
	dir := t.TempDir()
	n, e := OpenSegmented(dir, config())
	if e != nil {
		t.Fatal(e)
	}
	defer n.Close()
	n.rollBytes = 1
	publish(t, n, "a")
	publish(t, n, "b")
	file := filepath.Join(dir, "segments", filterName(1))
	if e = os.Remove(file); e != nil {
		t.Fatal(e)
	}
	if e = n.PrepareIDFilters(ctx); e != nil {
		t.Fatal(e)
	}
	n.Close()
	n = openTest(t, dir, config())
	if e = n.PrepareIDFilters(ctx); e != nil {
		t.Fatal(e)
	}
	if n.segments.indexLoads != 0 {
		t.Fatal("loaded an exact index while warming valid filters")
	}
	if r := publish(t, n, "a"); r.Offset != 0 {
		t.Fatal(r)
	}
}
