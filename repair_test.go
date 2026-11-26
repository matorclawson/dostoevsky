package dostoevsky

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// threeRecords writes a journal of three records and returns its path, bytes and where each record's frame starts.
func threeRecords(t *testing.T) (dir, path string, data []byte, starts []int) {
	t.Helper()
	dir = t.TempDir()
	path = filepath.Join(dir, "journal.v1")
	n := openTest(t, dir, config())
	for _, v := range []string{"one", "two", "three"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		starts = append(starts, int(info.Size()))
		if _, err := n.Publish(ctx, p.Topic, Message{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return dir, path, data, starts
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func unchanged(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("the journal changed: %d bytes, want %d, %v", len(got), len(want), err)
	}
}

func records(t *testing.T, dir string, want ...string) {
	t.Helper()
	n := openTest(t, dir, config())
	defer n.Close()
	for i, v := range want {
		r, err := n.Read(ctx, p, int64(i), false)
		if err != nil || string(r.Value) != v {
			t.Fatalf("record %d: %q %v", i, r.Value, err)
		}
	}
	if _, err := n.Read(ctx, p, int64(len(want)), false); !errors.Is(err, ErrEmpty) {
		t.Fatalf("more records than %d: %v", len(want), err)
	}
}

func TestASoundJournalIsLeftAlone(t *testing.T) {
	dir, path, data, _ := threeRecords(t)
	for _, call := range []func() (JournalReport, error){
		func() (JournalReport, error) { return InspectJournal(dir) },
		func() (JournalReport, error) { return RepairJournal(dir, RepairOptions{}) },
	} {
		r, err := call()
		if err != nil || r.Damage != DamageNone || r.GoodBytes != int64(len(data)) || r.Quarantine != "" || r.LaterFrames != 0 {
			t.Fatalf("%+v %v", r, err)
		}
	}
	unchanged(t, path, data)
}

// A damaged last frame, a zero-filled tail and a damaged header at the end: Open refuses, repair keeps what verifies
// and sets the rest aside byte for byte.
func TestRepairOfADamagedEnd(t *testing.T) {
	for name, c := range map[string]struct {
		damage string
		keep   int
		break_ func(data []byte, starts []int) []byte
	}{
		"payload":     {DamageChecksum, 2, func(d []byte, s []int) []byte { d[len(d)-2] ^= 1; return d }},
		"header":      {DamageHeader, 2, func(d []byte, s []int) []byte { d[s[2]+9] ^= 1; return d }},
		"zeroed tail": {DamageHeader, 3, func(d []byte, s []int) []byte { return append(d, make([]byte, 4096)...) }},
	} {
		t.Run(name, func(t *testing.T) {
			dir, path, data, starts := threeRecords(t)
			broken := c.break_(bytes.Clone(data), starts)
			write(t, path, broken)
			if _, err := Open(dir, config()); err == nil {
				t.Fatal("Open accepted the damage")
			}
			unchanged(t, path, broken)
			seen, err := InspectJournal(dir)
			if err != nil || seen.Damage != c.damage || seen.LaterFrames != 0 {
				t.Fatalf("%+v %v", seen, err)
			}
			unchanged(t, path, broken)
			r, err := RepairJournal(dir, RepairOptions{})
			if err != nil || r.Quarantine == "" {
				t.Fatalf("%+v %v", r, err)
			}
			removed, err := os.ReadFile(r.Quarantine)
			if err != nil || !bytes.Equal(removed, broken[r.GoodBytes:]) {
				t.Fatalf("set aside %d bytes, want %d: %v", len(removed), len(broken)-int(r.GoodBytes), err)
			}
			records(t, dir, []string{"one", "two", "three"}[:c.keep]...)
		})
	}
}

// Damage with sound frames after it is refused until the caller says to discard them.
func TestRepairRefusesToDiscardLaterFramesUnasked(t *testing.T) {
	dir, path, data, starts := threeRecords(t)
	broken := bytes.Clone(data)
	broken[starts[2]-2] ^= 1 // inside the second record's payload
	write(t, path, broken)
	r, err := RepairJournal(dir, RepairOptions{})
	if !errors.Is(err, ErrLaterFrames) || r.Damage != DamageChecksum || r.LaterFrames != 1 || r.Quarantine != "" {
		t.Fatalf("%+v %v", r, err)
	}
	unchanged(t, path, broken)
	if r, err = RepairJournal(dir, RepairOptions{DiscardLaterFrames: true}); err != nil || r.GoodBytes != int64(starts[1]) {
		t.Fatalf("%+v %v", r, err)
	}
	records(t, dir, "one")
}

// A torn tail is still cut off by Open, and Open says so.
func TestOpenReportsTheTornTailItCut(t *testing.T) {
	dir, path, data, starts := threeRecords(t)
	write(t, path, data[:len(data)-1])
	if r, err := InspectJournal(dir); err != nil || r.Damage != DamageTornTail || r.GoodBytes != int64(starts[2]) {
		t.Fatalf("%+v %v", r, err)
	}
	n := openTest(t, dir, config())
	if got := n.Truncated(); got.At != int64(starts[2]) || got.Bytes != int64(len(data)-1-starts[2]) {
		t.Fatalf("%+v", got)
	}
	n.Close()
	n = openTest(t, dir, config())
	defer n.Close()
	if got := n.Truncated(); got != (Truncation{}) {
		t.Fatalf("nothing was cut this time: %+v", got)
	}
}

func TestRepairNeedsTheDirectoryClosedAndCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	n := openTest(t, dir, config())
	if _, err := InspectJournal(dir); err == nil {
		t.Fatal("inspected a directory a node has open")
	}
	if _, err := RepairJournal(dir, RepairOptions{}); err == nil {
		t.Fatal("repaired a directory a node has open")
	}
	n.Close()
	missing := filepath.Join(t.TempDir(), "nothing")
	if _, err := RepairJournal(missing, RepairOptions{}); err == nil {
		t.Fatal("repaired nothing")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the directory was created: %v", err)
	}
}
