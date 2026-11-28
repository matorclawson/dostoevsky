package dostoevsky

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/internal/seam"
)

// storedFiles reads every file under dir, by its path relative to dir.
func storedFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		out[name], err = os.ReadFile(path)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWithFixedInputsTwoRunsStoreTheSameBytes(t *testing.T) {
	for name, open := range map[string]func(string, Config) (*Node, error){"legacy": Open, "segmented": OpenSegmented} {
		t.Run(name, func(t *testing.T) {
			run := func() (map[string][]byte, []Record) {
				seam.Fix(1_700_000_000_000_000_000, 1_000_000, 7)
				defer seam.Restore()
				dir := t.TempDir()
				n, err := open(dir, config())
				if err != nil {
					t.Fatal(err)
				}
				var records []Record
				for _, m := range []Message{{Value: []byte("one"), ID: "a"}, {Value: []byte("two"), Key: "k"}} {
					r, err := n.Publish(context.Background(), p.Topic, m)
					if err != nil {
						t.Fatal(err)
					}
					records = append(records, r)
				}
				rs, err := n.PublishBatch(context.Background(), []Publication{{p.Topic, Message{Value: []byte("three")}}, {p.Topic, Message{Value: []byte("four"), ID: "d"}}})
				if err != nil {
					t.Fatal(err)
				}
				records = append(records, rs...)
				if err := n.CommitBatch(context.Background(), p, "g", 0, 2, []byte("state")); err != nil {
					t.Fatal(err)
				}
				if err := n.Close(); err != nil {
					t.Fatal(err)
				}
				return storedFiles(t, dir), records
			}
			first, records := run()
			second, again := run()
			for i, r := range records {
				if want := int64(1_700_000_000_000_000_000 + i*1_000_000); r.Time.UnixNano() != want {
					t.Errorf("record %d has time %d, want %d", i, r.Time.UnixNano(), want)
				}
			}
			if !reflect.DeepEqual(records, again) {
				t.Errorf("replies differ: %+v and %+v", records, again)
			}
			if len(first) == 0 || len(first) != len(second) {
				t.Fatalf("%d and %d files", len(first), len(second))
			}
			for file, data := range first {
				if !bytes.Equal(data, second[file]) {
					t.Errorf("%s differs between the runs", file)
				}
			}
		})
	}
}

// Unset, the seam is the standard library: the same functions, and stored times are the time of the publish.
func TestUnsetSeamIsTheStandardLibrary(t *testing.T) {
	seam.Fix(0, 1, 1)
	seam.Restore()
	if reflect.ValueOf(seam.Now).Pointer() != reflect.ValueOf(time.Now).Pointer() || reflect.ValueOf(seam.Random).Pointer() != reflect.ValueOf(rand.Read).Pointer() {
		t.Fatal("the unset seam is not the standard library")
	}
	n := openTest(t, t.TempDir(), config())
	defer n.Close()
	before := time.Now()
	r, err := n.Publish(context.Background(), p.Topic, Message{Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	if r.Time.Before(before.Add(-time.Second)) || r.Time.After(time.Now().Add(time.Second)) {
		t.Fatalf("stored time %v is not the time of the publish", r.Time)
	}
}

// The randomness is part of the contract another implementation follows, so it is pinned here.
func TestFixedRandomnessIsTheDocumentedStream(t *testing.T) {
	seam.Fix(0, 1, 7)
	defer seam.Restore()
	a, b := make([]byte, 16), make([]byte, 40)
	if _, err := seam.Random(a); err != nil {
		t.Fatal(err)
	}
	if _, err := seam.Random(b); err != nil {
		t.Fatal(err)
	}
	// SHA-256(seed=7 || counter), little-endian: block 0 cut to 16 bytes; then block 1 and 8 bytes of block 2.
	block := func(counter byte) []byte {
		in := make([]byte, 16)
		in[0], in[8] = 7, counter
		return sum256(in)
	}
	if !bytes.Equal(a, block(0)[:16]) || !bytes.Equal(b, append(block(1), block(2)[:8]...)) {
		t.Fatalf("unexpected stream %s %s", hex.EncodeToString(a), hex.EncodeToString(b))
	}
}

func sum256(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}
