package workflow

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rodriveracom/dostoevsky/internal/seam"
)

// The store generation, lease tokens and transition times all reach disk. With fixed inputs two runs store the same
// bytes and hand out the same token.
func TestWithFixedInputsTwoRunsStoreTheSameBytes(t *testing.T) {
	run := func() (map[string][]byte, string) {
		seam.Fix(1_700_000_000_000_000_000, 1_000_000, 7)
		defer seam.Restore()
		dir := filepath.Join(t.TempDir(), "store")
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Register("work", 1, []Step{{Kind: "activity", Name: "send", Attempts: 2}}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.StartOnce("run", "work", 1, "q", []byte("0"), map[string]string{"b": "2", "a": "1"}); err != nil {
			t.Fatal(err)
		}
		task, err := s.Poll("q", time.Minute)
		if err != nil || task == nil {
			t.Fatalf("no task: %v", err)
		}
		if err := s.Finish("run", task.Token, []byte("1"), ""); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		files := map[string][]byte{}
		err = filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return err
			}
			name, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			files[name], err = os.ReadFile(path)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return files, task.Token
	}
	first, token := run()
	second, again := run()
	if token != again {
		t.Errorf("lease tokens differ: %s and %s", token, again)
	}
	if len(first) == 0 || len(first) != len(second) {
		t.Fatalf("%d and %d files", len(first), len(second))
	}
	for name, data := range first {
		if !bytes.Equal(data, second[name]) {
			t.Errorf("%s differs between the runs", name)
		}
	}
}
