package dostoevsky

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const killBatchSize = 16

func killConfig() Config { return Config{"a", []string{"a"}, map[string]int{"words": 4}} }

func killItems(batch int) []Publication {
	items := make([]Publication, killBatchSize)
	for i := range items {
		seq := batch*killBatchSize + i
		items[i] = Publication{"words", Message{
			Key:   fmt.Sprintf("key-%d", seq%7),
			ID:    fmt.Sprintf("id-%06d", seq),
			Value: []byte(fmt.Sprintf("val-%06d", seq)),
		}}
	}
	return items
}

// TestPublishBatchKillWriter is the child process of TestPublishBatchSIGKILL.
// It publishes batches until it is killed and prints one line per acknowledged batch.
func TestPublishBatchKillWriter(t *testing.T) {
	dir := os.Getenv("DOST_TEST_KILL")
	if dir == "" {
		t.Skip("helper process")
	}
	n, e := Open(dir, killConfig())
	if e != nil {
		os.Exit(2)
	}
	for b := 0; b < 100000; b++ {
		if _, e = n.PublishBatch(ctx, killItems(b)); e != nil {
			os.Exit(3)
		}
		fmt.Printf("ack %d ok\n", b)
	}
	os.Exit(0)
}

// scanAll reads every partition to its end and returns the records per partition.
func scanAll(t *testing.T, n *Node) map[int][]Record {
	t.Helper()
	out := map[int][]Record{}
	for part := 0; part < 4; part++ {
		for off := int64(0); ; off++ {
			r, e := n.Read(ctx, Partition{"words", part}, off, false)
			if e == ErrEmpty {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			if r.Offset != off || r.Partition.Number != part {
				t.Fatalf("record at partition %d offset %d reports %v", part, off, r)
			}
			out[part] = append(out[part], r)
		}
	}
	return out
}

func seqOf(t *testing.T, id string) int {
	t.Helper()
	v, e := strconv.Atoi(strings.TrimPrefix(id, "id-"))
	if e != nil {
		t.Fatal(id, e)
	}
	return v
}

// checkLog verifies unique IDs and ascending publication order inside each partition,
// and returns the records by ID.
func checkLog(t *testing.T, n *Node) map[string]Record {
	t.Helper()
	byID := map[string]Record{}
	for part, rs := range scanAll(t, n) {
		last := -1
		for _, r := range rs {
			if _, dup := byID[r.ID]; dup {
				t.Fatalf("duplicate ID %s", r.ID)
			}
			byID[r.ID] = r
			s := seqOf(t, r.ID)
			if s <= last {
				t.Fatalf("partition %d out of order: %d after %d", part, s, last)
			}
			last = s
		}
	}
	return byID
}

func TestPublishBatchSIGKILL(t *testing.T) {
	for iter := 0; iter < 4; iter++ {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestPublishBatchKillWriter$")
		cmd.Env = append(os.Environ(), "DOST_TEST_KILL="+dir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, e := cmd.StdoutPipe()
		if e != nil {
			t.Fatal(e)
		}
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		target := 1 + iter*3
		acked, killed := 0, false
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var b int
			// A line cut short by the kill lacks the trailing " ok" and is not an acknowledgement.
			if _, e := fmt.Sscanf(sc.Text(), "ack %d ok", &b); e != nil || b != acked {
				continue
			}
			acked++
			if acked >= target && !killed {
				killed = true
				cmd.Process.Kill()
			}
		}
		cmd.Wait()
		if !killed {
			t.Fatalf("writer exited before %d acknowledged batches (%d): %s", target, acked, stderr.String())
		}

		n, e := Open(dir, killConfig())
		if e != nil {
			t.Fatal("reopen after SIGKILL:", e)
		}
		before := checkLog(t, n)
		for seq := 0; seq < acked*killBatchSize; seq++ {
			id := fmt.Sprintf("id-%06d", seq)
			r, ok := before[id]
			want := killItems(seq / killBatchSize)[seq%killBatchSize].Message
			if !ok {
				t.Fatalf("iteration %d: acknowledged record %s lost", iter, id)
			}
			wp, _ := n.partition("words", want.Key)
			if r.Key != want.Key || !bytes.Equal(r.Value, want.Value) || r.Partition != wp {
				t.Fatalf("iteration %d: acknowledged record %s changed: %+v", iter, id, r)
			}
		}
		// Retry every acknowledged batch and the one in flight with the original IDs.
		for b := 0; b <= acked; b++ {
			rs, e := n.PublishBatch(ctx, killItems(b))
			if e != nil {
				t.Fatal(e)
			}
			for _, r := range rs {
				if old, ok := before[r.ID]; ok && (old.Partition != r.Partition || old.Offset != r.Offset || !old.Time.Equal(r.Time)) {
					t.Fatalf("retry moved %s from %+v to %+v", r.ID, old, r)
				}
			}
		}
		after := checkLog(t, n)
		if len(after) != (acked+1)*killBatchSize {
			t.Fatalf("iteration %d: %d records after retry, want %d", iter, len(after), (acked+1)*killBatchSize)
		}
		for id, old := range before {
			if r := after[id]; r.Offset != old.Offset || r.Partition != old.Partition {
				t.Fatalf("record %s moved by retry", id)
			}
		}
		n.Close()
	}
}

func copyJournal(t *testing.T, from string) string {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(from, "journal.v1"))
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	if e = os.WriteFile(filepath.Join(dir, "journal.v1"), b, 0600); e != nil {
		t.Fatal(e)
	}
	return dir
}

func TestPublishBatchTruncatedFinalFrame(t *testing.T) {
	src := t.TempDir()
	n := openTest(t, src, killConfig())
	if _, e := n.PublishBatch(ctx, killItems(0)); e != nil {
		t.Fatal(e)
	}
	start := n.j.end
	last := Publication{"words", Message{Key: "tail", ID: "id-999999", Value: []byte("final")}}
	if _, e := n.PublishBatch(ctx, []Publication{last}); e != nil {
		t.Fatal(e)
	}
	end := n.j.end
	n.Close()
	for _, cut := range []int64{1, 8, 19, 20, 21, end - start - 1} {
		dir := copyJournal(t, src)
		if e := os.Truncate(filepath.Join(dir, "journal.v1"), start+cut); e != nil {
			t.Fatal(e)
		}
		m, e := Open(dir, killConfig())
		if e != nil {
			t.Fatalf("cut %d: %v", cut, e)
		}
		if info, _ := os.Stat(filepath.Join(dir, "journal.v1")); info.Size() != start {
			t.Fatalf("cut %d: torn tail kept, size %d want %d", cut, info.Size(), start)
		}
		if got := len(checkLog(t, m)); got != killBatchSize {
			t.Fatalf("cut %d: %d records survive, want %d", cut, got, killBatchSize)
		}
		r, e := m.PublishBatch(ctx, []Publication{last})
		if e != nil || r[0].Value == nil || string(r[0].Value) != "final" {
			t.Fatalf("cut %d: retry %v %v", cut, r, e)
		}
		m.Close()
	}
}

func TestPublishBatchCorruptFrameStopsRecovery(t *testing.T) {
	src := t.TempDir()
	n := openTest(t, src, killConfig())
	first := n.j.end // the manifest frame ends here; the first record frame starts here
	if _, e := n.PublishBatch(ctx, killItems(0)); e != nil {
		t.Fatal(e)
	}
	n.Close()
	path := filepath.Join(src, "journal.v1")
	orig, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	// The first record frame is followed by more frames, so the damage is not a torn tail.
	// Byte 3 of a frame is inside its signature; byte 24 is inside its body.
	for name, at := range map[string]int{"body": int(first) + 24, "header": int(first) + 3} {
		dir := t.TempDir()
		b := append([]byte(nil), orig...)
		b[at] ^= 0x40
		jp := filepath.Join(dir, "journal.v1")
		if e = os.WriteFile(jp, b, 0600); e != nil {
			t.Fatal(e)
		}
		if m, e := Open(dir, killConfig()); e == nil {
			m.Close()
			t.Fatalf("%s corruption accepted", name)
		} else if !strings.Contains(e.Error(), "frame at") && !strings.Contains(e.Error(), "invalid journal header") {
			t.Fatalf("%s corruption error does not say where: %v", name, e)
		}
		if info, _ := os.Stat(jp); info.Size() != int64(len(orig)) {
			t.Fatalf("%s corruption: recovery truncated the journal", name)
		}
	}
}
