package dostoevsky

import (
	"errors"
	"math"
	"strconv"
	"testing"
)

func value(t *testing.T, n *Node, id string) (Record, error) {
	t.Helper()
	return n.Publish(ctx, p.Topic, Message{ID: id, Value: []byte("0123456789012345678901234567890123456789")})
}

// fill publishes until the cap refuses, and returns how many records were accepted.
func fill(t *testing.T, n *Node, prefix string) int {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if _, err := value(t, n, prefix+strconv.Itoa(i)); err != nil {
			if !errors.Is(err, ErrJournalFull) || !errors.Is(err, ErrLimit) {
				t.Fatal(err)
			}
			return i
		}
	}
	t.Fatal("the cap never refused")
	return 0
}

func TestAJournalCapRefusesPublishesAndLetsRetainMakeRoom(t *testing.T) {
	dir := t.TempDir()
	n := openTest(t, dir, config())
	if err := n.SetJournalCap(-1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	capAt := n.JournalSize() + 2000
	if err := n.SetJournalCap(capAt); err != nil {
		t.Fatal(err)
	}
	count := fill(t, n, "a")
	if count < 2 || n.JournalSize() > capAt {
		t.Fatalf("%d records, size %d of %d", count, n.JournalSize(), capAt)
	}
	size := n.JournalSize()
	// Refusing is not a failure of the node: reads work, a repeated ID still returns its record, nothing is written.
	if r, err := n.Read(ctx, p, 0, false); err != nil || r.ID != "a0" {
		t.Fatal(r, err)
	}
	if r, err := value(t, n, "a0"); err != nil || r.Offset != 0 {
		t.Fatal("a repeat of a stored record:", r, err)
	}
	batch := []Publication{{Topic: p.Topic, Message: Message{Value: []byte("x")}}, {Topic: p.Topic, Message: Message{Value: []byte("y")}}}
	if _, err := n.PublishBatch(ctx, batch); !errors.Is(err, ErrJournalFull) {
		t.Fatal("batch:", err)
	}
	if n.JournalSize() != size {
		t.Fatalf("a refused write changed the journal: %d to %d", size, n.JournalSize())
	}
	// Commits still go through, so Retain can remove what was consumed.
	for i := 0; i < count; i++ {
		if err := n.Commit(ctx, p, "g", int64(i), nil); err != nil {
			t.Fatal(i, err)
		}
	}
	if err := n.Retain(ctx, map[Partition]int64{p: int64(count)}); err != nil {
		t.Fatal(err)
	}
	if n.JournalSize() >= size {
		t.Fatalf("retention freed nothing: %d", n.JournalSize())
	}
	// The cap survives the compaction, which replaces the journal.
	if again := fill(t, n, "b"); again < 1 || n.JournalSize() > capAt {
		t.Fatalf("after retention: %d records, size %d of %d", again, n.JournalSize(), capAt)
	}
	// It belongs to the node, not the directory.
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n = openTest(t, dir, config())
	if _, err := value(t, n, "after-reopen"); err != nil {
		t.Fatal(err)
	}
}

func TestTheCommitReserveIsBoundedToo(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	for i := 0; i < 8; i++ {
		if _, err := value(t, n, strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	capAt := n.JournalSize()
	if err := n.SetJournalCap(capAt); err != nil {
		t.Fatal(err)
	}
	state := make([]byte, MaxValue)
	done := 0
	for ; done < 8; done++ {
		if err := n.Commit(ctx, p, "g", int64(done), state); err != nil {
			if !errors.Is(err, ErrJournalFull) {
				t.Fatal(err)
			}
			break
		}
	}
	if done < 1 || done == 8 || n.JournalSize() > capAt+minCommitReserve {
		t.Fatalf("%d commits, size %d, ceiling %d", done, n.JournalSize(), capAt+minCommitReserve)
	}
	if pos, err := n.Position(ctx, p, "g"); err != nil || pos.Next != int64(done) {
		t.Fatalf("a refused commit moved the cursor: %+v %v", pos, err)
	}
}

func TestConcurrentPublishersStayUnderTheCap(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	capAt := n.JournalSize() + 4000
	if err := n.SetJournalCap(capAt); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 64)
	for i := 0; i < 64; i++ {
		go func() {
			_, err := n.Publish(ctx, p.Topic, Message{Value: []byte("0123456789012345678901234567890123456789")})
			results <- err
		}()
	}
	refused := 0
	for i := 0; i < 64; i++ {
		if err := <-results; err != nil {
			if !errors.Is(err, ErrJournalFull) {
				t.Fatal(err)
			}
			refused++
		}
	}
	if refused == 0 || n.JournalSize() > capAt {
		t.Fatalf("refused %d, size %d of %d", refused, n.JournalSize(), capAt)
	}
}

// A refused write keeps nothing in memory either: ever new topics on a full journal must not grow the index.
func TestARefusedPublishLeavesNoStreamBehind(t *testing.T) {
	n := openTest(t, t.TempDir(), Config{"a", []string{"a"}, map[string]int{"*": 1}})
	if err := n.SetJournalCap(n.JournalSize()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := n.Publish(ctx, "new-"+strconv.Itoa(i), Message{Value: []byte("v")}); !errors.Is(err, ErrJournalFull) {
			t.Fatal(err)
		}
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.streams) != 0 {
		t.Fatalf("%d streams kept after refused publishes", len(n.streams))
	}
}

// The largest cap does not overflow into "no ceiling".
func TestTheCommitCeilingDoesNotOverflow(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	if err := n.SetJournalCap(math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	n.mu.RLock()
	got := n.commitCeiling()
	n.mu.RUnlock()
	if got != math.MaxInt64 {
		t.Fatal(got)
	}
	if _, err := value(t, n, "x"); err != nil {
		t.Fatal(err)
	}
}
