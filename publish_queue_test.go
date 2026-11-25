package dostoevsky

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func pending(id, value string) *queuedPublish {
	return &queuedPublish{ctx: ctx, partition: p, msg: Message{ID: id, Value: []byte(value)}}
}

func TestQueuedPublishIsolation(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	first, dup := pending("a", "hello"), pending("a", "hello")
	canceled := pending("skip", "canceled")
	c, cancel := context.WithCancel(ctx)
	cancel()
	canceled.ctx = c
	n.writeQueued([]*queuedPublish{first, canceled, dup})
	if first.err != nil || dup.err != nil || first.result.Offset != 0 || dup.result.Offset != 0 || canceled.err != context.Canceled {
		t.Fatal(first, dup, canceled)
	}
	dup.result.Value[0] = 'X'
	if first.result.Value[0] != 'h' || first.msg.Value[0] != 'h' {
		t.Fatal("result aliases another publisher")
	}
	bad, good := pending("a", "different"), pending("b", "new")
	n.writeQueued([]*queuedPublish{bad, good})
	if bad.err != ErrConflict || good.err != nil || good.result.Offset != 1 {
		t.Fatal(bad, good)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n.writeQueued([]*queuedPublish{first, dup})
	if first.err != ErrClosed || dup.err != ErrClosed {
		t.Fatal(first.err, dup.err)
	}
}

func TestQueuedWriteFailure(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	if err := n.j.file.Close(); err != nil {
		t.Fatal(err)
	}
	a, b := pending("a", "a"), pending("b", "b")
	n.writeQueued([]*queuedPublish{a, b})
	if a.err == nil || b.err == nil || n.j.failed == nil || len(n.streams) != 0 {
		t.Fatal("failed group became visible")
	}
}

// The parent only records receipts that crossed the pipe before the kill.
// Additional complete writes are allowed to be recovered.
func TestConcurrentPublishKill(t *testing.T) {
	if dir := os.Getenv("DOST_QUEUE_TEST_DIR"); dir != "" {
		n, e := Open(dir, config())
		if e != nil {
			os.Exit(2)
		}
		var next atomic.Uint64
		workers, err := strconv.Atoi(os.Getenv("DOST_QUEUE_TEST_WORKERS"))
		if err != nil || workers < 1 {
			os.Exit(4)
		}
		for i := 0; i < workers; i++ {
			go func() {
				for {
					id := strconv.FormatUint(next.Add(1), 10)
					r, e := n.Publish(ctx, p.Topic, Message{ID: id, Value: []byte(id)})
					if e != nil {
						os.Exit(3)
					}
					fmt.Fprintf(os.Stdout, "%s %d\n", id, r.Offset)
				}
			}()
		}
		select {}
	}
	for _, workers := range []int{1, 8} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) { verifyPublishKill(t, workers) })
	}
}

func verifyPublishKill(t *testing.T, workers int) {
	t.Helper()
	dir := t.TempDir()
	deadline, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(deadline, os.Args[0], "-test.run=^TestConcurrentPublishKill$")
	cmd.Env = append(os.Environ(), "DOST_QUEUE_TEST_DIR="+dir, "DOST_QUEUE_TEST_WORKERS="+strconv.Itoa(workers))
	out, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	acks := map[int64]string{}
	scanner := bufio.NewScanner(out)
	for len(acks) < 256 && scanner.Scan() {
		var id string
		var offset int64
		if _, e = fmt.Sscan(scanner.Text(), &id, &offset); e != nil {
			t.Fatal(e)
		}
		if _, exists := acks[offset]; exists {
			t.Fatalf("duplicate offset %d", offset)
		}
		acks[offset] = id
	}
	if len(acks) != 256 {
		t.Fatalf("received %d acknowledgments: %v", len(acks), scanner.Err())
	}
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	if e = cmd.Wait(); e == nil {
		t.Fatal("child exited normally")
	}
	n := openTest(t, dir, config())
	for offset, id := range acks {
		r, e := n.Read(ctx, p, offset, false)
		if e != nil || r.ID != id || string(r.Value) != id {
			t.Fatalf("recovered offset %d: %v", offset, e)
		}
		r, e = n.Publish(ctx, p.Topic, Message{ID: id, Value: []byte(id)})
		if e != nil || r.Offset != offset {
			t.Fatalf("retry %s: %v", id, e)
		}
	}
}

func TestQueuedAdmissionAndCancel(t *testing.T) {
	n := openTest(t, t.TempDir(), config())
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	n.mu.Lock()
	locked := true
	defer func() {
		if locked {
			n.mu.Unlock()
		}
	}()
	type response struct {
		id int
		r  Record
		e  error
	}
	const total = maxQueuedPublishes + 8
	results := make(chan response, total)
	for i := 0; i < total; i++ {
		go func(id int) {
			callCtx := ctx
			if id == 1 {
				callCtx = canceled
			}
			r, e := n.Publish(callCtx, p.Topic, Message{ID: strconv.Itoa(id)})
			results <- response{id, r, e}
		}(i)
	}
	// Hold the writer until the bounded queue fills. Remaining publishers wait on the node lock.
	deadline := time.Now().Add(5 * time.Second)
	for {
		n.publishes.mu.Lock()
		size := len(n.publishes.pending)
		n.publishes.mu.Unlock()
		if size == maxQueuedPublishes {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d calls queued", size)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	locked = false
	n.mu.Unlock()
	seen := map[int64]bool{}
	for i := 0; i < total; i++ {
		select {
		case res := <-results:
			if res.id == 1 {
				if res.e != context.Canceled {
					t.Fatal("canceled send", res.e)
				}
				continue
			}
			if res.e != nil || seen[res.r.Offset] {
				t.Fatal("unexpected receipt", res)
			}
			seen[res.r.Offset] = true
		case <-time.After(5 * time.Second):
			t.Fatal("stranded publisher")
		}
	}
	for i := 0; i < total-1; i++ {
		if !seen[int64(i)] {
			t.Fatal("missing offset", i)
		}
	}
}

// Group commit and repeated compaction share one node. Each Retain runs only
// after publishers have acknowledged records, so it compacts a non-empty,
// concurrently growing journal. Every acknowledged record keeps its offset and
// payload across both compactions and a reopen.
func TestConcurrentPublishAcrossRetainAndReopen(t *testing.T) {
	dir := t.TempDir()
	n := openTest(t, dir, config())
	const workers, each = 8, 60
	acked := make(chan Record, workers*each)
	var progress atomic.Int64
	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < each; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				r, e := n.Publish(ctx, p.Topic, Message{ID: id, Value: []byte(id)})
				if e != nil {
					t.Errorf("publish %s: %v", id, e)
					return
				}
				acked <- r
				progress.Add(1)
			}
		}(w)
	}
	for _, floor := range []int64{workers, 4 * workers} {
		for progress.Load() < floor {
			time.Sleep(time.Millisecond)
		}
		before := progress.Load()
		if e := n.Retain(ctx, map[Partition]int64{}); e != nil {
			t.Fatal(e)
		}
		if before < floor {
			t.Fatalf("retained after only %d acknowledgments", before)
		}
	}
	for w := 0; w < workers; w++ {
		<-done
	}
	close(acked)
	if e := n.Close(); e != nil {
		t.Fatal(e)
	}
	n = openTest(t, dir, config())
	seen := map[int64]string{}
	for r := range acked {
		if prev, dup := seen[r.Offset]; dup {
			t.Fatalf("offset %d acknowledged for %s and %s", r.Offset, prev, r.ID)
		}
		seen[r.Offset] = r.ID
		got, e := n.Read(ctx, p, r.Offset, false)
		if e != nil || got.ID != r.ID || string(got.Value) != r.ID {
			t.Fatalf("offset %d: %v %q", r.Offset, e, got.ID)
		}
	}
	if len(seen) != workers*each {
		t.Fatalf("acknowledged %d, want %d", len(seen), workers*each)
	}
}

// With many synchronous producers every caller of a group waits for that group, so
// the queue is empty when they are released. The groups must still stay large:
// the drainer gathers the callers it just released instead of letting one of them
// write alone. The sync is slowed to 2 ms so the producers' timing is irrelevant.
func TestGroupCommitKeepsGroupsFullUnderClosedLoopLoad(t *testing.T) {
	old := syncData
	syncData = func(f *os.File) error { time.Sleep(2 * time.Millisecond); return old(f) }
	t.Cleanup(func() { syncData = old })
	n := openTest(t, t.TempDir(), config())
	// The first write of a journal is a full sync; get it out of the way.
	if _, err := n.Publish(ctx, p.Topic, Message{ID: "warm", Value: []byte("w")}); err != nil {
		t.Fatal(err)
	}
	g0, r0 := n.publishes.groups.Load(), n.publishes.records.Load()
	const producers, each = 32, 25
	var wg sync.WaitGroup
	offsets := make(chan int64, producers*each)
	for w := 0; w < producers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				id := fmt.Sprintf("p%d-%d", w, i)
				r, err := n.Publish(ctx, p.Topic, Message{ID: id, Value: []byte(id)})
				if err != nil {
					t.Errorf("publish %s: %v", id, err)
					return
				}
				offsets <- r.Offset
			}
		}()
	}
	wg.Wait()
	close(offsets)
	seen := map[int64]bool{}
	for o := range offsets {
		if seen[o] {
			t.Fatalf("offset %d acknowledged twice", o)
		}
		seen[o] = true
	}
	groups, records := n.publishes.groups.Load()-g0, n.publishes.records.Load()-r0
	if records != producers*each {
		t.Fatalf("%d records written, want %d", records, producers*each)
	}
	if avg := float64(records) / float64(groups); avg < 22 {
		t.Fatalf("average group of %.1f over %d groups; with %d producers the groups should stay near full", avg, groups, producers)
	}
}
