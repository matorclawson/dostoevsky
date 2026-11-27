package dostoevsky

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// External receipt oracle for the opt-in identified publish fixture. Use only on a disposable store.
func TestDeepIDCrash(t *testing.T) {
	dir := os.Getenv("DOST_ID_CRASH_DIR")
	if dir == "" {
		t.Skip("set disposable segmented ID fixture directory")
	}
	prefix := os.Getenv("DOST_ID_CRASH_PREFIX")
	if prefix == "" {
		t.Fatal("supply a fresh DOST_ID_CRASH_PREFIX")
	}
	open := func() *Node {
		n, e := OpenSegmented(dir, Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}})
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	item := func(i int) Publication {
		return Publication{Topic: "events", Message: Message{ID: fmt.Sprintf("%s-%08d", prefix, i), Value: bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 256)}}
	}
	const size = 64
	ctx := context.Background()
	if os.Getenv("DOST_ID_CRASH_CHILD") == "1" {
		n := open()
		defer n.Close()
		for start := 0; ; start += size {
			items := make([]Publication, size)
			for i := range items {
				items[i] = item(start + i)
			}
			rs, err := n.PublishBatch(ctx, items)
			if err != nil {
				t.Fatal(err)
			}
			for i, r := range rs {
				if r.Offset != rs[0].Offset+int64(i) || r.ID != items[i].Message.ID {
					t.Fatal("incorrect publisher receipt")
				}
			}
			if _, err = fmt.Printf("receipt %d %d\n", start, rs[0].Offset); err != nil {
				return
			}
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run", "^TestDeepIDCrash$")
	cmd.Env = append(os.Environ(), "DOST_ID_CRASH_CHILD=1")
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	scanner := bufio.NewScanner(output)
	var offsets []int64
	for scanner.Scan() {
		var start int
		var offset int64
		if _, e := fmt.Sscanf(scanner.Text(), "receipt %d %d", &start, &offset); e != nil || start != size*len(offsets) {
			t.Fatal("invalid acknowledgement", scanner.Text())
		}
		offsets = append(offsets, offset)
		if len(offsets) == 320 {
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil || len(offsets) < 320 {
		t.Fatal("unconfirmed kill", err, len(offsets))
	}
	n := open()
	defer n.Close()
	for block, offset := range offsets {
		if offset != offsets[0]+int64(block*size) {
			t.Fatal("non-contiguous acknowledged batches")
		}
		page, err := n.ReadBatch(ctx, Partition{Topic: "events"}, offset, Limits{size, 1 << 20}, false)
		if err != nil || len(page.Records) != size {
			t.Fatal("short page", err)
		}
		items := make([]Publication, size)
		for i, r := range page.Records {
			items[i] = item(block*size + i)
			if r.Offset != offset+int64(i) || r.ID != items[i].Message.ID || !bytes.Equal(r.Value, items[i].Message.Value) {
				t.Fatal("receipt differs", r.Offset)
			}
		}
		retries, err := n.PublishBatch(ctx, items)
		if err != nil {
			t.Fatal(err)
		}
		for i, r := range retries {
			if r.Offset != offset+int64(i) {
				t.Fatal("retry offset", r.Offset)
			}
		}
	}
	fmt.Printf("signal=SIGKILL initial_offset=%d acknowledged=%d verified=%d retries_verified=%d\n", offsets[0], len(offsets)*size, len(offsets)*size, len(offsets)*size)
}
