package main

import (
	"bytes"
	"sync"
	"testing"
)

func TestSingleWriterRoundTripAndReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if n := st.(store).db.Stats().MaxOpenConnections; n != 1 {
		t.Fatalf("%d connections: SQLite must be measured with one writer connection", n)
	}
	// Concurrent producers queue in Go; every record gets its own id.
	var wg sync.WaitGroup
	ids := make([]uint64, 8)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := st.Publish([]byte{byte(i)})
			if err != nil {
				t.Error(err)
			}
			ids[i] = id
		}()
	}
	wg.Wait()
	st.Close()
	st, err = openStore(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	seen := map[uint64]bool{}
	for i, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
		got, err := st.Read(id)
		if err != nil || !bytes.Equal(got, []byte{byte(i)}) {
			t.Fatalf("record %d at %d: %v %v", i, id, got, err)
		}
	}
}
