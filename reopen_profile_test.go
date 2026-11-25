package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// Reopen profile (evidence for the reopen-time work, not a gate).
//
//	DOST_REOPEN_PROFILE=1 go test -run TestReopenBreakdown -v -count=1 .
//	go test -run '^$' -bench BenchmarkOpenReplay -benchtime=50x -cpuprofile cpu.out -memprofile mem.out .
//
// The journal is built the way the publisher workload builds it: one topic, 512-byte
// values, no key, no ID, one record per frame, written through the node's own journal
// writer. DOST_REOPEN_RECORDS (default 16000) sets the count and DOST_REOPEN_DIR
// the directory (default a test temp dir; on production-03 use the ext4 disk, not tmpfs).

func reopenRecords(tb testing.TB) int {
	n := 16000
	if s := os.Getenv("DOST_REOPEN_RECORDS"); s != "" {
		if _, err := fmt.Sscan(s, &n); err != nil || n <= 0 {
			tb.Fatalf("DOST_REOPEN_RECORDS=%q", s)
		}
	}
	return n
}

func reopenDir(tb testing.TB) string {
	if d := os.Getenv("DOST_REOPEN_DIR"); d != "" {
		dir, err := os.MkdirTemp(d, "reopen-")
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { os.RemoveAll(dir) })
		return dir
	}
	return tb.TempDir()
}

func reopenConfig() Config {
	return Config{Node: "n", Nodes: []string{"n"}, Topics: map[string]int{"events": 1}}
}

// buildReopenJournal writes the journal in groups of 64 frames per sync, so building
// 16,000 records takes a moment rather than the gate's seconds.
func buildReopenJournal(tb testing.TB, dir string, records int) {
	n, err := Open(dir, reopenConfig())
	if err != nil {
		tb.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	now := time.Now().UTC()
	var batch []byte
	for i := 0; i < records; i++ {
		v := make([]byte, 512)
		rng.Read(v)
		e := entry{Record: &Record{Partition: Partition{"events", 0}, Message: Message{Value: v}, Offset: int64(i), Time: now}}
		var ferr error
		if batch, ferr = appendEncodedFrame(batch, e, 1<<30); ferr != nil {
			tb.Fatal(ferr)
		}
		if (i+1)%64 == 0 || i == records-1 {
			if err = n.j.writeFrames(batch); err != nil {
				tb.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err = n.Close(); err != nil {
		tb.Fatal(err)
	}
}

func BenchmarkOpenReplay(b *testing.B) {
	dir := reopenDir(b)
	buildReopenJournal(b, dir, reopenRecords(b))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := Open(dir, reopenConfig())
		if err != nil {
			b.Fatal(err)
		}
		if err = n.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func median(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[len(s)/2]
}

func timeIt(rounds int, f func()) time.Duration {
	d := make([]time.Duration, rounds)
	for i := range d {
		t := time.Now()
		f()
		d[i] = time.Since(t)
	}
	return median(d)
}

// TestReopenBreakdown prints where Open spends its time. Each phase is measured
// alone on the same bytes, so the numbers add up to roughly the total but are not
// a partition: the page cache is warm for all of them, as it is after a process
// restart on the same host.
func TestReopenBreakdown(t *testing.T) {
	if os.Getenv("DOST_REOPEN_PROFILE") == "" {
		t.Skip("set DOST_REOPEN_PROFILE=1")
	}
	records := reopenRecords(t)
	dir := reopenDir(t)
	buildReopenJournal(t, dir, records)
	path := filepath.Join(dir, "journal.v1")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const rounds = 15
	report := func(name string, d time.Duration) {
		t.Logf("reopen_phase=%s records=%d journal_bytes=%d median_ms=%.3f", name, records, len(data), float64(d.Microseconds())/1000)
	}

	report("open_close_total", timeIt(rounds, func() {
		n, err := Open(dir, reopenConfig())
		if err != nil {
			t.Fatal(err)
		}
		n.Close()
	}))
	report("read_file", timeIt(rounds, func() {
		if _, err := os.ReadFile(path); err != nil {
			t.Fatal(err)
		}
	}))
	walk := func(decode bool, hash bool) {
		var e entry
		for at := 0; at < len(data); {
			h := data[at : at+20]
			size := int(binary.BigEndian.Uint32(h[8:]))
			p := data[at+20 : at+20+size]
			if hash && crc32.Checksum(p, crcTable) != binary.BigEndian.Uint32(h[12:]) {
				t.Fatal("checksum")
			}
			if decode {
				e = entry{}
				if err := json.Unmarshal(p, &e); err != nil {
					t.Fatal(err)
				}
			}
			at += 20 + size
		}
	}
	report("walk_headers_only", timeIt(rounds, func() { walk(false, false) }))
	report("walk_plus_crc32c", timeIt(rounds, func() { walk(false, true) }))
	report("walk_plus_crc_plus_json_decode", timeIt(rounds, func() { walk(true, true) }))
	report("json_decode_alone", timeIt(rounds, func() { walk(true, false) }))

	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	report("file_fsync_clean", timeIt(rounds, func() {
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}))
	report("dir_fsync", timeIt(rounds, func() {
		d, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		d.Sync()
		d.Close()
	}))

	// Allocation per Open, and the cost of the index step alone (apply over decoded entries).
	var entries []entry
	var locs []location
	for at := 0; at < len(data); {
		size := int(binary.BigEndian.Uint32(data[at+8:]))
		var e entry
		if err := json.Unmarshal(data[at+20:at+20+size], &e); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
		locs = append(locs, location{int64(at + 20), size})
		at += 20 + size
	}
	report("index_apply_alone", timeIt(rounds, func() {
		n := &Node{cfg: reopenConfig(), streams: make(map[Partition]*stream)}
		for i, e := range entries {
			if e.Record == nil {
				continue
			}
			if err := n.index(e, locs[i]); err != nil {
				t.Fatal(err)
			}
		}
	}))
	allocs := testing.AllocsPerRun(5, func() {
		n, err := Open(dir, reopenConfig())
		if err != nil {
			t.Fatal(err)
		}
		n.Close()
	})
	t.Logf("reopen_allocs_per_open=%.0f records=%d", allocs, records)
	if !bytes.Equal(data[:8], signature) {
		t.Fatalf("unexpected journal signature %q", data[:8])
	}
}
