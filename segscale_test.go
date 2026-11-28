package dostoevsky

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This opt-in lab exercises the internal segment layer on real files. It never opens
// or migrates an application directory. All files belong to the disposable test.
func TestSegmentScale(t *testing.T) {
	raw := os.Getenv("DOST_SEG_SCALE")
	if raw == "" {
		t.Skip("set DOST_SEG_SCALE to the count")
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		t.Fatal("invalid count")
	}
	keep := 0
	if s := os.Getenv("DOST_SEG_KEEP"); s != "" {
		keep, err = strconv.Atoi(s)
		if err != nil || keep < 0 {
			t.Fatal("invalid sealed segment count")
		}
	}
	dir := t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	l, err := openSegLog(osFS{}, dir)
	must(err)
	st := newLogState()
	st.setConfig([]byte(`{"Node":"n","Nodes":["n"],"Topics":{"events":1}}`))
	pid, _, err := st.ensure(Partition{"events", 0})
	must(err)
	must(l.start(st.snapshot(1, 1)))
	var verified uint64
	var expected [512]byte
	fill := func(b []byte, i uint64) {
		binary.LittleEndian.PutUint64(b, i)
		h := sha256.Sum256(b[:8])
		copy(b[8:], h[:])
		for n := 32; n < len(b)-8; n *= 2 {
			copy(b[8+n:], b[8:8+n])
		}
	}
	verify := func(id uint64) {
		seg, _, err := scanSegmentCore(osFS{}, dir, id, false, func(r record3, _ int64, _ int) error {
			fill(expected[:], verified)
			if r.Partition != pid || r.Offset != verified || string(r.ID) != strconv.FormatUint(verified, 10) || !bytes.Equal(r.Value, expected[:]) {
				return fmt.Errorf("payload mismatch at %d", verified)
			}
			verified++
			return nil
		}, nil)
		must(err)
		must(seg.f.Close())
	}
	var val [512]byte
	var scratch [24]byte
	buf := make([]byte, 0, 150<<10)
	started := time.Now()
	points := []int{16000, 1000000, 10000000, count}
	for off := 0; off < count; {
		if l.active.end >= 8<<20 {
			sealed := l.active.id
			if keep > 0 && len(l.ids) >= keep {
				old := l.ids[0]
				verify(old)
				must(st.retain(map[uint32]int64{pid: int64(verified)}))
			}
			must(l.roll(st.snapshot(sealed+1, sealed+1)))
			if keep > 0 && len(l.ids) > keep {
				l.retire(l.ids[0])
			}
		}
		end := min(count, off+256)
		for _, point := range points {
			if point > off && point < end {
				end = point
			}
		}
		buf = buf[:0]
		for i := off; i < end; i++ {
			fill(val[:], uint64(i))
			buf, err = appendRecord3(buf, record3{Partition: pid, Offset: uint64(i), Time: int64(i), ID: strconv.AppendUint(scratch[:0], uint64(i), 10), Value: val[:]})
			must(err)
		}
		pos := l.active.end
		must(l.append(buf))
		for frames := buf; len(frames) > 0; {
			_, body, n, err := readFrame3(frames)
			must(err)
			r, err := parseRecord3(body)
			must(err)
			l.observe(r, pos)
			must(st.applyRecord(pid, r.Offset))
			frames = frames[n:]
			pos += int64(n)
		}
		off = end
		for _, point := range points {
			if off != point {
				continue
			}
			must(l.close())
			if os.Getenv("DOST_SEG_FRESH") == "1" {
				exe, err := os.Executable()
				must(err)
				cmd := exec.Command(exe, "-test.run", "^TestFreshSegmentProbe$", "-test.v")
				cmd.Env = append(os.Environ(), "DOST_SEG_PROBE_DIR="+dir, fmt.Sprintf("DOST_SEG_PROBE_LAST=%d", off-1), fmt.Sprintf("DOST_SEG_PROBE_PID=%d", pid))
				output, err := cmd.CombinedOutput()
				t.Logf("child process: %s", output)
				must(err)
			}
			before := time.Now()
			l, err = openSegLog(osFS{}, dir)
			must(err)
			opened := time.Since(before)
			if err = st.equal(l.state); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			rss := "unavailable"
			if b, err := os.ReadFile("/proc/self/status"); err == nil {
				for _, line := range strings.Split(string(b), "\n") {
					if strings.HasPrefix(line, "VmRSS:") {
						rss = strings.Fields(line)[1]
					}
				}
			}
			t.Logf("written=%d retained=%d sealed=%d active_bytes=%d reopen_ms=%.3f heap_KiB=%d currentRSS_KiB=%s elapsed_s=%.3f", off, off-int(verified), len(l.ids), l.active.end, float64(opened)/1e6, m.HeapAlloc/1024, rss, time.Since(started).Seconds())
			break
		}
	}
	for _, id := range l.ids {
		verify(id)
	}
	verify(l.active.id)
	if verified != uint64(count) {
		t.Fatal("incomplete readback", verified)
	}
	must(l.close())
	t.Logf("appended=%d verified=%d keep_sealed=%d", count, verified, keep)
}

// TestFreshSegmentProbe runs in a new test process while the parent has closed the log.
func TestFreshSegmentProbe(t *testing.T) {
	dir := os.Getenv("DOST_SEG_PROBE_DIR")
	if dir == "" {
		t.Skip("child of TestSegmentScale")
	}
	last, err := strconv.ParseUint(os.Getenv("DOST_SEG_PROBE_LAST"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.ParseUint(os.Getenv("DOST_SEG_PROBE_PID"), 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	log, err := openSegLog(osFS{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.close()
	elapsed := time.Since(start)
	proc, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	var rss string
	for _, line := range strings.Split(string(proc), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			rss = strings.Fields(line)[1]
		}
	}
	if rss == "" {
		t.Fatal("VmRSS missing")
	}
	record, err := log.readRecord(uint32(pid), last)
	if err != nil || record.Offset != last || len(record.Value) != 512 || binary.LittleEndian.Uint64(record.Value) != last {
		t.Fatalf("tail %d: %v", last, err)
	}
	t.Logf("fresh_process=true written=%d sealed=%d open_ms=%.3f openRSS_KiB=%s tail_verified=true", last+1, len(log.ids), float64(elapsed)/1e6, rss)
}
