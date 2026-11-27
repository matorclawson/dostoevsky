package dostoevsky

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func recordFrame(t testing.TB, r Record) []byte {
	t.Helper()
	b, err := json.Marshal(entry{Record: &r})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// allowedScanDifference: the scanner does not read Value or Time, so a frame whose
// only defect is one of those is accepted by the scanner and rejected by json.Unmarshal.
func allowedScanDifference(err error) bool {
	var b64 base64.CorruptInputError
	var tm *time.ParseError
	return errors.As(err, &b64) || errors.As(err, &tm) || strings.Contains(err.Error(), "Time.UnmarshalJSON")
}

func checkScanAgainstJSON(t testing.TB, b []byte) (accepted bool) {
	t.Helper()
	var sc recordScanner
	pp, off, id, ok := sc.scan(b)
	if !ok {
		return false
	}
	if !json.Valid(b) {
		t.Fatalf("the scanner accepted invalid JSON: %q", b)
	}
	var e entry
	if err := json.Unmarshal(b, &e); err != nil {
		if !allowedScanDifference(err) {
			t.Fatalf("the scanner accepted %q that json.Unmarshal rejects for another reason: %v", b, err)
		}
		return true
	}
	if e.Record == nil || e.Record.Partition != pp || e.Record.Offset != off || e.Record.ID != id {
		t.Fatalf("scanner %v %d %q, json %+v for %q", pp, off, id, e.Record, b)
	}
	return true
}

func TestScanRecordMatchesJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	texts := []string{"", "a", "plain-id", "ünï-çødé", "日本語", "tab\there", `quote"inside`, `back\slash`, "<tag>&amp;", "line\nbreak", " sep", string(rune(0x1F600))}
	accepted, declined := 0, 0
	for i := 0; i < 3000; i++ {
		v := make([]byte, rng.Intn(40))
		rng.Read(v)
		if i%7 == 0 {
			v = nil
		}
		r := Record{
			Partition: Partition{texts[rng.Intn(len(texts))] + "t", rng.Intn(1000)},
			Message:   Message{Key: texts[rng.Intn(len(texts))], Value: v, ID: texts[rng.Intn(len(texts))]},
			Offset:    rng.Int63n(1 << 40),
			Time:      time.Unix(rng.Int63n(1<<31), rng.Int63n(1e9)).UTC(),
		}
		if i%50 == 0 {
			r.Offset = int64(i)
			r.Number = 0
		}
		if checkScanAgainstJSON(t, recordFrame(t, r)) {
			accepted++
		} else {
			declined++
		}
	}
	if accepted < 150 || declined == 0 {
		t.Fatalf("accepted %d, declined %d: the plain shapes must take the fast path and escaped text must fall back", accepted, declined)
	}
}

func TestScanRecordDeclinesWhatItDoesNotFullyUnderstand(t *testing.T) {
	good := string(recordFrame(t, Record{Partition: Partition{"events", 0}, Message: Message{Key: "k", Value: []byte("hello"), ID: "i"}, Offset: 7, Time: time.Unix(1, 2).UTC()}))
	if !checkScanAgainstJSON(t, []byte(good)) {
		t.Fatalf("the writer's own frame was declined: %s", good)
	}
	for name, b := range map[string]string{
		"reordered":        strings.Replace(good, `"Number":0,"Key":"k"`, `"Key":"k","Number":0`, 1),
		"whitespace":       strings.Replace(good, `"Topic":"events"`, `"Topic": "events"`, 1),
		"escaped topic":    strings.Replace(good, `"events"`, `"\u0065vents"`, 1),
		"escaped id":       strings.Replace(good, `"ID":"i"`, `"ID":"\"i"`, 1),
		"leading zero":     strings.Replace(good, `"Offset":7`, `"Offset":07`, 1),
		"signed":           strings.Replace(good, `"Offset":7`, `"Offset":-7`, 1),
		"float":            strings.Replace(good, `"Offset":7`, `"Offset":7.0`, 1),
		"huge":             strings.Replace(good, `"Offset":7`, `"Offset":9999999999999999999`, 1),
		"odd base64 len":   strings.Replace(good, `"Value":"aGVsbG8="`, `"Value":"aGVsbG8"`, 1),
		"padding midway":   strings.Replace(good, `"Value":"aGVsbG8="`, `"Value":"aG=sbG8="`, 1),
		"unknown field":    strings.Replace(good, `"ID":"i"`, `"Extra":1,"ID":"i"`, 1),
		"missing field":    strings.Replace(good, `"Key":"k",`, ``, 1),
		"other entry":      `{"Fence":1}`,
		"trailing bytes":   good + " ",
		"truncated":        good[:len(good)-1],
		"empty":            ``,
		"not json":         `Record`,
		"invalid utf8 id":  strings.Replace(good, `"ID":"i"`, "\"ID\":\"\xff\"", 1),
		"control in key":   strings.Replace(good, `"Key":"k"`, "\"Key\":\"\x01\"", 1),
		"nested record":    `{"Record":{"Topic":"a","Number":0,"Key":"","Value":null,"ID":"","Offset":0,"Time":"x"}}garbage`,
		"config frame":     `{"Config":{"Node":"a","Nodes":["a"],"Topics":{"t":1}}}`,
		"record is a list": `{"Record":[]}`,
	} {
		var sc recordScanner
		if _, _, _, ok := sc.scan([]byte(b)); ok {
			t.Errorf("%s was accepted by the scanner: %s", name, b)
		}
	}
	// Documented limits: a malformed time or a base64 value of the right shape but wrong
	// characters is accepted by the scanner and rejected by json.Unmarshal, and by Read.
	for name, b := range map[string]string{
		"bad time":   strings.Replace(good, `"Time":"1970-01-01T00:00:01.000000002Z"`, `"Time":"yesterday"`, 1),
		"bad base64": strings.Replace(good, `"Value":"aGVsbG8="`, `"Value":"!!!!!!!!"`, 1),
	} {
		var sc recordScanner
		if _, _, _, ok := sc.scan([]byte(b)); !ok {
			t.Errorf("%s: the scanner declined, expected the documented acceptance", name)
		}
		var e entry
		if err := json.Unmarshal([]byte(b), &e); err == nil {
			t.Errorf("%s: json.Unmarshal accepted it", name)
		}
	}
}

func FuzzScanRecord(f *testing.F) {
	for _, r := range []Record{
		{Partition: Partition{"events", 0}, Message: Message{Value: []byte("hello")}, Offset: 1, Time: time.Unix(1, 0).UTC()},
		{Partition: Partition{"é", 12}, Message: Message{Key: "k", ID: "i", Value: nil}, Offset: 99, Time: time.Unix(2, 5).UTC()},
		{Partition: Partition{"a\"b", 1}, Message: Message{ID: "<&>", Value: []byte{}}, Offset: 0, Time: time.Unix(3, 0).UTC()},
	} {
		b, _ := json.Marshal(entry{Record: &r})
		f.Add(b)
	}
	f.Add([]byte(`{"Fence":1}`))
	f.Fuzz(func(t *testing.T, b []byte) { checkScanAgainstJSON(t, b) })
}

// ----- replay equivalence: the scanner path and the encoding/json path build the same node -----

func copyJournalPrefix(t testing.TB, src string, cut int64) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(src, "journal.v1"))
	if err != nil {
		t.Fatal(err)
	}
	if cut >= 0 {
		b = b[:cut]
	}
	dst := t.TempDir()
	if err = os.WriteFile(filepath.Join(dst, "journal.v1"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

type replayResult struct {
	streams map[Partition]*stream
	end     int64
	size    int64
	err     error
}

func replayWith(t testing.TB, dir string, cfg Config, fast bool, chunk int) replayResult {
	t.Helper()
	oldFast, oldChunk := fastRecordReplay, replayChunk
	fastRecordReplay, replayChunk = fast, chunk
	defer func() { fastRecordReplay, replayChunk = oldFast, oldChunk }()
	n, err := Open(dir, cfg)
	if err != nil {
		return replayResult{err: err}
	}
	res := replayResult{streams: n.streams, end: n.j.end}
	if fi, e := os.Stat(filepath.Join(dir, "journal.v1")); e == nil {
		res.size = fi.Size()
	}
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	return res
}

func mixedConfig() Config {
	return Config{"a", []string{"a"}, map[string]int{"words": 1, "other": 2}}
}

// buildMixedJournal writes the shapes replay meets: empty and nil values, IDs and keys
// that need escapes, unicode, every base64 padding length, a value at MaxValue (its frame
// is larger than the default window), batches over two topics, consumer commits, and a
// Retain that rewrites the journal with seed frames and records after it.
func buildMixedJournal(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	n, err := Open(dir, mixedConfig())
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(3))
	ids := []string{"", "plain", `quote"id`, "<tag>&", "ünï", "tab\tid", `back\slash`, "日本語"}
	pub := func(topic, key string, v []byte, id string) {
		t.Helper()
		if _, err := n.Publish(ctx, topic, Message{Key: key, Value: v, ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		v := make([]byte, i%7)
		rng.Read(v)
		id := ids[i%len(ids)]
		if id != "" {
			id = fmt.Sprintf("%s-%d", id, i)
		}
		pub("words", ids[(i+3)%len(ids)], v, id)
	}
	pub("words", "", nil, "")
	pub("words", "", []byte{}, "")
	big := make([]byte, MaxValue)
	rng.Read(big)
	pub("words", "big", big, "big-1")
	pub("words", "key", []byte("after-big"), "after")
	var items []Publication
	for i := 0; i < 20; i++ {
		items = append(items, Publication{Topic: []string{"words", "other"}[i%2], Message: Message{Key: fmt.Sprintf("k%d", i), Value: []byte(fmt.Sprintf("batch-%d", i)), ID: fmt.Sprintf("b%d", i)}})
	}
	if _, err = n.PublishBatch(ctx, items); err != nil {
		t.Fatal(err)
	}
	// A new group stands at 0: one commit that jumps (it records the expected cursor) and one ordinary commit.
	if err = n.CommitBatch(ctx, Partition{"words", 0}, "g", 0, 10, []byte("state-é\"")); err != nil {
		t.Fatal(err)
	}
	if err = n.Commit(ctx, Partition{"words", 0}, "g", 10, []byte("state-é\"")); err != nil {
		t.Fatal(err)
	}
	if err = n.Retain(ctx, map[Partition]int64{{"words", 0}: 5}); err != nil {
		t.Fatal(err)
	}
	pub("words", "post", []byte("post-retain"), "post-1")
	pub("words", "post", []byte("post-retain-2"), `post"2`)
	if err = n.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func sameReplay(t *testing.T, name string, a, b replayResult) {
	t.Helper()
	if (a.err == nil) != (b.err == nil) || (a.err != nil && a.err.Error() != b.err.Error()) {
		t.Fatalf("%s: errors differ: %v vs %v", name, a.err, b.err)
	}
	if a.err != nil {
		return
	}
	if a.end != b.end || a.size != b.size {
		t.Fatalf("%s: journal end %d/%d size %d/%d", name, a.end, b.end, a.size, b.size)
	}
	if !reflect.DeepEqual(a.streams, b.streams) {
		t.Fatalf("%s: the replayed nodes differ", name)
	}
}

func TestFastReplayEqualsJSONReplay(t *testing.T) {
	dir := buildMixedJournal(t)
	slow := replayWith(t, copyJournalPrefix(t, dir, -1), mixedConfig(), false, 1<<20)
	if slow.err != nil || len(slow.streams) < 2 {
		t.Fatalf("reference replay: %v, %d streams", slow.err, len(slow.streams))
	}
	for _, chunk := range []int{1 << 20, 4 << 20, 1, 7, 64, 4096, 100000} {
		fast := replayWith(t, copyJournalPrefix(t, dir, -1), mixedConfig(), true, chunk)
		sameReplay(t, fmt.Sprintf("chunk %d", chunk), slow, fast)
		// the chunked reader with the JSON path alone is also the same node
		sameReplay(t, fmt.Sprintf("json chunk %d", chunk), slow, replayWith(t, copyJournalPrefix(t, dir, -1), mixedConfig(), false, chunk))
	}
	// Every record reads back through the fast-replayed node.
	copyDir := copyJournalPrefix(t, dir, -1)
	n, err := Open(copyDir, mixedConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for tp, s := range n.streams {
		for off := s.base; off < s.base+s.count(); off++ {
			if _, err := n.Read(ctx, tp, off, false); err != nil {
				t.Fatalf("%v offset %d: %v", tp, off, err)
			}
		}
	}
}

// A journal cut at every byte of its last frame reopens to the same node on both
// paths and is truncated back to the last complete frame, as before.
func TestTornTailAtEveryBytePosition(t *testing.T) {
	dir := t.TempDir()
	n, err := Open(dir, config())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		publish(t, n, fmt.Sprintf("id-%d", i))
	}
	n.Close()
	full, err := os.ReadFile(filepath.Join(dir, "journal.v1"))
	if err != nil {
		t.Fatal(err)
	}
	// find the start of the last frame
	var starts []int64
	for at := int64(0); at < int64(len(full)); {
		starts = append(starts, at)
		at += 20 + int64(binaryBE32(full[at+8:]))
	}
	last := starts[len(starts)-1]
	for cut := last + 1; cut < int64(len(full)); cut++ {
		slow := replayWith(t, copyJournalPrefix(t, dir, cut), config(), false, 1<<20)
		fast := replayWith(t, copyJournalPrefix(t, dir, cut), config(), true, 1<<20)
		tiny := replayWith(t, copyJournalPrefix(t, dir, cut), config(), true, 5)
		sameReplay(t, fmt.Sprintf("cut %d", cut), slow, fast)
		sameReplay(t, fmt.Sprintf("cut %d tiny chunk", cut), slow, tiny)
		if fast.err != nil || fast.size != last || fast.end != last {
			t.Fatalf("cut %d: err %v, size %d end %d, want %d", cut, fast.err, fast.size, fast.end, last)
		}
	}
}

func binaryBE32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// A corrupted frame is refused with the same error on both paths.
func TestCorruptionIsRefusedIdentically(t *testing.T) {
	dir := t.TempDir()
	n, err := Open(dir, config())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		publish(t, n, fmt.Sprintf("id-%d", i))
	}
	n.Close()
	full, _ := os.ReadFile(filepath.Join(dir, "journal.v1"))
	var starts []int64
	for at := int64(0); at < int64(len(full)); {
		starts = append(starts, at)
		at += 20 + int64(binaryBE32(full[at+8:]))
	}
	for _, off := range []int64{starts[2] + 3, starts[2] + 9, starts[2] + 13, starts[2] + 17, starts[2] + 20, starts[2] + 60} {
		bad := append([]byte(nil), full...)
		bad[off] ^= 0x40
		mk := func() string {
			d := t.TempDir()
			os.WriteFile(filepath.Join(d, "journal.v1"), bad, 0o600)
			return d
		}
		slow := replayWith(t, mk(), config(), false, 1<<20)
		fast := replayWith(t, mk(), config(), true, 1<<20)
		if slow.err == nil {
			t.Fatalf("byte %d flipped: the reference replay accepted it", off)
		}
		sameReplay(t, fmt.Sprintf("flip %d", off), slow, fast)
	}
	if !bytes.Equal(full[:8], signature) {
		t.Fatal("unexpected signature")
	}
}
