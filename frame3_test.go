package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func sampleRecord3() record3 {
	return record3{Partition: 7, Offset: 123456789, Time: 1_700_000_000_123_456_789, Key: []byte("key-é"), ID: []byte("id-1"), Value: bytes.Repeat([]byte{0xAB, 0x00, 0xFF}, 100)}
}

func equalRecord3(a, b record3) bool {
	return a.Partition == b.Partition && a.Offset == b.Offset && a.Time == b.Time &&
		bytes.Equal(a.Key, b.Key) && bytes.Equal(a.ID, b.ID) && bytes.Equal(a.Value, b.Value)
}

func TestFrame3RecordRoundTrip(t *testing.T) {
	for name, r := range map[string]record3{
		"typical":    sampleRecord3(),
		"all empty":  {},
		"extremes":   {Partition: math.MaxUint32, Offset: math.MaxUint64, Time: math.MinInt64, Key: []byte{0}, ID: []byte{0xff}, Value: []byte{0}},
		"max time":   {Time: math.MaxInt64, Value: []byte("v")},
		"no key/id":  {Partition: 1, Offset: 2, Value: bytes.Repeat([]byte("x"), 512)},
		"long key":   {Key: bytes.Repeat([]byte("k"), math.MaxUint16), ID: bytes.Repeat([]byte("i"), math.MaxUint16), Value: []byte("v")},
		"only value": {Value: bytes.Repeat([]byte{1}, 1<<20)},
	} {
		b, err := appendRecord3(nil, r)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		typ, body, size, err := readFrame3(b)
		if err != nil || typ != f3Record || size != len(b) {
			t.Fatalf("%s: type %d size %d/%d err %v", name, typ, size, len(b), err)
		}
		got, err := parseRecord3(body)
		if err != nil || !equalRecord3(got, r) {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
		if want := frame3Header + record3Fixed + len(r.Key) + len(r.ID) + len(r.Value); len(b) != want {
			t.Fatalf("%s: %d bytes, want %d", name, len(b), want)
		}
	}
	// The overhead of a record frame is 38 bytes, against 300 or more for the JSON frame.
	b, _ := appendRecord3(nil, record3{Value: make([]byte, 512)})
	if len(b) != 512+38 {
		t.Fatalf("512-byte record frame is %d bytes, want 550", len(b))
	}
}

func TestFrame3AppendsToTheBufferItIsGiven(t *testing.T) {
	prefix := []byte("keep")
	b, err := appendRecord3(prefix, sampleRecord3())
	if err != nil || !bytes.HasPrefix(b, []byte("keep")) {
		t.Fatal(err)
	}
	b2, err := appendRecord3(b, sampleRecord3())
	if err != nil {
		t.Fatal(err)
	}
	_, _, n, err := readFrame3(b2[4:])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, m, err := readFrame3(b2[4+n:]); err != nil || 4+n+m != len(b2) {
		t.Fatalf("second frame: %d %v", m, err)
	}
}

func TestFrame3EverySingleBitFlipIsDetected(t *testing.T) {
	for _, frame := range [][]byte{
		mustRecord3(t, sampleRecord3()),
		mustRecord3(t, record3{}),
		mustFrame3(t, f3Fence, nil),
	} {
		for i := range frame {
			for bit := 0; bit < 8; bit++ {
				bad := append([]byte(nil), frame...)
				bad[i] ^= 1 << bit
				if _, _, _, err := readFrame3(bad); err == nil {
					t.Fatalf("flipping bit %d of byte %d (of %d) was not detected", bit, i, len(frame))
				}
			}
		}
	}
}

func mustRecord3(t testing.TB, r record3) []byte {
	t.Helper()
	b, err := appendRecord3(nil, r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustFrame3(t testing.TB, typ byte, body []byte) []byte {
	t.Helper()
	b, err := appendFrame3(nil, typ, body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFrame3TruncationAtEveryLength(t *testing.T) {
	b := mustRecord3(t, sampleRecord3())
	for cut := 0; cut < len(b); cut++ {
		if _, _, _, err := readFrame3(b[:cut]); !errors.Is(err, errFrame3Short) {
			t.Fatalf("cut at %d of %d: %v", cut, len(b), err)
		}
	}
	// Bytes after the frame are not part of it.
	tail := append(append([]byte(nil), b...), 1, 2, 3)
	if _, _, n, err := readFrame3(tail); err != nil || n != len(b) {
		t.Fatalf("trailing bytes: size %d err %v", n, err)
	}
}

func TestFrame3Limits(t *testing.T) {
	room := frame3Max - record3Fixed
	if _, err := appendRecord3(nil, record3{Value: make([]byte, room)}); err != nil {
		t.Fatalf("a frame exactly at the limit was refused: %v", err)
	}
	if _, err := appendRecord3(nil, record3{Value: make([]byte, room+1)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("one byte over: %v", err)
	}
	if _, err := appendRecord3(nil, record3{Key: make([]byte, math.MaxUint16+1)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("key over 65,535 bytes: %v", err)
	}
	if _, err := appendRecord3(nil, record3{ID: make([]byte, math.MaxUint16+1)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("id over 65,535 bytes: %v", err)
	}
	if _, err := appendFrame3(nil, f3Fence, make([]byte, frame3Max)); err != nil {
		t.Fatalf("a generic frame exactly at the limit was refused: %v", err)
	}
	if _, err := appendFrame3(nil, f3Fence, make([]byte, frame3Max+1)); !errors.Is(err, ErrLimit) {
		t.Fatalf("generic frame over the limit: %v", err)
	}
	// A reader refuses a declared length above the limit even when the header check is right.
	for _, n := range []uint32{frame3Max + 1, math.MaxUint32} {
		b := make([]byte, frame3Header)
		binary.LittleEndian.PutUint32(b, n)
		b[4] = f3Fence
		sealFrame3(b, 0)
		if _, _, _, err := readFrame3(b); !errors.Is(err, errFrame3Invalid) {
			t.Fatalf("declared length %d: %v", n, err)
		}
	}
	// Non-zero flags are refused even when both checks are right.
	b := mustFrame3(t, f3Fence, nil)
	b[5] = 1
	sealFrame3(b, 0)
	if _, _, _, err := readFrame3(b); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("flags: %v", err)
	}
	// A frame with an empty body is valid (the fence frame).
	if _, body, size, err := readFrame3(mustFrame3(t, f3Fence, nil)); err != nil || len(body) != 0 || size != frame3Header {
		t.Fatalf("empty body: %d %d %v", len(body), size, err)
	}
}

// cx PR 30 M1: a damaged length field must never look like a torn tail. A scanner truncates a
// torn append, so confusing the two would destroy acknowledged data.
func TestFrame3ADamagedHeaderIsNeverATornTail(t *testing.T) {
	frame := mustRecord3(t, sampleRecord3())
	// every bit of the header (length, type, flags, header check): corruption, not "short"
	for i := 0; i < 10; i++ {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), frame...)
			bad[i] ^= 1 << bit
			if _, _, _, err := readFrame3(bad); !errors.Is(err, errFrame3Header) || errors.Is(err, errFrame3Short) {
				t.Fatalf("bit %d of header byte %d: %v", bit, i, err)
			}
			// even when the damaged length claims far more data than the file holds
			if _, _, _, err := readFrame3(bad[:frame3Header+3]); errors.Is(err, errFrame3Short) {
				t.Fatalf("a damaged header looked torn when truncated: bit %d of byte %d", bit, i)
			}
		}
	}
	// damage to the body or its check, with the header intact, is a checksum failure
	for i := 10; i < len(frame); i++ {
		bad := append([]byte(nil), frame...)
		bad[i] ^= 0x01
		if _, _, _, err := readFrame3(bad); !errors.Is(err, errFrame3Checksum) {
			t.Fatalf("damage at byte %d: %v", i, err)
		}
	}
	// a verified header with the body cut short IS a torn append; an incomplete header is torn too
	for cut := 0; cut < len(frame); cut++ {
		if _, _, _, err := readFrame3(frame[:cut]); !errors.Is(err, errFrame3Short) {
			t.Fatalf("cut at %d: %v", cut, err)
		}
	}
	// a body cannot be moved under another valid header
	other := mustRecord3(t, record3{Value: bytes.Repeat([]byte{9}, len(sampleRecord3().Value)+len(sampleRecord3().Key)+len(sampleRecord3().ID))})
	spliced := append(append([]byte(nil), frame[:frame3Header]...), other[frame3Header:]...)
	if len(spliced) == len(frame) {
		if _, _, _, err := readFrame3(spliced); !errors.Is(err, errFrame3Checksum) {
			t.Fatalf("a body moved under another header was accepted: %v", err)
		}
	}
}

func TestFrame3RecordBodyIsValidated(t *testing.T) {
	good := mustRecord3(t, sampleRecord3())
	_, body, _, _ := readFrame3(good)
	if _, err := parseRecord3(body[:record3Fixed-1]); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("short body: %v", err)
	}
	bad := append([]byte(nil), body...)
	binary.LittleEndian.PutUint16(bad[20:], uint16(len(body)))
	if _, err := parseRecord3(bad); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("key longer than the body: %v", err)
	}
	bad = append([]byte(nil), body...)
	binary.LittleEndian.PutUint16(bad[20:], 40000)
	binary.LittleEndian.PutUint16(bad[22:], 40000) // the sum overflows 16 bits
	if _, err := parseRecord3(bad); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("key plus id longer than the body: %v", err)
	}
}

func TestFrame3CommitRoundTripAndValidation(t *testing.T) {
	for name, c := range map[string]commit3{
		"with expected": {Partition: 3, Group: []byte("billing"), Next: 10, HasExpected: true, Expected: 9, State: []byte("state-é")},
		"no expected":   {Partition: 3, Group: []byte("g"), Next: 1, State: nil},
		"expected zero": {Group: []byte{}, Next: math.MaxUint64, HasExpected: true, Expected: 0, State: bytes.Repeat([]byte{7}, 1000)},
		"large state":   {Group: []byte("g"), Next: 2, State: bytes.Repeat([]byte("s"), 1<<20)},
	} {
		b, err := appendCommit3(nil, c)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		typ, body, _, err := readFrame3(b)
		if err != nil || typ != f3Commit {
			t.Fatalf("%s: %d %v", name, typ, err)
		}
		got, err := parseCommit3(body)
		if err != nil || got.Partition != c.Partition || !bytes.Equal(got.Group, c.Group) || got.Next != c.Next ||
			got.HasExpected != c.HasExpected || got.Expected != c.Expected || !bytes.Equal(got.State, c.State) {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
	good := mustCommit3(t, commit3{Group: []byte("g"), Next: 1})
	_, body, _, _ := readFrame3(good)
	for name, mutate := range map[string]func([]byte) []byte{
		"short":                  func(b []byte) []byte { return b[:5] },
		"group too long":         func(b []byte) []byte { binary.LittleEndian.PutUint16(b[4:], 500); return b },
		"flag 2":                 func(b []byte) []byte { b[6+1+8] = 2; return b },
		"absent but not zero":    func(b []byte) []byte { b[6+1+8+1] = 1; return b },
		"truncated before state": func(b []byte) []byte { return b[:len(b)-9] },
	} {
		if _, err := parseCommit3(mutate(append([]byte(nil), body...))); !errors.Is(err, errFrame3Invalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := appendCommit3(nil, commit3{Group: make([]byte, math.MaxUint16+1)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("group over 65,535 bytes: %v", err)
	}
	if _, err := appendCommit3(nil, commit3{State: make([]byte, frame3Max)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("state over the frame limit: %v", err)
	}
}

func mustCommit3(t testing.TB, c commit3) []byte {
	t.Helper()
	b, err := appendCommit3(nil, c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestFrame3TopicDefRoundTrip(t *testing.T) {
	d := topicDef3{Partition: 9, Number: 4, Topic: []byte("orders-é")}
	b, err := appendTopicDef3(nil, d)
	if err != nil {
		t.Fatal(err)
	}
	typ, body, _, err := readFrame3(b)
	if err != nil || typ != f3TopicDef {
		t.Fatal(typ, err)
	}
	got, err := parseTopicDef3(body)
	if err != nil || got.Partition != 9 || got.Number != 4 || !bytes.Equal(got.Topic, d.Topic) {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err = parseTopicDef3(body[:7]); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("short topic definition: %v", err)
	}
	if _, err = appendTopicDef3(nil, topicDef3{Topic: make([]byte, math.MaxUint16+1)}); !errors.Is(err, ErrLimit) {
		t.Fatalf("topic over 65,535 bytes: %v", err)
	}
}

func TestFrame3Fence(t *testing.T) {
	typ, body, size, err := readFrame3(mustFrame3(t, f3Fence, nil))
	if err != nil || typ != f3Fence || len(body) != 0 || size != frame3Header {
		t.Fatalf("%d %d %d %v", typ, len(body), size, err)
	}
}

func TestFrame3TimeConversion(t *testing.T) {
	for _, tm := range []time.Time{time.Unix(0, 0).UTC(), time.Unix(1_700_000_000, 123_456_789).UTC(), time.Date(1700, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(2200, 12, 31, 23, 59, 59, 999, time.UTC)} {
		n, err := timeToNanos(tm)
		if err != nil || !nanosToTime(n).Equal(tm) {
			t.Fatalf("%v: %d %v %v", tm, n, err, nanosToTime(n))
		}
	}
	if n, err := timeToNanos(time.Time{}); err != nil || n != math.MinInt64 || !nanosToTime(n).IsZero() {
		t.Fatalf("zero time: %d %v", n, err)
	}
	// the exact bounds, inclusive: MinInt64 is the zero-time sentinel, so the earliest instant is one nanosecond later
	if n, err := timeToNanos(minFrameTime); err != nil || n != math.MinInt64+1 || !nanosToTime(n).Equal(minFrameTime) {
		t.Fatalf("earliest instant: %d %v", n, err)
	}
	if n, err := timeToNanos(maxFrameTime); err != nil || n != math.MaxInt64 || !nanosToTime(n).Equal(maxFrameTime) {
		t.Fatalf("latest instant: %d %v", n, err)
	}
	if _, err := timeToNanos(minFrameTime.Add(-time.Nanosecond)); err == nil {
		t.Fatal("one nanosecond before the earliest instant was accepted")
	}
	if _, err := timeToNanos(maxFrameTime.Add(time.Nanosecond)); err == nil {
		t.Fatal("one nanosecond after the latest instant was accepted")
	}
	// the documented dates
	if got := minFrameTime.Format("2006-01-02T15:04:05.000000000Z"); got != "1677-09-21T00:12:43.145224193Z" {
		t.Fatalf("earliest instant is %s", got)
	}
	if got := maxFrameTime.Format("2006-01-02T15:04:05.000000000Z"); got != "2262-04-11T23:47:16.854775807Z" {
		t.Fatalf("latest instant is %s", got)
	}
	for _, tm := range []time.Time{time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if _, err := timeToNanos(tm); err == nil {
			t.Fatalf("%v was accepted", tm)
		}
	}
	// only the instant matters, not the location it is expressed in; monotonic readings do not change it
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("east", 14*3600), time.FixedZone("west", -12*3600)} {
		tm := time.Date(2026, 11, 25, 12, 0, 0, 123, loc)
		want, err := timeToNanos(tm.UTC())
		got, err2 := timeToNanos(tm)
		if err != nil || err2 != nil || got != want || !nanosToTime(got).Equal(tm) || nanosToTime(got).Location() != time.UTC {
			t.Fatalf("%v: %d vs %d (%v %v)", loc, got, want, err, err2)
		}
	}
	// the same instants just inside the bounds, written in a far-away zone
	west := time.FixedZone("west", -12*3600)
	if _, err := timeToNanos(minFrameTime.In(west)); err != nil {
		t.Fatalf("earliest instant in another zone: %v", err)
	}
	if _, err := timeToNanos(maxFrameTime.In(time.FixedZone("east", 14*3600))); err != nil {
		t.Fatalf("latest instant in another zone: %v", err)
	}
	if _, err := timeToNanos(time.Now()); err != nil { // carries a monotonic reading
		t.Fatalf("time.Now: %v", err)
	}
}

// Whatever bytes the fuzzer builds, the reader neither panics nor reads past the input, and
// a frame it accepts is exactly what the writer would produce for the same content.
func FuzzReadFrame3(f *testing.F) {
	f.Add(mustRecord3(f, sampleRecord3()))
	f.Add(mustFrame3(f, f3Fence, nil))
	f.Add(mustCommit3(f, commit3{Group: []byte("g"), Next: 3, HasExpected: true, Expected: 2, State: []byte("s")}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		typ, body, size, err := readFrame3(b)
		if err != nil {
			return
		}
		if size > len(b) || size < frame3Header {
			t.Fatalf("size %d of %d", size, len(b))
		}
		again, err := appendFrame3(nil, typ, body)
		if err != nil || !bytes.Equal(again, b[:size]) {
			t.Fatalf("re-encoding differs: %v", err)
		}
		switch typ {
		case f3Record:
			if r, err := parseRecord3(body); err == nil {
				if round, err := appendRecord3(nil, r); err != nil || !bytes.Equal(round, b[:size]) {
					t.Fatalf("record re-encoding differs: %v", err)
				}
			}
		case f3Commit:
			if c, err := parseCommit3(body); err == nil {
				if round, err := appendCommit3(nil, c); err != nil || !bytes.Equal(round, b[:size]) {
					t.Fatalf("commit re-encoding differs: %v", err)
				}
			}
		}
	})
}

// ----- benchmarks: the encode and decode cost of one 512-byte record, binary against JSON -----

func benchRecord() Record {
	return Record{Partition: Partition{"events", 0}, Message: Message{Value: bytes.Repeat([]byte{0xAB}, 512)}, Offset: 123456, Time: time.Unix(1_700_000_000, 5).UTC()}
}

func BenchmarkEncodeRecordBinary(b *testing.B) {
	r := record3{Partition: 1, Offset: 123456, Time: 1_700_000_000_000_000_005, Value: bytes.Repeat([]byte{0xAB}, 512)}
	var buf []byte
	b.ReportAllocs()
	b.SetBytes(512)
	for i := 0; i < b.N; i++ {
		buf, _ = appendRecord3(buf[:0], r)
	}
}

func BenchmarkEncodeRecordJSON(b *testing.B) {
	e := entry{Record: func() *Record { r := benchRecord(); return &r }()}
	var buf []byte
	b.ReportAllocs()
	b.SetBytes(512)
	for i := 0; i < b.N; i++ {
		buf, _ = appendEncodedFrame(buf[:0], e, maxFrame+20)
	}
}

func BenchmarkDecodeRecordBinary(b *testing.B) {
	frame, _ := appendRecord3(nil, record3{Partition: 1, Offset: 123456, Time: 1, Value: bytes.Repeat([]byte{0xAB}, 512)})
	b.ReportAllocs()
	b.SetBytes(512)
	for i := 0; i < b.N; i++ {
		_, body, _, err := readFrame3(frame)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = parseRecord3(body); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeRecordJSON(b *testing.B) {
	r := benchRecord()
	payload, _ := json.Marshal(entry{Record: &r})
	b.ReportAllocs()
	b.SetBytes(512)
	for i := 0; i < b.N; i++ {
		var e entry
		if err := json.Unmarshal(payload, &e); err != nil {
			b.Fatal(err)
		}
	}
}

// scan3 stands in for the scanner the segment layer will have: it walks frames, treats a verified
// short frame at the end as a torn append to truncate at, and anything else as corruption.
func scan3(b []byte) (good int, torn bool, err error) {
	for good < len(b) {
		_, _, n, e := readFrame3(b[good:])
		switch {
		case e == nil:
			good += n
		case errors.Is(e, errFrame3Short):
			return good, true, nil
		default:
			return good, false, e
		}
	}
	return good, false, nil
}

// cc PR 30 M1: corrupted lengths at every offset near the end of a file must be reported, never
// truncated as a torn tail (a torn tail is dropped; corruption is an error).
func TestFrame3ScanNeverTruncatesAfterADamagedByte(t *testing.T) {
	var file []byte
	var starts []int
	for i, v := range [][]byte{[]byte("a"), bytes.Repeat([]byte("b"), 300), nil, bytes.Repeat([]byte("c"), 40), []byte("last-record")} {
		starts = append(starts, len(file))
		var err error
		if file, err = appendRecord3(file, record3{Partition: 1, Offset: uint64(i), Value: v}); err != nil {
			t.Fatal(err)
		}
	}
	if good, torn, err := scan3(file); good != len(file) || torn || err != nil {
		t.Fatalf("the intact file: %d %v %v", good, torn, err)
	}
	// every bit of every byte of the last three frames
	for i := starts[len(starts)-3]; i < len(file); i++ {
		for bit := 0; bit < 8; bit++ {
			bad := append([]byte(nil), file...)
			bad[i] ^= 1 << bit
			good, torn, err := scan3(bad)
			if torn {
				t.Fatalf("bit %d of byte %d (of %d): damage was classified as a torn tail and would be truncated at %d", bit, i, len(file), good)
			}
			if err == nil {
				t.Fatalf("bit %d of byte %d: damage went unnoticed", bit, i)
			}
		}
	}
	// a real torn append, at every length of the last frame, is truncated at that frame's start
	last := starts[len(starts)-1]
	for cut := last + 1; cut < len(file); cut++ {
		if good, torn, err := scan3(file[:cut]); good != last || !torn || err != nil {
			t.Fatalf("cut at %d: good %d torn %v err %v, want %d true nil", cut, good, torn, err, last)
		}
	}
	if good, torn, err := scan3(file[:last]); good != last || torn || err != nil {
		t.Fatalf("a file ending on a frame boundary: %d %v %v", good, torn, err)
	}
}
