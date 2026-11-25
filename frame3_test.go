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
		if want := frame3Prefix + 2 + record3Fixed + len(r.Key) + len(r.ID) + len(r.Value); len(b) != want {
			t.Fatalf("%s: %d bytes, want %d", name, len(b), want)
		}
	}
	// The overhead of a record frame is 34 bytes, against 300 or more for the JSON frame.
	b, _ := appendRecord3(nil, record3{Value: make([]byte, 512)})
	if len(b) != 512+34 {
		t.Fatalf("512-byte record frame is %d bytes, want 546", len(b))
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
	room := frame3Max - 2 - record3Fixed
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
	if _, err := appendFrame3(nil, f3Fence, make([]byte, frame3Max)); !errors.Is(err, ErrLimit) {
		t.Fatalf("generic frame over the limit: %v", err)
	}
	// A reader refuses a declared length above the limit or below the minimum, even with a
	// matching checksum.
	for _, n := range []uint32{0, 1, frame3Max + 1, math.MaxUint32} {
		b := make([]byte, frame3Prefix+2)
		binary.LittleEndian.PutUint32(b, n)
		sealFrame3(b, 0)
		if _, _, _, err := readFrame3(b); !errors.Is(err, errFrame3Invalid) {
			t.Fatalf("declared length %d: %v", n, err)
		}
	}
	// Non-zero flags are refused even when the checksum is right.
	b := mustFrame3(t, f3Fence, nil)
	b[9] = 1
	sealFrame3(b, 0)
	if _, _, _, err := readFrame3(b); !errors.Is(err, errFrame3Invalid) {
		t.Fatalf("flags: %v", err)
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
	if err != nil || typ != f3Fence || len(body) != 0 || size != frame3Prefix+2 {
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
	for _, tm := range []time.Time{time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)} {
		if _, err := timeToNanos(tm); err == nil {
			t.Fatalf("%v was accepted", tm)
		}
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
		if size > len(b) || size < frame3Prefix+2 {
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
