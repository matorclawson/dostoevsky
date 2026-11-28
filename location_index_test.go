package dostoevsky

import "testing"

func TestLocationIndexBoundaries(t *testing.T) {
	var s stream
	want := make([]location, 4097)
	at := int64(1) << 33
	for i := range want {
		if i%5 == 0 {
			at += 1 << 34
		} // other partitions or control frames
		size := 1 + i%4096
		if i%257 == 0 {
			size = maxFrame
		}
		want[i] = location{at: at, size: size}
		s.addRecordFields("", int64(i), want[i])
		at += int64(size) + 20
	}
	if n := s.count(); n != int64(len(want)) {
		t.Fatalf("count %d", n)
	}
	for i := len(want) - 1; i >= 0; i-- {
		if got := s.location(int64(i)); got != want[i] {
			t.Fatalf("offset %d: got %+v want %+v", i, got, want[i])
		}
	}
}
