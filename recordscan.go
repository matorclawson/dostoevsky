package dostoevsky

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

// recordScanner reads the four fields replay needs from a Record frame without
// decoding the frame. A Record frame is json.Marshal of entry{Record: &r}, so its
// shape is fixed:
//
//	{"Record":{"Topic":"t","Number":0,"Key":"k","Value":"<base64>","ID":"i","Offset":7,"Time":"..."}}
//
// (Value is null for a nil slice). The scanner accepts only exactly that shape. For
// anything else it returns ok=false and the caller decodes the frame with
// encoding/json, so a frame the scanner does not fully understand behaves exactly as
// before, including its error.
//
// Not checked here, by design: that Value is valid base64 and that Time parses.
// json.Unmarshal rejected such a frame at open; the scanner does not read them (the
// base64 decode of every value is most of the cost it removes). A frame only passes
// its CRC if a writer produced it, and the writer marshals typed values. Read decodes
// a frame completely and fails there. The scanner keeps one cheap structural check:
// the value length is a multiple of four and any '=' padding is only at the end. A control byte in the value, which
// would make the frame invalid JSON, is declined.
type recordScanner struct {
	topic string // the previous frame's topic, so a repeated topic allocates nothing
}

const recordPrefix = `{"Record":{"Topic":"`

// plainString returns the bytes of a JSON string that starts after its opening quote
// at b[i], the index after the closing quote, and whether the string is plain: no
// escape, no control byte, valid UTF-8. A plain string decodes to exactly its bytes.
func plainString(b []byte, i int) (s []byte, next int, ok bool) {
	start, high := i, false
	for ; i < len(b); i++ {
		switch c := b[i]; {
		case c == '"':
			s = b[start:i]
			if high && !utf8.Valid(s) {
				return nil, 0, false
			}
			return s, i + 1, true
		case c == '\\' || c < 0x20:
			return nil, 0, false
		case c >= 0x80:
			high = true
		}
	}
	return nil, 0, false
}

// hasControl reports whether b holds a byte below 0x20, which JSON does not allow unescaped in a string. It looks at
// eight bytes at a time because it runs over every value (the subtraction borrows into a byte's top bit only if some
// byte is below 0x20).
func hasControl(b []byte) bool {
	const ones = 0x0101010101010101
	for ; len(b) >= 8; b = b[8:] {
		x := binary.LittleEndian.Uint64(b)
		if (x-ones*0x20)&^x&(ones*0x80) != 0 {
			return true
		}
	}
	for _, c := range b {
		if c < 0x20 {
			return true
		}
	}
	return false
}

// canonicalUint reads a non-negative integer written the way encoding/json writes
// one: digits only, no sign, no leading zero, at most 18 digits (it fits an int64).
func canonicalUint(b []byte, i int) (v int64, next int, ok bool) {
	start := i
	for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		v = v*10 + int64(b[i]-'0')
		if i-start >= 18 {
			return 0, 0, false
		}
	}
	n := i - start
	if n == 0 || (n > 1 && b[start] == '0') {
		return 0, 0, false
	}
	return v, i, true
}

func expect(b []byte, i int, lit string) (int, bool) {
	if len(b)-i < len(lit) || string(b[i:i+len(lit)]) != lit {
		return 0, false
	}
	return i + len(lit), true
}

func (s *recordScanner) scan(b []byte) (p Partition, offset int64, id string, ok bool) {
	i, ok := expect(b, 0, recordPrefix)
	if !ok {
		return
	}
	topic, i, ok := plainString(b, i)
	if !ok {
		return
	}
	if i, ok = expect(b, i, `,"Number":`); !ok {
		return
	}
	var number int64
	if number, i, ok = canonicalUint(b, i); !ok {
		return
	}
	if i, ok = expect(b, i, `,"Key":"`); !ok {
		return
	}
	if _, i, ok = plainString(b, i); !ok {
		return
	}
	if i, ok = expect(b, i, `,"Value":`); !ok {
		return
	}
	if j, null := expect(b, i, `null`); null {
		i = j
	} else {
		if i, ok = expect(b, i, `"`); !ok {
			return
		}
		end := bytes.IndexByte(b[i:], '"')
		if end < 0 {
			return p, 0, "", false
		}
		value := b[i : i+end]
		if end%4 != 0 || bytes.IndexByte(value, '\\') >= 0 || hasControl(value) {
			return p, 0, "", false
		}
		if pad := bytes.IndexByte(value, '='); pad >= 0 && (pad < end-2 || bytes.Count(value[pad:], []byte("=")) != end-pad) {
			return p, 0, "", false
		}
		i += end + 1
	}
	if i, ok = expect(b, i, `,"ID":"`); !ok {
		return
	}
	var idb []byte
	if idb, i, ok = plainString(b, i); !ok {
		return
	}
	if i, ok = expect(b, i, `,"Offset":`); !ok {
		return
	}
	if offset, i, ok = canonicalUint(b, i); !ok {
		return
	}
	if i, ok = expect(b, i, `,"Time":"`); !ok {
		return
	}
	if _, i, ok = plainString(b, i); !ok {
		return
	}
	if i, ok = expect(b, i, `}}`); !ok || i != len(b) {
		return p, 0, "", false
	}
	if string(topic) != s.topic {
		s.topic = string(topic)
	}
	if len(idb) > 0 {
		id = string(idb)
	}
	return Partition{s.topic, int(number)}, offset, id, true
}
