package dostoevsky

import (
	"bytes"
	"encoding/base64"
	"math"
	"time"
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
// The scanner accepts a frame only if json.Unmarshal would accept it, so Open, dost-repair and Read agree on what is
// damaged. Value is therefore decoded (into a buffer that is reused, and thrown away) and Time is parsed.
type recordScanner struct {
	topic string // the previous frame's topic, so a repeated topic allocates nothing
	value []byte // scratch for decoding values
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
	if number > math.MaxInt { // a 32-bit build: leave it to encoding/json, which refuses it
		return p, 0, "", false
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
		// The decoder skips line breaks, which unescaped in a JSON string are not JSON; every other byte that JSON would
		// refuse here, or that needs unescaping, is outside the alphabet and fails the decode.
		if bytes.IndexByte(value, '\n') >= 0 || bytes.IndexByte(value, '\r') >= 0 {
			return p, 0, "", false
		}
		if n := base64.StdEncoding.DecodedLen(len(value)); cap(s.value) < n {
			s.value = make([]byte, n)
		}
		if _, err := base64.StdEncoding.Decode(s.value[:cap(s.value)], value); err != nil {
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
	quoted := i - 1
	if _, i, ok = plainString(b, i); !ok {
		return
	}
	var at time.Time
	if at.UnmarshalJSON(b[quoted:i]) != nil {
		return p, 0, "", false
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
