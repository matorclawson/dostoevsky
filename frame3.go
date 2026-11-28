package dostoevsky

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"slices"
	"time"
)

// Binary frame, format version 3 (design v2: DESIGN-reopen-v2-segmented-binary-log.md).
//
// OpenSegmented uses these frames for journals and checkpoints.
//
//	0   4  length of the body (u32)
//	4   1  type
//	5   1  flags (reserved, must be zero)
//	6   4  header check: CRC-32C of bytes 0 to 5
//	10  4  body check: CRC-32C of the body, seeded with the header check
//	14  .. body
//
// The header check lets a reader trust the length before it has the body. That matters at the
// end of a file: "the data ends inside this frame" is a torn append, safe to truncate, only if
// the length that says so is itself verified; a damaged length field must never look like a torn
// tail, or truncating it would destroy acknowledged data. So:
//
//   - fewer than 14 bytes left: errFrame3Short (a torn header; at most a partial header is lost);
//   - a complete header whose check fails: errFrame3Header (corruption, never a torn tail);
//   - a verified header declaring more body than the data holds: errFrame3Short (a torn append);
//   - a complete frame whose body check fails: errFrame3Checksum (corruption).
//
// The body check is seeded with the header check, so a body cannot be moved under another
// valid header. All integers are little-endian. A body is at most frame3Max bytes; the writer
// refuses to produce more and the reader refuses a declared length above it.
const (
	frame3Header = 14
	frame3Max    = maxFrame // same limit as the JSON frame: 4 MiB of body

	f3Record   byte = 1
	f3Commit   byte = 2
	f3TopicDef byte = 4
	f3Fence    byte = 5
)

var (
	errFrame3Short    = errors.New("frame3: truncated")
	errFrame3Header   = errors.New("frame3: header check failed")
	errFrame3Checksum = errors.New("frame3: checksum mismatch")
	errFrame3Invalid  = errors.New("frame3: invalid frame")
)

// appendFrame3 appends one frame holding body to dst.
func appendFrame3(dst []byte, typ byte, body []byte) ([]byte, error) {
	if len(body) > frame3Max {
		return dst, ErrLimit
	}
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, typ, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(body)))
	dst = append(dst, body...)
	return sealFrame3(dst, start), nil
}

// sealFrame3 fills in the two checks of the frame that starts at dst[start:].
func sealFrame3(dst []byte, start int) []byte {
	f := dst[start:]
	hcrc := crc32.Checksum(f[:6], crcTable)
	binary.LittleEndian.PutUint32(f[6:], hcrc)
	binary.LittleEndian.PutUint32(f[10:], crc32.Update(hcrc, crcTable, f[frame3Header:]))
	return dst
}

// readFrame3 checks the frame at the start of b and returns its type, its body (a view
// into b) and the frame's total size. It never reads past b and never allocates.
func readFrame3(b []byte) (typ byte, body []byte, size int, err error) {
	if len(b) < frame3Header {
		return 0, nil, 0, errFrame3Short
	}
	hcrc := crc32.Checksum(b[:6], crcTable)
	if hcrc != binary.LittleEndian.Uint32(b[6:]) {
		return 0, nil, 0, errFrame3Header
	}
	n := int(binary.LittleEndian.Uint32(b))
	if n > frame3Max || n < 0 {
		return 0, nil, 0, fmt.Errorf("%w: length %d", errFrame3Invalid, n)
	}
	if b[5] != 0 {
		return 0, nil, 0, fmt.Errorf("%w: flags %#x", errFrame3Invalid, b[5])
	}
	size = frame3Header + n
	if len(b) < size {
		return 0, nil, 0, errFrame3Short // the header is verified, so the data really does end inside this frame
	}
	if crc32.Update(hcrc, crcTable, b[frame3Header:size]) != binary.LittleEndian.Uint32(b[10:]) {
		return 0, nil, 0, errFrame3Checksum
	}
	return b[4], b[frame3Header:size], size, nil
}

// ----- record -----

// record3 is a Record frame's content. Key, ID and Value alias the buffer they were
// decoded from.
type record3 struct {
	Partition uint32 // the partition's id; a topic-define frame maps it to a topic and number
	Offset    uint64
	Time      int64 // Unix nanoseconds; math.MinInt64 stands for the zero time.Time
	Key       []byte
	ID        []byte
	Value     []byte
}

const record3Fixed = 4 + 8 + 8 + 2 + 2

// The frame's time field is Unix nanoseconds in an int64. math.MinInt64 is reserved as the
// zero-time sentinel, so the instants a frame can hold are exactly those from
// 1677-09-21 00:12:43.145224193 UTC (MinInt64+1 ns) through 2262-04-11 23:47:16.854775807 UTC
// (MaxInt64 ns), inclusive, in any time.Location: only the instant matters, and it comes back
// as UTC.
var (
	minFrameTime = time.Unix(0, math.MinInt64+1).UTC()
	maxFrameTime = time.Unix(0, math.MaxInt64).UTC()
)

// timeToNanos converts to the frame's time field. The zero time is kept as the sentinel; an
// instant outside [minFrameTime, maxFrameTime] is refused.
func timeToNanos(t time.Time) (int64, error) {
	if t.IsZero() {
		return math.MinInt64, nil
	}
	if t.Before(minFrameTime) || t.After(maxFrameTime) {
		return 0, fmt.Errorf("time %v is outside the range of a nanosecond timestamp (%v to %v)", t, minFrameTime, maxFrameTime)
	}
	return t.UnixNano(), nil
}

func nanosToTime(n int64) time.Time {
	if n == math.MinInt64 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// appendRecord3 appends a Record frame. Key and ID are limited to 65,535 bytes (the
// engine's own limits are far lower); the frame to frame3Max.
func appendRecord3(dst []byte, r record3) ([]byte, error) {
	if len(r.Key) > math.MaxUint16 || len(r.ID) > math.MaxUint16 {
		return dst, ErrLimit
	}
	body := record3Fixed + len(r.Key) + len(r.ID) + len(r.Value)
	if body > frame3Max {
		return dst, ErrLimit
	}
	dst = slices.Grow(dst, frame3Header+body)
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, f3Record, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(dst[start:], uint32(body))
	dst = binary.LittleEndian.AppendUint32(dst, r.Partition)
	dst = binary.LittleEndian.AppendUint64(dst, r.Offset)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(r.Time))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(r.Key)))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(r.ID)))
	dst = append(dst, r.Key...)
	dst = append(dst, r.ID...)
	dst = append(dst, r.Value...)
	return sealFrame3(dst, start), nil
}

// parseRecord3 decodes a Record frame's body. The value takes whatever follows the key
// and the ID, so a body whose declared lengths exceed it is refused.
func parseRecord3(body []byte) (record3, error) {
	var r record3
	if len(body) < record3Fixed {
		return r, fmt.Errorf("%w: record body of %d bytes", errFrame3Invalid, len(body))
	}
	r.Partition = binary.LittleEndian.Uint32(body)
	r.Offset = binary.LittleEndian.Uint64(body[4:])
	r.Time = int64(binary.LittleEndian.Uint64(body[12:]))
	k, id := int(binary.LittleEndian.Uint16(body[20:])), int(binary.LittleEndian.Uint16(body[22:]))
	rest := body[record3Fixed:]
	if k+id > len(rest) {
		return record3{}, fmt.Errorf("%w: key %d and id %d exceed the %d bytes that follow", errFrame3Invalid, k, id, len(rest))
	}
	r.Key, r.ID, r.Value = rest[:k:k], rest[k:k+id:k+id], rest[k+id:]
	return r, nil
}

// ----- commit -----

// commit3 is a consumer-group commit: the group's next offset for a partition, the
// optional offset the commit expects the group to be at, and the processor state.
type commit3 struct {
	Partition   uint32
	Group       []byte
	Next        uint64
	HasExpected bool
	Expected    uint64
	State       []byte
}

func appendCommit3(dst []byte, c commit3) ([]byte, error) {
	if len(c.Group) > math.MaxUint16 {
		return dst, ErrLimit
	}
	body := 4 + 2 + len(c.Group) + 8 + 1 + 8 + len(c.State)
	if body > frame3Max {
		return dst, ErrLimit
	}
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, f3Commit, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(dst[start:], uint32(body))
	dst = binary.LittleEndian.AppendUint32(dst, c.Partition)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(c.Group)))
	dst = append(dst, c.Group...)
	dst = binary.LittleEndian.AppendUint64(dst, c.Next)
	if c.HasExpected {
		dst = append(dst, 1)
		dst = binary.LittleEndian.AppendUint64(dst, c.Expected)
	} else {
		dst = append(dst, 0)
		dst = binary.LittleEndian.AppendUint64(dst, 0) // fixed width: Expected is 0 when absent
	}
	dst = append(dst, c.State...)
	return sealFrame3(dst, start), nil
}

func parseCommit3(body []byte) (commit3, error) {
	var c commit3
	if len(body) < 4+2 {
		return c, fmt.Errorf("%w: commit body of %d bytes", errFrame3Invalid, len(body))
	}
	c.Partition = binary.LittleEndian.Uint32(body)
	g := int(binary.LittleEndian.Uint16(body[4:]))
	rest := body[6:]
	if len(rest) < g+8+1+8 {
		return commit3{}, fmt.Errorf("%w: commit group of %d bytes exceeds the body", errFrame3Invalid, g)
	}
	c.Group = rest[:g:g]
	rest = rest[g:]
	c.Next = binary.LittleEndian.Uint64(rest)
	switch rest[8] {
	case 0:
		if binary.LittleEndian.Uint64(rest[9:]) != 0 {
			return commit3{}, fmt.Errorf("%w: absent expected offset is not zero", errFrame3Invalid)
		}
	case 1:
		c.HasExpected = true
		c.Expected = binary.LittleEndian.Uint64(rest[9:])
	default:
		return commit3{}, fmt.Errorf("%w: expected-offset flag %d", errFrame3Invalid, rest[8])
	}
	c.State = rest[17:]
	return c, nil
}

// ----- topic definition -----

// topicDef3 maps a partition id to its topic and number, once, so a record frame
// carries a small integer instead of a name.
type topicDef3 struct {
	Partition uint32
	Number    uint32
	Topic     []byte
}

func appendTopicDef3(dst []byte, d topicDef3) ([]byte, error) {
	if len(d.Topic) > math.MaxUint16 {
		return dst, ErrLimit
	}
	body := make([]byte, 0, 4+4+len(d.Topic))
	body = binary.LittleEndian.AppendUint32(body, d.Partition)
	body = binary.LittleEndian.AppendUint32(body, d.Number)
	body = append(body, d.Topic...)
	return appendFrame3(dst, f3TopicDef, body)
}

func parseTopicDef3(body []byte) (topicDef3, error) {
	if len(body) < 8 {
		return topicDef3{}, fmt.Errorf("%w: topic definition of %d bytes", errFrame3Invalid, len(body))
	}
	return topicDef3{binary.LittleEndian.Uint32(body), binary.LittleEndian.Uint32(body[4:]), body[8:]}, nil
}
