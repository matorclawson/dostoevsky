package dostoevsky

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

// Binary frame, format version 3 (design v2: DESIGN-reopen-v2-segmented-binary-log.md).
//
// This file is the codec only. Nothing in the engine writes or reads these frames yet;
// the segmented log that will is a later change.
//
//	0   4  length: the bytes that follow the 8-byte prefix (type, flags and body)
//	4   4  CRC-32C (Castagnoli) of the length field, the type, the flags and the body
//	8   1  type
//	9   1  flags (reserved, must be zero)
//	10  .. body
//
// The checksum covers the length, so a damaged length is caught by the frame's own CRC
// rather than by a separate header checksum. All integers are little-endian. A frame is
// at most frame3Max bytes after the prefix; the writer refuses to produce more and the
// reader refuses to accept more.
const (
	frame3Prefix = 8
	frame3Max    = maxFrame // same limit as the JSON frame: 4 MiB

	f3Record   byte = 1
	f3Commit   byte = 2
	f3TopicDef byte = 4
	f3Fence    byte = 5
)

var (
	errFrame3Short    = errors.New("frame3: truncated")
	errFrame3Checksum = errors.New("frame3: checksum mismatch")
	errFrame3Invalid  = errors.New("frame3: invalid frame")
)

// appendFrame3 appends one frame holding body to dst.
func appendFrame3(dst []byte, typ byte, body []byte) ([]byte, error) {
	if 2+len(body) > frame3Max {
		return dst, ErrLimit
	}
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, typ, 0)
	binary.LittleEndian.PutUint32(dst[start:], uint32(2+len(body)))
	dst = append(dst, body...)
	return sealFrame3(dst, start), nil
}

// sealFrame3 fills in the checksum of the frame that starts at dst[start:].
func sealFrame3(dst []byte, start int) []byte {
	f := dst[start:]
	crc := crc32.Update(0, crcTable, f[:4])
	crc = crc32.Update(crc, crcTable, f[frame3Prefix:])
	binary.LittleEndian.PutUint32(f[4:], crc)
	return dst
}

// readFrame3 checks the frame at the start of b and returns its type, its body (a view
// into b) and the frame's total size. It never reads past b and never allocates.
func readFrame3(b []byte) (typ byte, body []byte, size int, err error) {
	if len(b) < frame3Prefix+2 {
		return 0, nil, 0, errFrame3Short
	}
	n := int(binary.LittleEndian.Uint32(b))
	if n < 2 || n > frame3Max {
		return 0, nil, 0, fmt.Errorf("%w: length %d", errFrame3Invalid, n)
	}
	size = frame3Prefix + n
	if len(b) < size {
		return 0, nil, 0, errFrame3Short
	}
	crc := crc32.Update(0, crcTable, b[:4])
	crc = crc32.Update(crc, crcTable, b[frame3Prefix:size])
	if crc != binary.LittleEndian.Uint32(b[4:]) {
		return 0, nil, 0, errFrame3Checksum
	}
	if b[9] != 0 {
		return 0, nil, 0, fmt.Errorf("%w: flags %#x", errFrame3Invalid, b[9])
	}
	return b[8], b[frame3Prefix+2 : size], size, nil
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

// timeToNanos converts to the frame's time field. The zero time is kept as a sentinel; a
// time outside the range int64 nanoseconds can hold (years 1678 to 2262) is refused.
func timeToNanos(t time.Time) (int64, error) {
	if t.IsZero() {
		return math.MinInt64, nil
	}
	if y := t.Year(); y < 1678 || y > 2261 {
		return 0, fmt.Errorf("time %v is outside the range of a nanosecond timestamp", t)
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
	if 2+body > frame3Max {
		return dst, ErrLimit
	}
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, f3Record, 0)
	binary.LittleEndian.PutUint32(dst[start:], uint32(2+body))
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
	if 2+body > frame3Max {
		return dst, ErrLimit
	}
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, f3Commit, 0)
	binary.LittleEndian.PutUint32(dst[start:], uint32(2+body))
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
