// Package wire is the byte layout of the Dostoevsky wire protocol, version 1, as specified in docs/wire-v1.md.
// It encodes and decodes the handshake and frames for both sides and enforces the protocol's bounds. It does no
// networking apart from reading a HELLO; package wireserver serves a store over it.
//
// Decoded Value slices alias the buffer they were decoded from.
package wire

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

const (
	Version = 1

	MaxValue = 1 << 20
	MaxKey   = 4096
	MaxID    = 256
	MaxName  = 96
	MaxToken = 4096
	MinToken = 16
	MaxWait  = 30000 // milliseconds
	MaxBody  = MaxValue + 8<<10

	HeaderSize  = 10
	WelcomeSize = 17

	// MaxHello is the most a server reads before authentication.
	MaxHello = 10 + MaxToken
)

const magic = "DSTV"

// Type is a frame type.
type Type uint8

const (
	Publish  Type = 0x01
	Read     Type = 0x02
	Position Type = 0x03
	Commit   Type = 0x04
	Ping     Type = 0x05

	replyBit Type = 0x80

	PublishOK  = Publish | replyBit
	ReadOK     = Read | replyBit
	PositionOK = Position | replyBit
	CommitOK   = Commit | replyBit
	PingOK     = Ping | replyBit

	Error Type = 0xFF
)

// Code is an error code carried by ERROR frames and WELCOME. Zero is success (WELCOME only).
type Code uint8

const (
	OK Code = iota
	Invalid
	Unauthorized
	Conflict
	WrongOwner
	Expired
	Limit
	Closed
	Timeout
	Canceled
	StorageFailure
	Protocol
	UnsupportedVersion
)

// known reports whether c is an error code of version 1.
func (c Code) known() bool { return c >= Invalid && c <= UnsupportedVersion }

var codeNames = [...]string{"ok", "invalid", "unauthorized", "conflict", "wrong_owner", "expired", "limit", "closed",
	"timeout", "canceled", "storage_failure", "protocol", "unsupported_version"}

func (c Code) String() string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return "code_" + strconv.Itoa(int(c))
}

var (
	// ErrProtocol is a handshake or frame that does not parse. The connection cannot continue.
	ErrProtocol = errors.New("wire: protocol error")
	// ErrTooLong is a frame announced longer than MaxBody, known from its header. The connection cannot continue.
	ErrTooLong = errors.New("wire: frame longer than the maximum")
)

// RequestError is a well-formed request that breaks a value rule. The connection continues.
type RequestError struct{ Code Code }

func (e *RequestError) Error() string { return "wire: request " + e.Code.String() }

func refuse(c Code) error { return &RequestError{c} }

// Header is the fixed part of a frame.
type Header struct {
	Length    uint32
	Type      Type
	Flags     uint8
	RequestID uint32
}

// ParseHeader reads the first HeaderSize bytes of b and checks what can be checked before the body is read or
// allocated.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrProtocol
	}
	h := Header{binary.LittleEndian.Uint32(b), Type(b[4]), b[5], binary.LittleEndian.Uint32(b[6:])}
	if h.Length > MaxBody {
		return h, ErrTooLong
	}
	if h.Flags != 0 {
		return h, ErrProtocol
	}
	return h, nil
}

// Request is a frame from the client. Which fields mean something depends on Type: Value is the published value or
// the committed state, Offset is the offset read or the expected cursor.
type Request struct {
	Type      Type
	RequestID uint32
	Topic     string
	Partition uint16
	Offset    uint64
	WaitMS    uint16
	Group     string
	Key       string
	MessageID string
	Value     []byte
}

// Reply is a frame from the server. Offset is the published or read offset, or the next position. Value is the record
// value or the group state. Found, Time, Key and MessageID belong to ReadOK; Code to Error.
type Reply struct {
	Type      Type
	RequestID uint32
	Partition uint16
	Offset    uint64
	Found     bool
	Time      int64 // nanoseconds since the Unix epoch
	Key       string
	MessageID string
	Value     []byte
	Code      Code
}

// body reads fields in order; any overrun is remembered and reported once.
type body struct {
	b   []byte
	bad bool
}

func (r *body) take(n int) []byte {
	if r.bad || n > len(r.b) {
		r.bad = true
		return nil
	}
	out := r.b[:n:n]
	r.b = r.b[n:]
	return out
}

func (r *body) u8() uint8 {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *body) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}

func (r *body) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

func (r *body) str8() string  { return string(r.take(int(r.u8()))) }
func (r *body) str16() string { return string(r.take(int(r.u16()))) }
func (r *body) rest() []byte  { return r.take(len(r.b)) }

// done is true when every field was there and nothing is left over.
func (r *body) done() bool { return !r.bad && len(r.b) == 0 }

func validName(s string) bool {
	if len(s) < 1 || len(s) > MaxName {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// check applies the value rules to a structurally sound request.
func (q *Request) check() error {
	if q.Type == Ping {
		return nil
	}
	if !validName(q.Topic) {
		return refuse(Invalid)
	}
	switch q.Type {
	case Publish:
		if len(q.Key) > MaxKey || len(q.MessageID) > MaxID || len(q.Value) > MaxValue {
			return refuse(Limit)
		}
		if !utf8.ValidString(q.Key) || !utf8.ValidString(q.MessageID) {
			return refuse(Invalid)
		}
	case Read:
		if q.Offset >= 1<<63 || q.WaitMS > MaxWait {
			return refuse(Invalid)
		}
	case Position:
		if !validName(q.Group) {
			return refuse(Invalid)
		}
	case Commit:
		if !validName(q.Group) || q.Offset >= 1<<63 {
			return refuse(Invalid)
		}
		if q.Offset == 1<<63-1 || len(q.Value) > MaxValue {
			return refuse(Limit)
		}
	}
	return nil
}

// DecodeRequest decodes the body that follows h. ErrProtocol ends the connection; a *RequestError is answered with an
// ERROR for the returned request's RequestID.
func DecodeRequest(h Header, b []byte) (Request, error) {
	q := Request{Type: h.Type, RequestID: h.RequestID}
	if h.RequestID == 0 || h.Flags != 0 || len(b) > MaxBody || uint64(len(b)) != uint64(h.Length) {
		return q, ErrProtocol
	}
	r := &body{b: b}
	switch h.Type {
	case Publish:
		q.Topic, q.Key, q.MessageID, q.Value = r.str8(), r.str16(), r.str16(), r.rest()
	case Read:
		q.Topic, q.Partition, q.Offset, q.WaitMS = r.str8(), r.u16(), r.u64(), r.u16()
	case Position:
		q.Topic, q.Partition, q.Group = r.str8(), r.u16(), r.str8()
	case Commit:
		q.Topic, q.Partition, q.Group, q.Offset, q.Value = r.str8(), r.u16(), r.str8(), r.u64(), r.rest()
	case Ping:
	default:
		return q, ErrProtocol
	}
	if !r.done() {
		return q, ErrProtocol
	}
	return q, q.check()
}

func appendStr8(b []byte, s string) []byte { return append(append(b, uint8(len(s))), s...) }

func appendStr16(b []byte, s string) []byte {
	return append(binary.LittleEndian.AppendUint16(b, uint16(len(s))), s...)
}

// begin appends a header with a zero length; end fills the length in.
func begin(dst []byte, t Type, id uint32) []byte {
	dst = append(dst, 0, 0, 0, 0, byte(t), 0)
	return binary.LittleEndian.AppendUint32(dst, id)
}

func end(dst []byte, start int) []byte {
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(dst)-start-HeaderSize))
	return dst
}

// AppendRequest appends q as a frame, refusing with a *RequestError what the rules forbid.
func AppendRequest(dst []byte, q Request) ([]byte, error) {
	if q.RequestID == 0 || q.Type < Publish || q.Type > Ping {
		return dst, ErrProtocol
	}
	if err := q.check(); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = begin(dst, q.Type, q.RequestID)
	if q.Type != Ping {
		dst = appendStr8(dst, q.Topic)
	}
	switch q.Type {
	case Publish:
		dst = appendStr16(appendStr16(dst, q.Key), q.MessageID)
		dst = append(dst, q.Value...)
	case Read:
		dst = binary.LittleEndian.AppendUint16(dst, q.Partition)
		dst = binary.LittleEndian.AppendUint64(dst, q.Offset)
		dst = binary.LittleEndian.AppendUint16(dst, q.WaitMS)
	case Position:
		dst = appendStr8(binary.LittleEndian.AppendUint16(dst, q.Partition), q.Group)
	case Commit:
		dst = appendStr8(binary.LittleEndian.AppendUint16(dst, q.Partition), q.Group)
		dst = binary.LittleEndian.AppendUint64(dst, q.Offset)
		dst = append(dst, q.Value...)
	}
	return end(dst, start), nil
}

// DecodeReply decodes the body that follows h. Whether the reply answers an outstanding request of the matching type
// is the client's to check. Any error ends the connection.
func DecodeReply(h Header, b []byte) (Reply, error) {
	p := Reply{Type: h.Type, RequestID: h.RequestID}
	if h.Flags != 0 || len(b) > MaxBody || uint64(len(b)) != uint64(h.Length) || (h.RequestID == 0 && h.Type != Error) {
		return p, ErrProtocol
	}
	r := &body{b: b}
	switch h.Type {
	case PublishOK:
		p.Partition, p.Offset = r.u16(), r.u64()
	case ReadOK:
		found := r.u8()
		if found > 1 {
			return p, ErrProtocol
		}
		if p.Found = found == 1; p.Found {
			p.Partition, p.Offset, p.Time = r.u16(), r.u64(), int64(r.u64())
			p.Key, p.MessageID, p.Value = r.str16(), r.str16(), r.rest()
			if len(p.Key) > MaxKey || len(p.MessageID) > MaxID || len(p.Value) > MaxValue || !utf8.ValidString(p.Key) || !utf8.ValidString(p.MessageID) {
				return p, ErrProtocol
			}
		}
	case PositionOK:
		p.Offset, p.Value = r.u64(), r.rest()
		if len(p.Value) > MaxValue {
			return p, ErrProtocol
		}
	case CommitOK, PingOK:
	case Error:
		if p.Code = Code(r.u8()); !p.Code.known() {
			return p, ErrProtocol
		}
	default:
		return p, ErrProtocol
	}
	if !r.done() || p.Offset >= 1<<63 {
		return p, ErrProtocol
	}
	return p, nil
}

// AppendReply appends p as a frame. It refuses what DecodeReply would reject.
func AppendReply(dst []byte, p Reply) ([]byte, error) {
	if (p.RequestID == 0 && p.Type != Error) || p.Offset >= 1<<63 {
		return dst, ErrProtocol
	}
	if len(p.Key) > MaxKey || len(p.MessageID) > MaxID || len(p.Value) > MaxValue || !utf8.ValidString(p.Key) || !utf8.ValidString(p.MessageID) {
		return dst, ErrProtocol
	}
	start := len(dst)
	out := begin(dst, p.Type, p.RequestID)
	switch p.Type {
	case PublishOK:
		out = binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint16(out, p.Partition), p.Offset)
	case ReadOK:
		if !p.Found {
			out = append(out, 0)
			break
		}
		out = binary.LittleEndian.AppendUint16(append(out, 1), p.Partition)
		out = binary.LittleEndian.AppendUint64(out, p.Offset)
		out = binary.LittleEndian.AppendUint64(out, uint64(p.Time))
		out = appendStr16(appendStr16(out, p.Key), p.MessageID)
		out = append(out, p.Value...)
	case PositionOK:
		out = append(binary.LittleEndian.AppendUint64(out, p.Offset), p.Value...)
	case CommitOK, PingOK:
	case Error:
		if !p.Code.known() {
			return dst, ErrProtocol
		}
		out = append(out, byte(p.Code))
	default:
		return dst, ErrProtocol
	}
	return end(out, start), nil
}

// Hello is the client's first message.
type Hello struct {
	MaxVersion uint16
	Token      []byte
}

// AppendHello appends a HELLO.
func AppendHello(dst []byte, h Hello) ([]byte, error) {
	if len(h.Token) > MaxToken {
		return dst, ErrProtocol
	}
	dst = append(dst, magic...)
	dst = binary.LittleEndian.AppendUint16(dst, h.MaxVersion)
	dst = binary.LittleEndian.AppendUint16(dst, 0)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(h.Token)))
	return append(dst, h.Token...), nil
}

// ReadHello reads one HELLO and never more than MaxHello bytes. ErrProtocol means: close without a reply. The caller
// sets the authentication deadline.
func ReadHello(r io.Reader) (Hello, error) {
	var fixed [10]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return Hello{}, err
	}
	n := int(binary.LittleEndian.Uint16(fixed[8:]))
	if string(fixed[:4]) != magic || binary.LittleEndian.Uint16(fixed[6:]) != 0 || n > MaxToken {
		return Hello{}, ErrProtocol
	}
	h := Hello{MaxVersion: binary.LittleEndian.Uint16(fixed[4:]), Token: make([]byte, n)}
	if _, err := io.ReadFull(r, h.Token); err != nil {
		return Hello{}, err
	}
	return h, nil
}

// Authenticate answers a HELLO: OK, Unauthorized or UnsupportedVersion, the token checked first. It compares SHA-256
// digests in constant time, so the comparison does not show where the tokens differ. Hashing them first still takes
// time that grows with each token's length.
func Authenticate(h Hello, token []byte) Code {
	offered, want := sha256.Sum256(h.Token), sha256.Sum256(token)
	same := subtle.ConstantTimeCompare(offered[:], want[:]) == 1
	if !same || len(h.Token) < MinToken {
		return Unauthorized
	}
	if h.MaxVersion < 1 {
		return UnsupportedVersion
	}
	return OK
}

// Welcome is the server's answer to HELLO.
type Welcome struct {
	Status      Code
	Version     uint16
	MaxBody     uint32
	Window      uint16
	WindowBytes uint32
}

// AppendWelcome appends a WELCOME. With a status other than OK every later field is sent as zero.
func AppendWelcome(dst []byte, w Welcome) []byte {
	if w.Status != OK {
		w = Welcome{Status: w.Status}
	}
	dst = append(append(dst, magic...), byte(w.Status))
	dst = binary.LittleEndian.AppendUint16(dst, w.Version)
	dst = binary.LittleEndian.AppendUint32(dst, w.MaxBody)
	dst = binary.LittleEndian.AppendUint16(dst, w.Window)
	return binary.LittleEndian.AppendUint32(dst, w.WindowBytes)
}

// ParseWelcome decodes exactly WelcomeSize bytes. Use Accept before sending anything else.
func ParseWelcome(b []byte) (Welcome, error) {
	if len(b) != WelcomeSize || string(b[:4]) != magic {
		return Welcome{}, ErrProtocol
	}
	return Welcome{Code(b[4]), binary.LittleEndian.Uint16(b[5:]), binary.LittleEndian.Uint32(b[7:]),
		binary.LittleEndian.Uint16(b[11:]), binary.LittleEndian.Uint32(b[13:])}, nil
}

// Accept is the client's validation of a WELCOME, given the highest version it offered. A version this package cannot
// decode is not accepted, whatever was offered. A refusal by the server (a
// known status with every later field zero) is a *RequestError with that status; any other unacceptable WELCOME is
// ErrProtocol. Both end the connection.
func (w Welcome) Accept(offered uint16) error {
	if w.Status != OK {
		if !w.Status.known() || w != (Welcome{Status: w.Status}) {
			return ErrProtocol
		}
		return refuse(w.Status)
	}
	if w.Version < 1 || w.Version > offered || w.Version > Version || w.MaxBody != MaxBody || w.Window < 1 || w.WindowBytes < MaxBody+HeaderSize {
		return ErrProtocol
	}
	return nil
}
