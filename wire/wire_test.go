package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
)

type vector struct {
	Name   string         `json:"name"`
	Sender string         `json:"sender"`
	Hex    string         `json:"hex"`
	Fields map[string]any `json:"fields"`
	Scope  string         `json:"scope"`
	Code   int            `json:"code"`
}

type vectorFile struct {
	Limits  map[string]int `json:"limits"`
	Valid   []vector       `json:"valid"`
	Invalid []vector       `json:"invalid"`
}

func load(t testing.TB) vectorFile {
	t.Helper()
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f vectorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Valid) == 0 || len(f.Invalid) == 0 {
		t.Fatal("no vectors")
	}
	return f
}

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLimitsMatchTheVectorFile(t *testing.T) {
	l := load(t).Limits
	for name, got := range map[string]int{"max_body": MaxBody, "max_value": MaxValue, "max_key": MaxKey, "max_id": MaxID, "max_token": MaxToken, "max_wait_ms": MaxWait} {
		if l[name] != got {
			t.Errorf("%s: package %d, vectors %d", name, got, l[name])
		}
	}
}

func num(t *testing.T, v any) uint64 {
	t.Helper()
	switch x := v.(type) {
	case nil:
		return 0
	case float64:
		return uint64(x)
	case string:
		n, err := strconv.ParseUint(x, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	t.Fatalf("not a number: %v", v)
	return 0
}

func text(t *testing.T, v any) string {
	t.Helper()
	if v == nil {
		return ""
	}
	return v.(string)
}

var types = map[string]Type{"PUBLISH": Publish, "READ": Read, "POSITION": Position, "COMMIT": Commit, "PING": Ping,
	"PUBLISH_OK": PublishOK, "READ_OK": ReadOK, "POSITION_OK": PositionOK, "COMMIT_OK": CommitOK, "PING_OK": PingOK, "ERROR": Error}

// Every valid vector is decoded to its listed fields and encoded back to the same bytes.
func TestValidVectors(t *testing.T) {
	for _, v := range load(t).Valid {
		t.Run(v.Name, func(t *testing.T) {
			b, f := unhex(t, v.Hex), v.Fields
			var again []byte
			var err error
			switch {
			case v.Name == "hello":
				h, err := ReadHello(bytes.NewReader(b))
				if err != nil || uint64(h.MaxVersion) != num(t, f["max_version"]) || string(h.Token) != text(t, f["token"]) {
					t.Fatalf("%+v %v", h, err)
				}
				if again, err = AppendHello(nil, h); err != nil {
					t.Fatal(err)
				}
			case len(v.Name) >= 7 && v.Name[:7] == "welcome":
				w, err := ParseWelcome(b)
				want := Welcome{Code(num(t, f["status"])), uint16(num(t, f["version"])), uint32(num(t, f["max_body"])), uint16(num(t, f["window"])), uint32(num(t, f["window_bytes"]))}
				if err != nil || w != want {
					t.Fatalf("%+v %v", w, err)
				}
				again = AppendWelcome(nil, w)
			default:
				h, herr := ParseHeader(b)
				if herr != nil {
					t.Fatal(herr)
				}
				typ := types[text(t, f["type"])]
				if h.Type != typ || uint64(h.RequestID) != num(t, f["request_id"]) {
					t.Fatalf("header %+v", h)
				}
				value := text(t, f["value_hex"]) + text(t, f["state_hex"])
				offset := num(t, f["offset"]) + num(t, f["expected"]) + num(t, f["next"])
				key, id := string(unhex(t, text(t, f["key_hex"]))), string(unhex(t, text(t, f["id_hex"])))
				if v.Sender == "client" {
					q, derr := DecodeRequest(h, b[HeaderSize:])
					if derr != nil {
						t.Fatal(derr)
					}
					if q.Topic != text(t, f["topic"]) || q.Group != text(t, f["group"]) || uint64(q.Partition) != num(t, f["partition"]) ||
						q.Offset != offset || uint64(q.WaitMS) != num(t, f["wait_ms"]) || q.Key != key || q.MessageID != id || hex.EncodeToString(q.Value) != value {
						t.Fatalf("%+v", q)
					}
					again, err = AppendRequest(nil, q)
				} else {
					p, derr := DecodeReply(h, b[HeaderSize:])
					if derr != nil {
						t.Fatal(derr)
					}
					if uint64(p.Partition) != num(t, f["partition"]) || p.Offset != offset || p.Found != (num(t, f["found"]) == 1) ||
						uint64(p.Time) != num(t, f["time_unix_nanos"]) || p.Key != key || p.MessageID != id || hex.EncodeToString(p.Value) != value ||
						uint64(p.Code) != num(t, f["code"]) {
						t.Fatalf("%+v", p)
					}
					again, err = AppendReply(nil, p)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !bytes.Equal(again, b) {
				t.Fatalf("re-encoded as %x", again)
			}
		})
	}
}

// Every invalid vector gets the treatment the file names.
func TestInvalidVectors(t *testing.T) {
	token := []byte("example-token-not-a-secret")
	for _, v := range load(t).Invalid {
		t.Run(v.Name, func(t *testing.T) {
			b := unhex(t, v.Hex)
			switch v.Scope {
			case "close-without-reply":
				if _, err := ReadHello(bytes.NewReader(b)); !errors.Is(err, ErrProtocol) {
					t.Fatalf("%v", err)
				}
			case "welcome":
				h, err := ReadHello(bytes.NewReader(b))
				if err != nil {
					t.Fatal(err)
				}
				// A short token is refused even if it is the configured one.
				configured := token
				if len(h.Token) < MinToken {
					configured = h.Token
				}
				if got := Authenticate(h, configured); got != Code(v.Code) {
					t.Fatalf("%v", got)
				}
			case "connection":
				h, err := ParseHeader(b)
				if err == nil {
					_, err = DecodeRequest(h, b[HeaderSize:])
				}
				want := ErrProtocol
				if Code(v.Code) == Limit {
					want = ErrTooLong
				}
				if !errors.Is(err, want) {
					t.Fatalf("%v, want %v", err, want)
				}
			case "request":
				h, err := ParseHeader(b)
				if err != nil {
					t.Fatal(err)
				}
				q, err := DecodeRequest(h, b[HeaderSize:])
				var refused *RequestError
				if !errors.As(err, &refused) || refused.Code != Code(v.Code) || q.RequestID != h.RequestID {
					t.Fatalf("%v", err)
				}
			case "client-closes":
				h, err := ParseHeader(b)
				if err == nil {
					_, err = DecodeReply(h, b[HeaderSize:])
				}
				if err == nil {
					t.Fatal("accepted")
				}
			default:
				t.Fatalf("unknown scope %q", v.Scope)
			}
		})
	}
}

func TestAuthenticate(t *testing.T) {
	token := []byte("0123456789abcdef")
	for _, c := range []struct {
		hello Hello
		want  Code
	}{
		{Hello{1, token}, OK},
		{Hello{9, token}, OK},
		{Hello{0, token}, UnsupportedVersion},
		{Hello{0, []byte("0123456789abcdeX")}, Unauthorized}, // the token is checked before the version
		{Hello{1, token[:15]}, Unauthorized},
		{Hello{1, nil}, Unauthorized},
	} {
		if got := Authenticate(c.hello, token); got != c.want {
			t.Errorf("%d %q: %v, want %v", c.hello.MaxVersion, c.hello.Token, got, c.want)
		}
	}
}

func TestAClientAcceptsOnlyAUsableWelcome(t *testing.T) {
	good := Welcome{OK, 1, MaxBody, 1, MaxBody + HeaderSize}
	if err := good.Accept(1); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Welcome){
		"version zero":       func(w *Welcome) { w.Version = 0 },
		"version not asked":  func(w *Welcome) { w.Version = 2 },
		"another max_body":   func(w *Welcome) { w.MaxBody-- },
		"no window":          func(w *Welcome) { w.Window = 0 },
		"window_bytes small": func(w *Welcome) { w.WindowBytes-- },
	} {
		w := good
		change(&w)
		if err := w.Accept(1); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: %v", name, err)
		}
	}
	var refused *RequestError
	if err := (Welcome{Status: Unauthorized}).Accept(1); !errors.As(err, &refused) || refused.Code != Unauthorized {
		t.Errorf("refusal: %v", err)
	}
}

func TestTheLargestLegalFramesFit(t *testing.T) {
	long := string(bytes.Repeat([]byte("n"), MaxName))
	for _, q := range []Request{
		{Type: Publish, RequestID: 1, Topic: long, Key: string(bytes.Repeat([]byte("k"), MaxKey)), MessageID: string(bytes.Repeat([]byte("i"), MaxID)), Value: make([]byte, MaxValue)},
		{Type: Commit, RequestID: 1, Topic: long, Group: long, Value: make([]byte, MaxValue)},
	} {
		b, err := AppendRequest(nil, q)
		if err != nil || len(b)-HeaderSize > MaxBody {
			t.Fatalf("type %d: %d bytes, %v", q.Type, len(b), err)
		}
	}
	b, err := AppendReply(nil, Reply{Type: ReadOK, RequestID: 1, Found: true, Key: string(bytes.Repeat([]byte("k"), MaxKey)), MessageID: string(bytes.Repeat([]byte("i"), MaxID)), Value: make([]byte, MaxValue)})
	if err != nil || len(b)-HeaderSize > MaxBody {
		t.Fatalf("reply: %d bytes, %v", len(b), err)
	}
}

// Appending after bytes already in the buffer leaves them alone.
func TestAppendKeepsWhatIsThere(t *testing.T) {
	b, err := AppendRequest([]byte("abc"), Request{Type: Ping, RequestID: 7})
	if err != nil || string(b[:3]) != "abc" {
		t.Fatalf("%x %v", b, err)
	}
	if h, err := ParseHeader(b[3:]); err != nil || h.RequestID != 7 || h.Length != 0 {
		t.Fatalf("%+v %v", h, err)
	}
}

func seed(f *testing.F, sender string) {
	v := load(f)
	for _, x := range append(v.Valid, v.Invalid...) {
		if x.Sender == sender {
			f.Add(unhex(f, x.Hex))
		}
	}
}

// Whatever arrives: no panic, and a frame that is accepted has exactly one encoding, the one received.
func FuzzDecodeRequest(f *testing.F) {
	seed(f, "client")
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := ParseHeader(b)
		if err != nil {
			return
		}
		q, err := DecodeRequest(h, b[HeaderSize:])
		if err != nil {
			return
		}
		again, err := AppendRequest(nil, q)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted %x, re-encoded %x, %v", b, again, err)
		}
	})
}

func FuzzDecodeReply(f *testing.F) {
	seed(f, "server")
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := ParseHeader(b)
		if err != nil {
			return
		}
		p, err := DecodeReply(h, b[HeaderSize:])
		if err != nil {
			return
		}
		again, err := AppendReply(nil, p)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("accepted %x, re-encoded %x, %v", b, again, err)
		}
	})
}

func FuzzReadHello(f *testing.F) {
	seed(f, "client")
	f.Fuzz(func(t *testing.T, b []byte) {
		r := bytes.NewReader(b)
		h, err := ReadHello(r)
		if read := len(b) - r.Len(); read > MaxHello {
			t.Fatalf("read %d bytes", read)
		}
		if err != nil {
			return
		}
		again, err := AppendHello(nil, h)
		if err != nil || !bytes.Equal(again, b[:len(again)]) {
			t.Fatalf("%x: %v", again, err)
		}
	})
}
