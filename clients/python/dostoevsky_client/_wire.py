"""The byte layout of the Dostoevsky wire protocol, version 1 (docs/wire-v1.md). No I/O: what a client sends is
encoded here and what a server sends is decoded here."""
import datetime
import struct

MAX_BODY = 1056768
MAX_VALUE = 1 << 20
MAX_KEY = 4096
MAX_ID = 256
MAX_TOKEN = 4096
MAX_WAIT_MS = 30000
HEADER = 10
WELCOME_BYTES = 17

PUBLISH, READ, POSITION, COMMIT, PING, ERROR = 0x01, 0x02, 0x03, 0x04, 0x05, 0xFF
REPLY = 0x80

# Error codes, with the HTTP status the gateway uses for the same name.
CODES = {
    1: ("invalid", 400), 2: ("unauthorized", 401), 3: ("conflict", 409), 4: ("wrong_owner", 409), 5: ("expired", 410),
    6: ("limit", 413), 7: ("closed", 503), 8: ("timeout", 504), 9: ("canceled", 408), 10: ("storage_failure", 500),
    11: ("protocol", 400), 12: ("unsupported_version", 400),
}


class Refused(ValueError):
    """A value this client refuses to send; `code` is "invalid" or "limit"."""

    def __init__(self, code: str, what: str):
        super().__init__(f"{code}: {what}")
        self.code = code


class Malformed(Exception):
    """Bytes from the server that do not follow the protocol."""


def _text(value, limit: int, what: str) -> bytes:
    if not isinstance(value, str):
        raise Refused("invalid", what)
    try:
        data = value.encode()  # a lone surrogate cannot be encoded
    except UnicodeError:
        raise Refused("invalid", what) from None
    if len(data) > limit:
        raise Refused("limit", what)
    return data


def _bytes(value, limit: int, what: str):
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise Refused("invalid", f"{what} must be bytes")
    view = memoryview(value).cast("B")
    if len(view) > limit:
        raise Refused("limit", what)
    return view


def _int(value, limit: int, what: str) -> int:
    if not isinstance(value, int) or not 0 <= value <= limit:
        raise Refused("invalid", what)
    return value


def _str8(data: bytes) -> bytes:
    return struct.pack("<B", len(data)) + data


def _str16(data: bytes) -> bytes:
    return struct.pack("<H", len(data)) + data


def _frame(kind: int, *parts) -> bytearray:
    """A new buffer (the caller's values are copied) with request id 0: `set_id` writes it."""
    out = bytearray(HEADER)
    for part in parts:
        out += part
    if len(out) - HEADER > MAX_BODY:
        raise Refused("limit", "frame")
    struct.pack_into("<IB", out, 0, len(out) - HEADER, kind)
    return out


def set_id(frame: bytearray, request_id: int) -> bytearray:
    struct.pack_into("<I", frame, 6, request_id)
    return frame


def hello(token: str, max_version: int = 1) -> bytes:
    data = _text(token, MAX_TOKEN, "token")
    return b"DSTV" + struct.pack("<HHH", max_version, 0, len(data)) + data


def publish(topic, value, key="", id="") -> bytearray:
    return _frame(PUBLISH, _str8(_text(topic, 255, "topic")), _str16(_text(key, MAX_KEY, "key")),
                  _str16(_text(id, MAX_ID, "id")), _bytes(value, MAX_VALUE, "value"))


def read(topic, partition, offset, wait_ms=0) -> bytearray:
    return _frame(READ, _str8(_text(topic, 255, "topic")), struct.pack(
        "<HQH", _int(partition, 65535, "partition"), _int(offset, (1 << 63) - 1, "offset"),
        _int(wait_ms, MAX_WAIT_MS, "wait_ms")))


def position(topic, partition, group) -> bytearray:
    return _frame(POSITION, _str8(_text(topic, 255, "topic")), struct.pack("<H", _int(partition, 65535, "partition")),
                  _str8(_text(group, 255, "group")))


def commit(topic, partition, group, expected, state=b"") -> bytearray:
    return _frame(COMMIT, _str8(_text(topic, 255, "topic")), struct.pack("<H", _int(partition, 65535, "partition")),
                  _str8(_text(group, 255, "group")), struct.pack("<Q", _int(expected, (1 << 63) - 1, "expected")),
                  _bytes(state, MAX_VALUE, "state"))


def ping() -> bytearray:
    return _frame(PING)


def welcome(data: bytes):
    """(status, version, max_body, window, window_bytes). A refusal is a known status with every later field zero."""
    if len(data) != WELCOME_BYTES or data[:4] != b"DSTV":
        raise Malformed("not a WELCOME")
    fields = struct.unpack_from("<BHIHI", data, 4)
    if fields[0] and (fields[0] not in CODES or any(fields[1:])):
        raise Malformed("not a refusal")
    return fields


def acceptable(version: int, max_body: int, window: int, window_bytes: int) -> bool:
    """Nothing more is sent unless the whole WELCOME is usable: every legal frame must fit the byte window."""
    return version == 1 and max_body == MAX_BODY and window >= 1 and window_bytes >= MAX_BODY + HEADER


def header(data):
    """(length, type, request_id). The length is checked here, before any body is waited for."""
    length, kind, flags, request_id = struct.unpack_from("<IBBI", data)
    if length > MAX_BODY:
        raise Malformed("frame longer than max_body")
    if flags:
        raise Malformed("flags set")
    return length, kind, request_id


def _rfc3339(nanos: int) -> str:
    """UTC with nanoseconds and no trailing zeros: the text the HTTP API puts in Dostoevsky-Time."""
    seconds, rest = divmod(nanos, 1_000_000_000)
    when = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
    fraction = "." + f"{rest:09d}".rstrip("0") if rest else ""
    return when.strftime("%Y-%m-%dT%H:%M:%S") + fraction + "Z"


class _Body:
    def __init__(self, data: bytes):
        self.data, self.at = data, 0

    def take(self, n: int) -> bytes:
        if self.at + n > len(self.data):
            raise Malformed("body too short")
        out = self.data[self.at:self.at + n]
        self.at += n
        return out

    def number(self, fmt: str) -> int:
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))[0]

    def offset(self) -> int:
        value = self.number("<Q")
        if value >= 1 << 63:
            raise Malformed("offset out of range")
        return value

    def string(self, limit: int) -> str:
        n = self.number("<H")
        if n > limit:
            raise Malformed("text too long")
        try:
            return self.take(n).decode()
        except UnicodeError:
            raise Malformed("text is not UTF-8") from None

    def rest(self, limit: int) -> bytes:
        n = len(self.data) - self.at
        if n > limit:
            raise Malformed("value too long")
        return self.take(n)


def reply(kind: int, data: bytes):
    """The body of a server frame: (partition, offset) for PUBLISH, None or (partition, offset, key, id, time, value)
    for READ, (next, state) for POSITION, None for COMMIT and PING, the code for ERROR."""
    body = _Body(bytes(data))
    out = None
    if kind == REPLY | PUBLISH:
        out = (body.number("<H"), body.offset())
    elif kind == REPLY | READ:
        found = body.number("<B")
        if found > 1:
            raise Malformed("found is neither 0 nor 1")
        if found:
            partition, offset, when = body.number("<H"), body.offset(), _rfc3339(body.number("<q"))
            key, ident = body.string(MAX_KEY), body.string(MAX_ID)
            out = (partition, offset, key, ident, when, body.rest(MAX_VALUE))
    elif kind == REPLY | POSITION:
        out = (body.offset(), body.rest(MAX_VALUE))
    elif kind == ERROR:
        out = body.number("<B")
        if out not in CODES:
            raise Malformed("unknown error code")
    elif kind not in (REPLY | COMMIT, REPLY | PING):
        raise Malformed("unknown reply type")
    if body.at != len(body.data):
        raise Malformed("bytes left over")
    return out
