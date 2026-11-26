// The byte layout of the Dostoevsky wire protocol, version 1 (docs/wire-v1.md). No I/O here: encoding what a client
// sends and decoding what a server sends. Internal to the package; use "dostoevsky-client/native".

export const MAX_BODY = 1056768;
export const MAX_VALUE = 1 << 20;
export const MAX_KEY = 4096;
export const MAX_ID = 256;
export const MAX_TOKEN = 4096;
export const MAX_WAIT_MS = 30000;
export const HEADER = 10;
export const WELCOME_BYTES = 17;

export const PUBLISH = 0x01, READ = 0x02, POSITION = 0x03, COMMIT = 0x04, PING = 0x05, ERROR = 0xff;
export const REPLY = 0x80;

// Error codes, with the HTTP status the gateway uses for the same name.
export const CODES = {
  1: ["invalid", 400], 2: ["unauthorized", 401], 3: ["conflict", 409], 4: ["wrong_owner", 409], 5: ["expired", 410],
  6: ["limit", 413], 7: ["closed", 503], 8: ["timeout", 504], 9: ["canceled", 408], 10: ["storage_failure", 500],
  11: ["protocol", 400], 12: ["unsupported_version", 400],
};

/** A value this client refuses to send; `reason` is "invalid" or "limit". */
export class Refused extends Error {
  constructor(reason, what) {
    super(`${reason}: ${what}`);
    this.reason = reason;
  }
}

/** Bytes from the server that do not follow the protocol. */
export class Malformed extends Error {}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

function text(value, max, what) {
  // A lone surrogate would be replaced silently by the encoder: two different strings, one key.
  if (typeof value !== "string" || /\p{Surrogate}/u.test(value)) throw new Refused("invalid", what);
  const bytes = encoder.encode(value);
  if (bytes.length > max) throw new Refused("limit", what);
  return bytes;
}

function bytes(value, max, what) {
  if (!(value instanceof Uint8Array)) throw new Refused("invalid", `${what} must be a Uint8Array`);
  if (value.length > max) throw new Refused("limit", what);
  return value;
}

function integer(value, max, what) {
  if (!Number.isInteger(value) || value < 0 || value > max) throw new Refused("invalid", what);
  return value;
}

function offset(value, what) {
  if (typeof value !== "bigint" || value < 0n || value >= 1n << 63n) throw new Refused("invalid", what);
  return value;
}

const u8 = (v) => Uint8Array.of(v);
function u16(v) { const b = new Uint8Array(2); new DataView(b.buffer).setUint16(0, v, true); return b; }
function u64(v) { const b = new Uint8Array(8); new DataView(b.buffer).setBigUint64(0, v, true); return b; }
const str8 = (b) => [u8(b.length), b];
const str16 = (b) => [u16(b.length), b];

function join(parts, headroom = 0) {
  const out = new Uint8Array(headroom + parts.reduce((n, p) => n + p.length, 0));
  let at = headroom;
  for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}

export function hello(token, maxVersion = 1) {
  const t = text(token, MAX_TOKEN, "token");
  return join([encoder.encode("DSTV"), u16(maxVersion), u16(0), u16(t.length), t]);
}

// frame returns a new buffer (the caller's values are copied), with the request id still 0: setId writes it.
function frame(type, parts) {
  const out = join(parts, HEADER);
  if (out.length - HEADER > MAX_BODY) throw new Refused("limit", "frame");
  const view = new DataView(out.buffer);
  view.setUint32(0, out.length - HEADER, true);
  view.setUint8(4, type);
  return out;
}

export function setId(buffer, id) {
  new DataView(buffer.buffer).setUint32(6, id, true);
  return buffer;
}

export const publish = (topic, value, key = "", id = "") => frame(PUBLISH, [
  ...str8(text(topic, 255, "topic")), ...str16(text(key, MAX_KEY, "key")), ...str16(text(id, MAX_ID, "id")),
  bytes(value, MAX_VALUE, "value"),
]);

export const read = (topic, partition, at, waitMs = 0) => frame(READ, [
  ...str8(text(topic, 255, "topic")), u16(integer(partition, 65535, "partition")), u64(offset(at, "offset")),
  u16(integer(waitMs, MAX_WAIT_MS, "waitMs")),
]);

export const position = (topic, partition, group) => frame(POSITION, [
  ...str8(text(topic, 255, "topic")), u16(integer(partition, 65535, "partition")), ...str8(text(group, 255, "group")),
]);

export const commit = (topic, partition, group, expected, state = new Uint8Array()) => frame(COMMIT, [
  ...str8(text(topic, 255, "topic")), u16(integer(partition, 65535, "partition")), ...str8(text(group, 255, "group")),
  u64(offset(expected, "expected")), bytes(state, MAX_VALUE, "state"),
]);

export const ping = () => frame(PING, []);

export function welcome(b) {
  if (b.length !== WELCOME_BYTES || b[0] !== 0x44 || b[1] !== 0x53 || b[2] !== 0x54 || b[3] !== 0x56) {
    throw new Malformed("not a WELCOME");
  }
  const v = new DataView(b.buffer, b.byteOffset, b.length);
  const out = {
    status: v.getUint8(4), version: v.getUint16(5, true), maxBody: v.getUint32(7, true),
    window: v.getUint16(11, true), windowBytes: v.getUint32(13, true),
  };
  if (out.status !== 0 && (!CODES[out.status] || out.version || out.maxBody || out.window || out.windowBytes)) {
    throw new Malformed("not a refusal");
  }
  return out;
}

/** The header of a server frame. The length is checked here, before any body is waited for. */
export function header(b, maxBody = MAX_BODY) {
  const v = new DataView(b.buffer, b.byteOffset, HEADER);
  const h = { length: v.getUint32(0, true), type: v.getUint8(4), flags: v.getUint8(5), id: v.getUint32(6, true) };
  if (h.length > maxBody) throw new Malformed("frame longer than max_body");
  if (h.flags !== 0) throw new Malformed("flags set");
  return h;
}

// RFC 3339 with nanoseconds and no trailing zeros, in UTC: the text the HTTP API puts in Dostoevsky-Time.
function rfc3339(nanos) {
  let seconds = nanos / 1000000000n, rest = nanos % 1000000000n;
  if (rest < 0n) { seconds -= 1n; rest += 1000000000n; }
  const fraction = rest === 0n ? "" : "." + String(rest).padStart(9, "0").replace(/0+$/, "");
  return new Date(Number(seconds) * 1000).toISOString().slice(0, 19) + fraction + "Z";
}

class Body {
  #b; #at = 0;
  constructor(b) { this.#b = b; }
  take(n) {
    if (this.#at + n > this.#b.length) throw new Malformed("body too short");
    return this.#b.subarray(this.#at, (this.#at += n));
  }
  view(n) { const p = this.take(n); return new DataView(p.buffer, p.byteOffset, n); }
  u8() { return this.view(1).getUint8(0); }
  u16() { return this.view(2).getUint16(0, true); }
  u64() { return this.view(8).getBigUint64(0, true); }
  offset() {
    const v = this.u64();
    if (v >= 1n << 63n) throw new Malformed("offset out of range");
    return v;
  }
  i64() { return this.view(8).getBigInt64(0, true); }
  string(max) {
    const b = this.take(this.u16());
    if (b.length > max) throw new Malformed("text too long");
    try { return decoder.decode(b); } catch { throw new Malformed("text is not UTF-8"); }
  }
  rest(max) {
    const n = this.#b.length - this.#at;
    if (n > max) throw new Malformed("value too long");
    return new Uint8Array(this.take(n)); // a copy: nothing pins the receive buffer
  }
  end() { if (this.#at !== this.#b.length) throw new Malformed("bytes left over"); }
}

/** The body of a server frame of the given type, in the shapes the HTTP client returns. */
export function reply(type, b) {
  const body = new Body(b);
  let out;
  switch (type) {
    case REPLY | PUBLISH:
      out = { partition: body.u16(), offset: body.offset() };
      break;
    case REPLY | READ: {
      const found = body.u8();
      if (found > 1) throw new Malformed("found is neither 0 nor 1");
      out = null;
      if (found === 1) {
        const partition = body.u16(), at = body.offset(), time = rfc3339(body.i64()), key = body.string(MAX_KEY), id = body.string(MAX_ID);
        out = { partition, offset: at, key, id, time, value: body.rest(MAX_VALUE) };
      }
      break;
    }
    case REPLY | POSITION:
      out = { next: body.offset(), state: body.rest(MAX_VALUE) };
      break;
    case REPLY | COMMIT:
    case REPLY | PING:
      out = undefined;
      break;
    case ERROR:
      out = { code: body.u8() };
      if (!CODES[out.code]) throw new Malformed("unknown error code");
      break;
    default:
      throw new Malformed("unknown reply type");
  }
  body.end();
  return out;
}
