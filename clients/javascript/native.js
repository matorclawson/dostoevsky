// Native client for the Dostoevsky wire protocol, version 1 (docs/wire-v1.md): one connection, many requests at once.
// PREVIEW: no released Dostoevsky server speaks this protocol yet.
//
// The methods and results are those of the HTTP Client. Nothing is retried and there is no reconnect: after the
// connection is lost every call fails, and a request that had been sent has an unknown outcome (ConnectionLost.sent).
import net from "node:net";
import tls from "node:tls";
import { ClientError } from "./index.js";
import * as wire from "./wire.js";

export { ClientError };

/** The connection ended. `sent` true: the request left this process and may or may not have taken effect. */
export class ConnectionLost extends ClientError {
  constructor(sent) {
    super(0, "connection_lost");
    this.sent = sent;
  }
}

/** The client's own queue is full: the request was refused here and never sent. */
export class Overloaded extends ClientError {
  constructor() {
    super(0, "client_overloaded");
  }
}

// The client's own ceilings. The server's window can only lower what is in flight, never raise it above these.
const MAX_IN_FLIGHT = 256;
const MAX_IN_FLIGHT_BYTES = 4 << 20;
const DEFAULT_MAX_REQUESTS = 1024; // unanswered, queued ones included
const DEFAULT_MAX_BYTES = 8 << 20; // their frames

const REQUEST_TIMEOUT_MS = 40000;
const HANDSHAKE_TIMEOUT_MS = 5000;

const failure = (code) => {
  const [name, status] = wire.CODES[code] ?? ["protocol", 400];
  return new ClientError(status, name);
};
const refused = (error) => (error instanceof wire.Refused ? new ClientError(error.reason === "limit" ? 413 : 400, error.reason) : error);

function isLoopbackAddress(address = "") {
  const ip = address.startsWith("::ffff:") ? address.slice(7) : address;
  return ip === "::1" || (net.isIPv4(ip) && ip.startsWith("127."));
}

// The bearer token grants the whole store: plaintext only to a unix socket or loopback.
function open(target, options) {
  if (typeof target.path === "string") {
    if (options.tls) throw new Error("tls does not apply to a unix socket: remove one of the two");
    return net.connect({ path: target.path });
  }
  const host = target.host ?? "127.0.0.1", port = target.port ?? 8788;
  if (options.tls) {
    const { ca, servername } = options.tls === true ? {} : options.tls;
    return tls.connect({ host, port, ca, servername: servername ?? (net.isIP(host) ? undefined : host), minVersion: "TLSv1.2", rejectUnauthorized: true });
  }
  if (host !== "localhost" && !isLoopbackAddress(host)) throw new Error("refusing to send the token without TLS to a non-loopback address; set the tls option");
  return net.connect({ host, port });
}

// Received bytes not yet consumed, joined only when a whole item is there.
class Inbox {
  #chunks = [];
  size = 0;
  push(chunk) { this.#chunks.push(chunk); this.size += chunk.length; }
  peek(n) {
    if (this.#chunks[0].length < n) this.#chunks = [Buffer.concat(this.#chunks, this.size)];
    return this.#chunks[0].subarray(0, n);
  }
  take(n) {
    if (n === 0) return new Uint8Array();
    const out = this.peek(n);
    if (this.#chunks[0].length === n) this.#chunks.shift();
    else this.#chunks[0] = this.#chunks[0].subarray(n);
    this.size -= n;
    return out;
  }
}

export class NativeClient {
  #socket;
  #inbox = new Inbox();
  #limits = null; // set by WELCOME
  #pending = new Map(); // request id -> {type, bytes, deadline, resolve, reject}
  #queue = []; // requests waiting for the window, in call order
  #unanswered = 0; // bytes of the pending frames
  #held = 0; // bytes of every unanswered frame, queued ones included
  #sent = 0; // requests written so far
  #carried = 0; // #sent when the first byte of a partly received frame arrived
  #blocked = false; // the socket asked us to wait for "drain"
  #maxRequests;
  #maxBytes;
  #nextId = 0;
  #dead = false;
  #sweep = null;
  #corked = false;
  #handshake = null;

  /**
   * target: { host, port } (default 127.0.0.1:8788) or { path } for a unix socket.
   * options: { tls: true | { ca, servername }, maxRequests, maxBytes }. maxRequests (1024) and maxBytes (8 MiB, at
   * least one maximum frame) bound what is unanswered here; a call beyond them fails with Overloaded.
   */
  static async connect(target, token, options = {}) {
    let first;
    try { first = wire.hello(token); } catch (error) { throw refused(error); }
    const { maxRequests = DEFAULT_MAX_REQUESTS, maxBytes = DEFAULT_MAX_BYTES } = options;
    if (!Number.isInteger(maxRequests) || maxRequests < 1 || !Number.isInteger(maxBytes) || maxBytes < wire.MAX_BODY + wire.HEADER) {
      throw new Error("maxRequests must be at least 1 and maxBytes at least one maximum frame");
    }
    const client = new NativeClient();
    client.#maxRequests = maxRequests;
    client.#maxBytes = maxBytes;
    const plaintextTcp = typeof target.path !== "string" && !options.tls;
    await client.#start(open(target, options), first, plaintextTcp);
    return client;
  }

  #start(socket, first, plaintextTcp) {
    this.#socket = socket;
    socket.setNoDelay?.(true);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.#fail(), HANDSHAKE_TIMEOUT_MS);
      this.#handshake = {
        resolve: () => { clearTimeout(timer); this.#handshake = null; resolve(); },
        reject: (error) => { clearTimeout(timer); this.#handshake = null; reject(error); },
      };
      socket.on("data", (chunk) => this.#receive(chunk));
      socket.on("drain", () => { this.#blocked = false; if (this.#limits) this.#send(); });
      socket.on("error", () => this.#fail()); // the error text is not passed on
      socket.on("close", () => this.#fail());
      // With TLS the token waits for the verified handshake. Without, it is the address actually connected to that must
      // be loopback, whatever the name resolved to.
      socket.once(socket instanceof tls.TLSSocket ? "secureConnect" : "connect", () => {
        if (plaintextTcp && !isLoopbackAddress(socket.remoteAddress)) return this.#fail();
        socket.write(first);
      });
    });
  }

  #receive(chunk) {
    if (this.#dead) return;
    // A reply may only answer a request that had been sent when the reply's first byte arrived. Nothing is sent while
    // a batch is being parsed, so parsing one reply cannot create the request that a buffered second one "answers".
    let arrived = this.#inbox.size > 0 ? this.#carried : this.#sent;
    this.#inbox.push(chunk);
    try {
      if (this.#limits === null) {
        if (this.#inbox.size < wire.WELCOME_BYTES) return;
        const w = wire.welcome(this.#inbox.take(wire.WELCOME_BYTES));
        if (w.status !== 0) return this.#fail(failure(w.status));
        // Nothing more is sent unless the whole WELCOME is acceptable: every legal frame must fit the byte window.
        if (w.version !== 1 || w.maxBody !== wire.MAX_BODY || w.window < 1 || w.windowBytes < wire.MAX_BODY + wire.HEADER) {
          throw new wire.Malformed("unusable WELCOME");
        }
        this.#limits = { window: Math.min(w.window, MAX_IN_FLIGHT), windowBytes: Math.min(w.windowBytes, MAX_IN_FLIGHT_BYTES) };
        this.#handshake.resolve();
      }
      while (!this.#dead && this.#inbox.size >= wire.HEADER) {
        const h = wire.header(this.#inbox.peek(wire.HEADER));
        if (this.#inbox.size < wire.HEADER + h.length) break;
        this.#inbox.take(wire.HEADER);
        this.#answer(h, this.#inbox.take(h.length), arrived);
        arrived = this.#sent;
      }
      this.#carried = arrived;
      if (!this.#dead) this.#send();
    } catch (error) {
      if (!(error instanceof wire.Malformed)) throw error;
      this.#fail(failure(11));
    }
  }

  #answer(h, body, arrived) {
    if (h.id === 0) { // a connection error: the server closes after it
      if (h.type !== wire.ERROR) throw new wire.Malformed("request id 0");
      return this.#fail(failure(wire.reply(h.type, body).code));
    }
    const request = this.#pending.get(h.id);
    if (!request || request.seq > arrived || (h.type !== wire.ERROR && h.type !== (wire.REPLY | request.type))) {
      throw new wire.Malformed("a reply that answers nothing");
    }
    const result = wire.reply(h.type, body);
    this.#pending.delete(h.id);
    this.#unanswered -= request.bytes;
    this.#held -= request.bytes;
    if (h.type === wire.ERROR) request.reject(failure(result.code));
    else request.resolve(result);
  }

  // Ends the connection once: pending requests were sent (unknown outcome), queued ones never left.
  #fail(cause) {
    if (this.#dead) return;
    this.#dead = true;
    clearInterval(this.#sweep);
    this.#sweep = null;
    this.#socket?.destroy();
    this.#handshake?.reject(cause ?? new ConnectionLost(false));
    for (const request of this.#pending.values()) request.reject(new ConnectionLost(true));
    for (const request of this.#queue) request.reject(new ConnectionLost(false));
    this.#pending.clear();
    this.#queue = [];
  }

  // Sends from the head of the queue while the window allows; nothing overtakes, so publishes keep call order.
  #send() {
    while (!this.#dead && !this.#blocked && this.#queue.length > 0) {
      const request = this.#queue[0];
      if (this.#pending.size >= this.#limits.window || this.#unanswered + request.bytes > this.#limits.windowBytes) break;
      this.#queue.shift();
      do { this.#nextId = this.#nextId === 0xffffffff ? 1 : this.#nextId + 1; } while (this.#pending.has(this.#nextId));
      request.deadline = Date.now() + REQUEST_TIMEOUT_MS;
      this.#pending.set(this.#nextId, request);
      this.#unanswered += request.bytes;
      if (!this.#corked) { // one write for everything sent in this turn
        this.#corked = true;
        this.#socket.cork();
        process.nextTick(() => { this.#corked = false; this.#socket.uncork(); });
      }
      request.seq = ++this.#sent;
      this.#blocked = !this.#socket.write(wire.setId(request.frame, this.#nextId));
      request.frame = null;
    }
    this.#watch();
  }

  // One timer for all requests, only while some are unanswered.
  #watch() {
    if (this.#pending.size === 0) {
      clearInterval(this.#sweep);
      this.#sweep = null;
    } else if (this.#sweep === null) {
      this.#sweep = setInterval(() => {
        const now = Date.now();
        for (const request of this.#pending.values()) if (now > request.deadline) return this.#fail();
      }, 1000);
    }
  }

  async #call(type, build) {
    if (this.#dead) throw new ConnectionLost(false);
    // Refused before the caller's bytes are copied, as far as the count goes; the exact size is checked after.
    if (this.#pending.size + this.#queue.length >= this.#maxRequests) throw new Overloaded();
    let frame;
    try { frame = build(); } catch (error) { throw refused(error); }
    if (this.#held + frame.length > this.#maxBytes) throw new Overloaded();
    this.#held += frame.length;
    return new Promise((resolve, reject) => {
      this.#queue.push({ type, frame, bytes: frame.length, deadline: 0, seq: 0, resolve, reject });
      this.#send();
    });
  }

  publish(topic, value, { key = "", id = "" } = {}) {
    return this.#call(wire.PUBLISH, () => wire.publish(topic, value, key, id));
  }
  read(topic, partition, offset, waitMs = 0) {
    return this.#call(wire.READ, () => wire.read(topic, partition, offset, waitMs));
  }
  position(topic, partition, group) {
    return this.#call(wire.POSITION, () => wire.position(topic, partition, group));
  }
  commit(topic, partition, group, expected, state = new Uint8Array()) {
    return this.#call(wire.COMMIT, () => wire.commit(topic, partition, group, expected, state));
  }
  ping() {
    return this.#call(wire.PING, () => wire.ping());
  }
  /** Closes now. Unanswered requests fail with ConnectionLost. */
  close() {
    this.#fail();
  }
}
