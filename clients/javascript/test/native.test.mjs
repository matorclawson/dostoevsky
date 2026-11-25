import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { readFileSync } from "node:fs";
import * as wire from "../wire.js";
import { NativeClient, ClientError, ConnectionLost } from "../native.js";

const vectors = JSON.parse(readFileSync(new URL("../../../wire/testdata/vectors.json", import.meta.url)));
const hex = (b) => Buffer.from(b).toString("hex");
const from = (h) => new Uint8Array(Buffer.from(h ?? "", "hex"));
const str = (h) => Buffer.from(h, "hex").toString("utf8");
const TOKEN = "example-token-not-a-secret";

test("every client message in the vectors is encoded byte for byte", () => {
  let seen = 0;
  for (const v of vectors.valid.filter((v) => v.sender === "client")) {
    const f = v.fields;
    const build = {
      PUBLISH: () => wire.publish(f.topic, from(f.value_hex), str(f.key_hex), str(f.id_hex)),
      READ: () => wire.read(f.topic, f.partition, BigInt(f.offset), f.wait_ms),
      POSITION: () => wire.position(f.topic, f.partition, f.group),
      COMMIT: () => wire.commit(f.topic, f.partition, f.group, BigInt(f.expected), from(f.state_hex)),
      PING: () => wire.ping(),
    }[f.type];
    const got = v.name === "hello" ? wire.hello(f.token, f.max_version) : wire.setId(build(), f.request_id);
    assert.equal(hex(got), v.hex, v.name);
    seen++;
  }
  assert.equal(seen, 7);
});

test("every server message in the vectors is decoded", () => {
  for (const v of vectors.valid.filter((v) => v.sender === "server")) {
    const b = from(v.hex), f = v.fields;
    if (v.name.startsWith("welcome")) {
      assert.deepEqual(wire.welcome(b), { status: f.status, version: f.version, maxBody: f.max_body, window: f.window, windowBytes: f.window_bytes }, v.name);
      continue;
    }
    const h = wire.header(b);
    assert.equal(h.id, f.request_id, v.name);
    assert.equal(h.length, b.length - wire.HEADER, v.name);
    const got = wire.reply(h.type, b.subarray(wire.HEADER));
    const want = {
      PUBLISH_OK: () => ({ partition: f.partition, offset: BigInt(f.offset) }),
      READ_OK: () => f.found === 0 ? null : {
        partition: f.partition, offset: BigInt(f.offset), key: str(f.key_hex), id: str(f.id_hex),
        time: "2025-11-25T22:00:00.123456789Z", value: from(f.value_hex),
      },
      POSITION_OK: () => ({ next: BigInt(f.next), state: from(f.state_hex) }),
      COMMIT_OK: () => undefined,
      PING_OK: () => undefined,
      ERROR: () => ({ code: f.code }),
    }[f.type]();
    assert.deepEqual(got, want, v.name);
  }
});

test("every malformed server message in the vectors is rejected", () => {
  for (const v of vectors.invalid.filter((v) => v.sender === "server")) {
    const b = from(v.hex);
    assert.throws(() => wire.reply(wire.header(b).type, b.subarray(wire.HEADER)), wire.Malformed, v.name);
  }
});

test("a frame announced longer than max_body is rejected from its header", () => {
  const b = new Uint8Array(wire.HEADER);
  new DataView(b.buffer).setUint32(0, wire.MAX_BODY + 1, true);
  assert.throws(() => wire.header(b), wire.Malformed);
});

// A scripted server: it reads HELLO, answers WELCOME, and hands each request frame to onFrame.
async function fake(t, { status = 0, window = 256, windowBytes = 4 << 20 } = {}, onFrame = () => {}) {
  const sockets = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    socket.on("error", () => {});
    let buffer = Buffer.alloc(0), greeted = false;
    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (!greeted) {
        if (buffer.length < 10 || buffer.length < 10 + buffer.readUInt16LE(8)) return;
        buffer = buffer.subarray(10 + buffer.readUInt16LE(8));
        greeted = true;
        const w = Buffer.alloc(17);
        w.write("DSTV");
        w.writeUInt8(status, 4);
        if (status === 0) {
          w.writeUInt16LE(1, 5);
          w.writeUInt32LE(wire.MAX_BODY, 7);
          w.writeUInt16LE(window, 11);
          w.writeUInt32LE(windowBytes, 13);
        }
        socket.write(w);
      }
      while (buffer.length >= 10 && buffer.length >= 10 + buffer.readUInt32LE(0)) {
        const n = buffer.readUInt32LE(0);
        onFrame({ type: buffer[4], id: buffer.readUInt32LE(6), body: Buffer.from(buffer.subarray(10, 10 + n)), socket });
        buffer = buffer.subarray(10 + n);
      }
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => { sockets.forEach((s) => s.destroy()); server.close(); });
  return { host: "127.0.0.1", port: server.address().port };
}

function send(socket, type, id, body = Buffer.alloc(0)) {
  const h = Buffer.alloc(10);
  h.writeUInt32LE(body.length, 0);
  h.writeUInt8(type, 4);
  h.writeUInt32LE(id, 6);
  socket.write(Buffer.concat([h, body]));
}

function publishOk(offset) {
  const b = Buffer.alloc(10);
  b.writeBigUInt64LE(BigInt(offset), 2);
  return b;
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

test("replies out of order reach the call that asked", async (t) => {
  const held = [];
  const target = await fake(t, {}, (f) => {
    held.push(f);
    if (held.length === 3) for (const g of held.reverse()) send(g.socket, 0x81, g.id, publishOk(g.body.at(-1)));
  });
  const c = await NativeClient.connect(target, TOKEN);
  t.after(() => c.close());
  const got = await Promise.all([7, 8, 9].map((n) => c.publish("events", Uint8Array.of(n))));
  assert.deepEqual(got.map((r) => r.offset), [7n, 8n, 9n]);
});

test("the window holds requests back and they leave in call order", async (t) => {
  const seen = [];
  const target = await fake(t, { window: 2 }, (f) => seen.push(f));
  const c = await NativeClient.connect(target, TOKEN);
  t.after(() => c.close());
  const calls = [1, 2, 3, 4].map((n) => c.publish("events", Uint8Array.of(n)));
  await settle();
  assert.equal(seen.length, 2);
  for (let i = 0; i < 4; i++) {
    send(seen[i].socket, 0x81, seen[i].id, publishOk(i));
    await settle();
  }
  assert.deepEqual(seen.map((f) => f.body.at(-1)), [1, 2, 3, 4]);
  await Promise.all(calls);
});

test("a lost connection tells sent requests from unsent ones and nothing is retried", async (t) => {
  let frames = 0;
  const target = await fake(t, { window: 1 }, (f) => { frames++; f.socket.destroy(); });
  const c = await NativeClient.connect(target, TOKEN);
  const [first, second] = await Promise.allSettled([c.publish("events", Uint8Array.of(1)), c.publish("events", Uint8Array.of(2))]);
  assert.ok(first.reason instanceof ConnectionLost && first.reason.sent === true);
  assert.ok(second.reason instanceof ConnectionLost && second.reason.sent === false);
  await assert.rejects(c.ping(), (e) => e instanceof ConnectionLost && e.sent === false);
  assert.equal(frames, 1);
});

test("a request error keeps the connection; the value is copied before it waits for the window", async (t) => {
  const seen = [];
  const target = await fake(t, { window: 1 }, (f) => seen.push(f));
  const c = await NativeClient.connect(target, TOKEN);
  t.after(() => c.close());
  const value = Uint8Array.of(2);
  const first = c.publish("events", Uint8Array.of(1));
  const second = c.publish("events", value);
  value[0] = 99;
  await settle();
  send(seen[0].socket, 0xff, seen[0].id, Buffer.from([3]));
  await assert.rejects(first, (e) => e instanceof ClientError && e.code === "conflict" && e.status === 409);
  await settle();
  assert.equal(seen[1].body.at(-1), 2);
  send(seen[1].socket, 0x81, seen[1].id, publishOk(5));
  assert.equal((await second).offset, 5n);
});

test("a bad reply ends the connection", async (t) => {
  for (const [name, answer] of [
    ["unknown id", (f) => send(f.socket, 0x85, f.id + 1)],
    ["wrong type", (f) => send(f.socket, 0x84, f.id)],
    ["bytes left over", (f) => send(f.socket, 0x85, f.id, Buffer.from([0]))],
    ["connection error", (f) => send(f.socket, 0xff, 0, Buffer.from([11]))],
  ]) {
    await t.test(name, async (t) => {
      const target = await fake(t, {}, answer);
      const c = await NativeClient.connect(target, TOKEN);
      await assert.rejects(c.ping(), (e) => e instanceof ConnectionLost && e.sent === true);
      await assert.rejects(c.ping(), ConnectionLost);
    });
  }
});

test("a refused handshake is the server's code and carries no token", async (t) => {
  const target = await fake(t, { status: 2 });
  await assert.rejects(NativeClient.connect(target, TOKEN), (e) => e.code === "unauthorized" && !String(e.stack).includes(TOKEN));
});

test("values the protocol forbids are refused before anything is sent", async (t) => {
  let frames = 0;
  const target = await fake(t, {}, () => frames++);
  const c = await NativeClient.connect(target, TOKEN);
  t.after(() => c.close());
  const code = (want) => (e) => e instanceof ClientError && e.code === want;
  await assert.rejects(c.publish("events", new Uint8Array((1 << 20) + 1)), code("limit"));
  await assert.rejects(c.publish("events", new Uint8Array(), { id: "i".repeat(257) }), code("limit"));
  await assert.rejects(c.publish("events", new Uint8Array(), { key: "\ud800" }), code("invalid"));
  await assert.rejects(c.read("events", 0, -1n), code("invalid"));
  await assert.rejects(c.read("events", 0, 0n, 30001), code("invalid"));
  await assert.rejects(c.commit("events", 70000, "g", 0n), code("invalid"));
  await settle();
  assert.equal(frames, 0);
});

test("the token is never sent in plaintext off loopback", async () => {
  await assert.rejects(NativeClient.connect({ host: "192.0.2.1" }, TOKEN), /without TLS/);
  await assert.rejects(NativeClient.connect({ host: "example.com" }, TOKEN), /without TLS/);
});
