import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { Client, ClientError } from "../index.js";

const TOKEN = "a-test-token-with-entropy";
const seen = [];
const server = http.createServer(async (req, res) => {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const body = Buffer.concat(chunks);
  const u = new URL(req.url, "http://x");
  seen.push({ path: u.pathname, q: Object.fromEntries(u.searchParams), body, auth: req.headers.authorization, method: req.method });
  const op = u.pathname.split("/").pop();
  const send = (code, data = "", headers = {}) => { res.writeHead(code, headers); res.end(data); };
  if (req.headers.authorization !== "Bearer " + TOKEN) return send(401, "unauthorized\n");
  if (op === "publish") {
    if (u.searchParams.get("id") === "clash") return send(409, "conflict\n");
    return send(200, "", { "Dostoevsky-Partition": "0", "Dostoevsky-Offset": "9007199254740993" });
  }
  if (op === "read") {
    if (u.searchParams.get("offset") === "99") return send(204);
    if (u.searchParams.get("offset") === "77") return send(200, Buffer.alloc((1 << 20) + 1), { "Dostoevsky-Partition": "0", "Dostoevsky-Offset": "77", "Dostoevsky-Key": "", "Dostoevsky-ID": "", "Dostoevsky-Time": "t" });
    return send(200, Buffer.from([0, 255, 65]), { "Dostoevsky-Partition": "0", "Dostoevsky-Offset": u.searchParams.get("offset"),
      "Dostoevsky-Key": encodeURIComponent("+/λ"), "Dostoevsky-ID": "x", "Dostoevsky-Time": "2026-01-01T00:00:00Z" });
  }
  if (op === "position") return send(200, Buffer.from([1, 2]), { "Dostoevsky-Next": "3" });
  if (op === "commit") return send(204);
  send(404, "not found\n");
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const url = `http://127.0.0.1:${server.address().port}/`;
const client = new Client(url, TOKEN);
test.after(() => server.close());

test("publish sends a binary body, token and escaped params; offsets above 2^53 stay exact", async () => {
  seen.length = 0;
  const ack = await client.publish("jobs", new Uint8Array([0, 255]), { key: "+/λ", id: "once" });
  assert.equal(ack.offset, 9007199254740993n);
  const s = seen[0];
  assert.equal(s.method, "POST");
  assert.equal(s.path, "/dostoevsky/client/v1/publish");
  assert.deepEqual([s.q.topic, s.q.key, s.q.id], ["jobs", "+/λ", "once"]);
  assert.deepEqual([...s.body], [0, 255]);
  assert.equal(s.auth, "Bearer " + TOKEN);
});

test("read returns bytes and a decoded key, null at the end", async () => {
  const r = await client.read("jobs", 0, 5n, 100);
  assert.deepEqual([r.offset, r.key, r.id, [...r.value]], [5n, "+/λ", "x", [0, 255, 65]]);
  assert.equal(await client.read("jobs", 0, 99n), null);
});

test("position and commit", async () => {
  const p = await client.position("jobs", 0, "g");
  assert.deepEqual([p.next, [...p.state]], [3n, [1, 2]]);
  seen.length = 0;
  await client.commit("jobs", 0, "g", 3n, new Uint8Array([7]));
  assert.equal(seen[0].q.expected, "3");
  assert.deepEqual([...seen[0].body], [7]);
});

test("errors carry the status and the gateway's word", async () => {
  await assert.rejects(client.publish("jobs", new Uint8Array(), { id: "clash" }), (e) => e instanceof ClientError && e.status === 409 && e.code === "conflict");
  await assert.rejects(new Client(url, "wrong").position("jobs", 0, "g"), (e) => e.status === 401);
});

test("the token only travels over https or to loopback", () => {
  for (const ok of ["http://127.0.0.1:8787", "http://localhost:1", "http://[::1]:1", "https://example.com", "https://10.0.0.5:8787"]) new Client(ok, TOKEN);
  for (const bad of ["http://example.com", "http://10.0.0.5:8787", "http://0.0.0.0:1", "http://localhost.example.com", "ftp://127.0.0.1"]) {
    assert.throws(() => new Client(bad, TOKEN), /refusing|Invalid/, bad);
  }
});

test("an oversized response is refused", async () => {
  await assert.rejects(client.read("jobs", 0, 77n), (e) => e instanceof ClientError && e.code === "oversized_response");
});
