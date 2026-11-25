import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import * as core from "../core.mjs";
import { main } from "../worker.mjs";
import { packId, unpackId } from "../adapters.mjs";

const VECTOR = JSON.parse(readFileSync(new URL("../../hist_vector.json", import.meta.url)));

// An in-memory engine: publish returns the offset; read returns what was stored.
function memory({ failAt = null, delayMs = 0, corrupt = false } = {}) {
  const log = [];
  const m = {
    clientName: "memory-0", log, attempts: 0,
    async publish(data) {
      m.attempts++;
      if (failAt !== null && m.attempts === failAt) throw new Error("the engine went away");
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
      log.push(Buffer.from(data));
      return BigInt(log.length - 1);
    },
    async read(offset) {
      const b = Buffer.from(log[Number(offset)]);
      if (corrupt) b[b.length - 1] ^= 1;
      return b;
    },
    async close() {},
  };
  return m;
}

test("histogram bins and quantiles match the shared vector", () => {
  assert.equal(VECTOR.bins, core.HIST_BINS);
  for (const [ns, want] of Object.entries(VECTOR.index)) assert.equal(core.Hist.index(Number(ns)), want, ns);
  const h = new core.Hist();
  for (const ns of VECTOR.durations_ns) h.addNs(ns);
  for (const [q, want] of Object.entries(VECTOR.quantiles_us)) assert.equal(h.quantileUs(Number(q)), want, q);
});

test("an empty histogram has no quantiles; merging adds", () => {
  const a = new core.Hist(), b = new core.Hist();
  assert.equal(a.quantileUs(0.99), 0);
  a.addNs(5000); b.addNs(5000); a.merge(b);
  assert.equal(a.total(), 2);
});

test("a record is 512 bytes starting with its index", () => {
  const p = core.payload(258);
  assert.equal(p.length, 512);
  assert.deepEqual([...p.subarray(0, 8)], [0, 0, 0, 0, 0, 0, 1, 2]);
});

test("reached counts every acknowledgment exactly once at 1, 8 and 32 workers", async () => {
  for (const workers of [1, 8, 32]) {
    const m = memory();
    const res = await core.runWorkload(m, workers, 500, 0);
    assert.deepEqual([res.stop, res.acked, m.log.length], ["reached", 500, 500], String(workers));
    assert.deepEqual(res.records.map((r) => r[0]).sort((a, b) => a - b), Array.from({ length: 500 }, (_, i) => i));
    assert.equal(await core.verify(m, res.records), "");
  }
});

test("a failure is a result, never retried, and the failing publish is not counted", async () => {
  const m = memory({ failAt: 6 });
  const res = await core.runWorkload(m, 1, 100, 0);
  assert.equal(res.stop, "failed");
  assert.deepEqual([res.acked, m.attempts], [5, 6]); // five acknowledged, the sixth failed, no seventh
  assert.match(res.detail, /the engine went away/);
  assert.match(res.detail, /after 5 acknowledged/);
});

test("with many workers a failure stops the others; attempts exceed acks by one", async () => {
  const m = memory({ failAt: 40 });
  const res = await core.runWorkload(m, 8, 10000, 0);
  assert.equal(res.stop, "failed");
  assert.equal(res.acked, m.log.length);
  assert.equal(m.attempts - res.acked, 1);
});

test("the window ends the run", async () => {
  const begin = Date.now();
  const res = await core.runWorkload(memory({ delayMs: 10 }), 4, 1e9, 0.3);
  assert.equal(res.stop, "window");
  assert.ok(res.acked > 0);
  assert.ok(Date.now() - begin < 5000);
});

test("verification finds a changed byte and reports a read error in words", async () => {
  const m = memory({ corrupt: true });
  const res = await core.runWorkload(m, 2, 20, 0);
  assert.match(await core.verify(m, res.records), /differ/);
  const ok = memory();
  const r2 = await core.runWorkload(ok, 1, 5, 0);
  r2.records.push([99, 12345n]);
  assert.match(await core.verify(ok, r2.records), /record 99 at 12345/);
});

test("the receipt line has the documented keys, one line, no secret", async () => {
  const res = await core.runWorkload(memory(), 2, 100, 0);
  const line = core.receiptLine("javascript-memory", 2, res, 100, "memory-0", 12345);
  const keys = Object.fromEntries(line.split(" ").map((kv) => kv.split(/=(.*)/s).slice(0, 2)));
  for (const k of ["engine", "workers", "acked", "verified", "records/s", "elapsed_s", "p50_us", "p95_us", "p99_us", "p99_9_us", "p99_99_us", "client_rss_KiB", "client_rss_source", "lang", "client", "stop"]) assert.ok(k in keys, k);
  assert.deepEqual([keys.engine, keys.stop, keys.acked, keys.verified], ["javascript-memory", "reached", "100", "100"]);
  assert.ok(!line.includes("\n"));
  const failed = core.receiptLine("x", 1, { acked: 3, elapsed: 1, stop: "failed", detail: "publish 4: boom happened", hist: new core.Hist(), records: [] }, 3, "c", 1);
  assert.ok(failed.split(" ").every((kv) => kv.includes("=")), failed);
});

async function trial(argv, adapter) {
  const lines = [];
  const code = await main(argv, { BENCH_TOKEN: "secret-token-value" }, (s) => lines.push(s), { memory: async () => adapter });
  return [code, lines];
}

test("one trial prints one receipt line and never the token", async () => {
  const [code, lines] = await trial(["--engine", "memory", "--addr", "x:1", "--workers", "4", "--target", "200"], memory());
  assert.equal(code, 0);
  assert.equal(lines.length, 1);
  assert.match(lines[0], /verified=200/);
  assert.ok(!lines[0].includes("secret-token-value"));
});

test("a failed run exits 0 with stop=failed and verifies what it acknowledged", async () => {
  const [code, lines] = await trial(["--engine", "memory", "--addr", "x:1", "--target", "50"], memory({ failAt: 11 }));
  assert.equal(code, 0);
  assert.match(lines[0], /stop=failed/);
  assert.match(lines[0], /acked=10 verified=10/);
});

test("corrupt read-back is verification-failed; usage errors exit 2", async () => {
  const [code, lines] = await trial(["--engine", "memory", "--addr", "x:1", "--target", "20"], memory({ corrupt: true }));
  assert.equal(code, 0);
  assert.match(lines[0], /stop=verification-failed/);
  assert.match(lines[0], /verified=0/);
  assert.equal((await trial(["--engine", "nope", "--addr", "x:1"], memory()))[0], 2);
  assert.equal((await trial(["--engine", "memory", "--addr", "x:1", "--workers", "0"], memory()))[0], 2);
});

test("stream IDs pack into a BigInt and back, beyond 2^53", () => {
  const id = "1700000000123-7";
  assert.equal(unpackId(packId(id)), id);
  assert.ok(packId(id) > BigInt(Number.MAX_SAFE_INTEGER));
  assert.throws(() => packId("5-65536"), /does not fit/);
});
