import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import * as core from "../core.mjs";
import { main } from "../worker.mjs";
import { buildId, identity, kafkaClientConfigs, ownClientName, packId, unpackId, version } from "../adapters.mjs";

const here = (rel) => new URL(rel, import.meta.url);
const EXAMPLES = readFileSync(here("../../receipt_examples.txt"), "utf8").split("\n").filter((l) => l.trim() && !l.startsWith("#"));
const PINS = JSON.parse(readFileSync(here("../package.json"), "utf8")).dependencies;
const OWN = JSON.parse(readFileSync(here("../../../../clients/javascript/package.json"), "utf8"));

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
  for (const [q, want] of Object.entries(VECTOR.quantiles_ns)) assert.equal(h.quantileNs(Number(q)), want, q);
});

test("an empty histogram has no quantiles; merging adds", () => {
  const a = new core.Hist(), b = new core.Hist();
  assert.equal(a.quantileNs(0.99), 0);
  a.addNs(5000); b.addNs(5000); a.merge(b);
  assert.equal(a.total(), 2);
});

test("a short sample keeps its sub-microsecond precision", () => {
  const h = new core.Hist();
  h.addNs(1900);
  const got = h.quantileNs(0.5);
  assert.ok(got >= 1900 && got <= 1900 * 1.0101, String(got)); // not rounded down to 1 us
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
  assert.match(res.detail, /Error:engine-error/);
  assert.doesNotMatch(res.detail, /the engine went away/); // the message text never reaches a receipt
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
  for (const k of ["engine", "workers", "acked", "verified", "records/s", "elapsed_s", "p50_us", "p95_us", "p99_us", "p99_9_us", "p99_99_us", "quantiles", "client_rss_KiB", "client_rss_source", "lang", "client", "client_build", "result", "stop"]) assert.ok(k in keys, k);
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

function fixedResult(stop, { detail = "", acked = 1000, ambiguous = false } = {}) {
  const hist = new core.Hist();
  for (let i = 0; i < acked; i++) hist.addNs(2_000_000);
  return { acked, elapsed: 2, stop, detail, hist, records: [], ambiguous };
}

test("the JavaScript lines equal the shared examples, byte for byte", () => {
  const want = Object.fromEntries(EXAMPLES.map((l) => [l.split(" ")[0], l]));
  assert.equal(core.receiptLine("javascript-nats", 8, fixedResult("reached"), 1000, "@nats-io/transport-node-3.4.0+@nats-io/jetstream-3.4.0", 51200, "n/a"), want["engine=javascript-nats"]);
  assert.equal(core.receiptLine("javascript-dostoevsky", 8, fixedResult("reached"), 1000, "dostoevsky-client-0.1.0", 51200, "0123456789ab"), want["engine=javascript-dostoevsky"]);
  const failed = fixedResult("failed", { acked: 4, detail: "publish 4: Error:engine-error (after 4 acknowledged)" });
  assert.equal(core.receiptLine("javascript-redis", 8, failed, 4, "ioredis-6.0.0", 51200, "n/a"), want["engine=javascript-redis"]);
});

test("every token is a key=value pair even for a client name with spaces", () => {
  const line = core.receiptLine("javascript-x", 8, fixedResult("reached"), 1000, "name with spaces (pool)", 1, "build id");
  assert.ok(line.split(" ").every((t) => t.includes("=")), line);
});

test("a failure that lands after the run was already reached still makes it failed", async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const m = memory();
  const inner = m.publish;
  m.publish = async (data) => {
    if (data.readBigUInt64BE(0) === 1n) { await gate; throw new Error("reset by peer"); }
    return inner(data);
  };
  const run = core.runWorkload(m, 2, 2, 0);
  setTimeout(release, 300);
  const res = await run;
  assert.equal(res.stop, "failed");
  assert.equal(res.acked, 1);
  assert.equal(core.receiptLine("x", 2, res, 1, "c", 1).split(" ")[2], "result=FAILED");
});

test("result is FAILED for a failure and a verification failure, ok otherwise; throughput counts verified messages only", async () => {
  const run = async (m) => { const [, lines] = await trial(["--engine", "memory", "--addr", "x:1", "--target", "20"], m); return Object.fromEntries(lines[0].split(" ").map((kv) => kv.split(/=(.*)/s).slice(0, 2))); };
  const ok = await run(memory());
  assert.deepEqual([ok.result, ok.stop, ok.verified], ["ok", "reached", "20"]);
  const failed = await run(memory({ failAt: 5 }));
  assert.deepEqual([failed.result, failed.stop], ["FAILED", "failed"]);
  const bad = await run(memory({ corrupt: true }));
  assert.deepEqual([bad.result, bad.stop, bad.verified, bad["records/s"]], ["FAILED", "verification-failed", "0", "0"]);
});

test("error text never reaches the receipt, only the class and a fixed word", async () => {
  const leaky = memory();
  leaky.publish = async () => { throw new Error("AUTH failed for redis://user:hunter2@10.0.0.9:6379 payload=abababab"); };
  const res = await core.runWorkload(leaky, 1, 5, 0);
  const line = core.receiptLine("x", 1, res, 0, "c", 1);
  for (const leak of ["hunter2", "10.0.0.9", "redis://", "payload", "AUTH"]) assert.ok(!line.includes(leak), leak);
  assert.match(line, /Error:engine-error/);
  const m = memory();
  const r = await core.runWorkload(m, 1, 3, 0);
  m.read = async () => { throw new Error("secret-in-read hunter2"); };
  const why = await core.verify(m, r.records);
  assert.ok(!why.includes("hunter2"));
  assert.match(why, /Error:engine-error/);
  assert.equal(core.reason(new core.AmbiguousOutcome("x")), "AmbiguousOutcome:ambiguous-timeout");
  assert.equal(core.reason(Object.assign(new Error("x"), { code: "ECONNRESET" })), "Error:connection");
});

test("the debug callback sees the raw read error while the receipt text stays sanitized", async () => {
  const m = memory();
  const r = await core.runWorkload(m, 1, 3, 0);
  m.read = async () => { throw new Error("coordinator is not available hunter2"); };
  const seen = [];
  const why = await core.verify(m, r.records, (where, e) => seen.push([where, e.message]));
  assert.ok(!why.includes("hunter2") && !why.includes("coordinator"), why);
  assert.deepEqual(seen, [["read record 0 at 0", "coordinator is not available hunter2"]]);
});

test("an ambiguous timeout is reported as such", async () => {
  const silent = memory();
  silent.publish = async () => { throw new core.AmbiguousOutcome("no answer"); };
  const res = await core.runWorkload(silent, 1, 5, 0);
  assert.equal(res.ambiguous, true);
  const line = core.receiptLine("x", 1, res, 0, "c", 1);
  assert.match(line, / ambiguous=1 /);
  assert.match(line, /AmbiguousOutcome:ambiguous-timeout/);
});

// These need the dependencies installed (npm ci --ignore-scripts): they fail, loudly, when they are not.
test("every real adapter reports its installed client identity, whitespace-free, matching the pins", () => {
  const need = (name) => { try { return version(name); } catch (e) { assert.fail(`${name} is not installed: run npm ci --ignore-scripts first (${e.message})`); } };
  assert.equal(need("kafkajs"), `kafkajs-${PINS.kafkajs}`);
  assert.equal(need("ioredis"), `ioredis-${PINS.ioredis}`);
  assert.equal(need("@nats-io/jetstream"), `@nats-io/jetstream-${PINS["@nats-io/jetstream"]}`); // its exports map hides ./package.json
  assert.equal(need("@nats-io/transport-node"), `@nats-io/transport-node-${PINS["@nats-io/transport-node"]}`);
  for (const e of ["dostoevsky", "kafka", "nats", "redis"]) assert.doesNotMatch(identity(e), /\s/, e);
  assert.equal(identity("dostoevsky"), `${OWN.name}-${OWN.version}`);
  assert.equal(ownClientName(), `${OWN.name}-${OWN.version}`);
});

test("the build id is a commit (or unknown outside a checkout)", () => {
  assert.match(buildId(), /^([0-9a-f]{12}(-dirty)?|unknown)$/);
  assert.equal(buildId("/"), "unknown");
});

test("workers and target must be whole numbers and the window finite", async () => {
  for (const bad of [["--workers", "1.5"], ["--workers", "1e9999"], ["--workers", "abc"], ["--target", "2.5"], ["--target", "0"], ["--window", "Infinity"], ["--window", "-1"], ["--window", "86401"], ["--window", "1e300"], ["--window", "NaN"]]) {
    assert.equal((await trial(["--engine", "memory", "--addr", "x:1", ...bad], memory()))[0], 2, bad.join(" "));
  }
});

test("a gated run refuses an unknown client build", async () => {
  const unknown = memory();
  unknown.clientBuild = "unknown";
  assert.equal((await trial(["--engine", "memory", "--addr", "x:1", "--target", "5", "--require-known-build"], unknown))[0], 2);
  const [code, lines] = await trial(["--engine", "memory", "--addr", "x:1", "--target", "5"], unknown);
  assert.equal(code, 0);
  assert.match(lines[0], /client_build=unknown/);
});

test("a window of exactly 24 h is accepted, and the run still ends at its target", async () => {
  const [code, lines] = await trial(["--engine", "memory", "--addr", "x:1", "--target", "3", "--window", "86400"], memory());
  assert.equal(code, 0);
  assert.match(lines[0], /stop=reached/);
});

test("a clean checkout's build id is a bare commit, a changed file adds -dirty, a non-repository is unknown", () => {
  const repo = mkdtempSync(path.join(tmpdir(), "buildid-"));
  const git = (...a) => execFileSync("git", ["-C", repo, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", ...a], { stdio: "ignore" });
  git("init", "-q");
  mkdirSync(path.join(repo, "clients/javascript"), { recursive: true });
  writeFileSync(path.join(repo, "clients/javascript/index.js"), "export {};\n");
  git("add", ".");
  git("commit", "-q", "-m", "one");
  const dir = path.join(repo, "clients/javascript");
  const clean = buildId(dir);
  assert.match(clean, /^[0-9a-f]{12}$/, clean);
  writeFileSync(path.join(dir, "untracked_build_output.js"), "junk\n"); // untracked build output must not count
  assert.equal(buildId(dir), clean);
  writeFileSync(path.join(dir, "index.js"), "export const x = 1;\n");
  assert.equal(buildId(dir), clean + "-dirty");
  assert.equal(buildId(mkdtempSync(path.join(tmpdir(), "notarepo-"))), "unknown");
});

test("a read with a fresh deadline WAITS for the message instead of giving up at once", async () => {
  const map = new Map();
  setTimeout(() => map.set(5n, "late"), 150);
  const begin = Date.now();
  assert.equal(await core.takeWhenItArrives(map, 5n, 5000), "late");
  const waited = Date.now() - begin;
  assert.ok(waited >= 100 && waited < 3000, `waited ${waited} ms`);
  assert.equal(map.has(5n), false, "the value is taken out");
});

test("a message that never arrives is waited for the whole budget, not given up on at once", async () => {
  const begin = Date.now();
  await assert.rejects(core.takeWhenItArrives(new Map(), 1n, 300), /did not arrive within \d+ ms/);
  assert.ok(Date.now() - begin >= 250, `gave up after ${Date.now() - begin} ms`);
});

test("a budget that is not a positive finite number is refused loudly", async () => {
  for (const bad of [0, -1, NaN, Infinity, undefined, -Date.now()]) {
    await assert.rejects(core.takeWhenItArrives(new Map([[1n, "x"]]), 1n, bad), RangeError, String(bad));
  }
});

test("the deadline is absolute: a clock that starts at zero cannot make it negative", async () => {
  let t = 0; // a fake clock that has just started
  const now = () => t;
  const map = new Map();
  const sleep = async (ms) => { t += ms; if (t >= 60) map.set(7n, "ok"); };
  assert.equal(await core.takeWhenItArrives(map, 7n, 1000, now, sleep), "ok");
});

test("the verification consumer's client keeps kafkajs's default retries while the producer's client never retries", () => {
  const { writer, reader } = kafkaClientConfigs("127.0.0.1:9", 0);
  assert.deepEqual(writer.retry, { retries: 0 }, "a publish is never retried");
  assert.equal("retry" in reader, false, "the reader client must not carry the producer's retry setting (a consumer inherits it)");
  assert.notEqual(reader.clientId, writer.clientId);
  assert.deepEqual([writer.brokers, reader.brokers], [["127.0.0.1:9"], ["127.0.0.1:9"]]);
});
