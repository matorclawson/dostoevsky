// The workload, histogram and receipt line shared by every engine adapter of the JavaScript worker.
// It follows benchmarks/embedded/internal/run and the Python worker: 512-byte records whose first
// eight bytes are the big-endian message index, concurrent synchronous publishes acknowledged only
// after the engine's durable acknowledgment, no retries, every acknowledged record read back, and
// the stop reasons reached / window / failed / verification-failed. Offsets are BigInt (a Redis
// stream ID packed into an integer does not fit a double). Node's standard library only.
export const RECORD_BYTES = 512;
export const HIST_BINS = 2400;
const LOG = Math.log(1.01);
export const STOP = { reached: "reached", window: "window", failed: "failed", verify: "verification-failed" };

// The engine gave no answer before the deadline: the publish may or may not have been stored. The run
// stops as failed, says so, and the message is not counted.
export class AmbiguousOutcome extends Error {
  constructor(message) { super(message); this.name = "AmbiguousOutcome"; }
}

// What may go into a receipt about an engine error: the error CLASS and one word from a fixed set.
// Never the message, which can carry tokens, addresses with credentials or payload bytes.
export function reason(e) {
  const name = typeof e?.name === "string" && /^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(e.name) ? e.name : "Error";
  const code = String(e?.code ?? "");
  let word = "engine-error";
  if (e instanceof AmbiguousOutcome) word = "ambiguous-timeout";
  else if (/timeout/i.test(name) || code === "ETIMEDOUT" || code === "ABORT_ERR") word = "timeout";
  else if (/^(ECONN|EPIPE|ENOTFOUND|EAI_)/.test(code) || /connection/i.test(name)) word = "connection";
  else if (e instanceof TypeError || e instanceof RangeError || e instanceof SyntaxError) word = "protocol";
  // A kafkajs protocol error names its cause with a fixed upper-case type (UNKNOWN_TOPIC_OR_PARTITION) and a number:
  // both are safe, unlike the message, and tell a missing topic from a leader election.
  const type = typeof e?.type === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(e.type) ? `:${e.type}` : "";
  const num = Number.isSafeInteger(e?.code) && e.code >= 0 ? `,${e.code}` : "";
  return `${name}:${word}${type}${type ? num : ""}`;
}

export class Hist {
  n = new Array(HIST_BINS).fill(0);
  static index(ns) {
    const us = Number(ns) / 1e3;
    if (us < 1) return 0;
    return Math.min(Math.trunc(Math.log(us) / LOG), HIST_BINS - 1);
  }
  addNs(ns) { this.n[Hist.index(ns)]++; }
  merge(other) { other.n.forEach((c, i) => { this.n[i] += c; }); }
  total() { return this.n.reduce((a, b) => a + b, 0); }
  // The upper edge of the bin holding the q-th quantile, in whole nanoseconds (the Go harness's rule).
  quantileNs(q) {
    const total = this.total();
    if (total === 0) return 0;
    const rank = Math.max(Math.ceil(q * total), 1);
    let seen = 0;
    for (let i = 0; i < this.n.length; i++) {
      seen += this.n[i];
      if (seen >= rank) return Math.trunc(Math.pow(1.01, i + 1) * 1e3);
    }
    return 0;
  }
  // As receipts print it: microseconds with three decimals, nothing rounded away.
  quantileUsText(q) { return (this.quantileNs(q) / 1000).toFixed(3); }
}

export function payload(i) {
  const b = Buffer.alloc(RECORD_BYTES);
  b.writeBigUInt64BE(BigInt(i), 0);
  return b;
}

const nowNs = () => process.hrtime.bigint();

// Publish with `workers` concurrent producers until `target` messages are acknowledged, the window
// (seconds) ends, or a publish fails. The failing publish is not counted and nothing is retried.
// Publishes in flight when the stop is decided still complete and are counted.
export async function runWorkload(adapter, workers, target, windowSeconds, clock = nowNs) {
  const state = { next: 0, stop: null, ambiguous: false };
  const hist = new Hist();
  const records = [];
  const setStop = (kind, text = "") => { if (state.stop === null) state.stop = { kind, text }; };
  // A failure is never hidden by an earlier "reached" or "window": whichever came first, a publish
  // that failed makes the run a failed run.
  const fail = (text, ambiguous) => {
    if (state.stop === null || state.stop.kind !== STOP.failed) state.stop = { kind: STOP.failed, text };
    state.ambiguous ||= ambiguous;
  };
  const start = clock();
  const deadline = windowSeconds > 0 ? start + BigInt(Math.round(windowSeconds * 1e9)) : null;
  async function producer() {
    for (;;) {
      if (deadline !== null && clock() >= deadline) setStop(STOP.window);
      if (state.stop !== null) return;
      const i = state.next;
      if (i >= target) { setStop(STOP.reached); return; }
      state.next = i + 1;
      const begin = nowNs();
      let offset;
      try {
        offset = await adapter.publish(payload(i));
      } catch (e) {
        fail(`publish ${i}: ${reason(e)}`, e instanceof AmbiguousOutcome);
        return;
      }
      hist.addNs(nowNs() - begin);
      records.push([i, BigInt(offset)]);
    }
  }
  await Promise.all(Array.from({ length: workers }, producer));
  const elapsed = Number(clock() - start) / 1e9;
  const stop = state.stop ?? { kind: STOP.reached, text: "" };
  const detail = stop.kind === STOP.failed ? `${stop.text} (after ${records.length} acknowledged)` : stop.text;
  return { acked: records.length, elapsed, stop: stop.kind, detail, hist, records, ambiguous: state.ambiguous };
}

// Reads every acknowledged record back in offset order; returns "" or a plain-words failure.
// `onError(where, error)`, when given, receives the RAW error (for a person debugging by hand at a terminal, never a receipt).
export async function verify(adapter, records, onError = () => {}) {
  const sorted = [...records].sort((a, b) => (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
  for (const [i, offset] of sorted) {
    let got;
    try {
      got = await adapter.read(offset);
    } catch (e) {
      onError(`read record ${i} at ${offset}`, e);
      return `record ${i} at ${offset}: read ${reason(e)}`;
    }
    if (!Buffer.from(got).equals(payload(i))) return `record ${i} at ${offset}: the bytes read back differ from the bytes published`;
  }
  return "";
}

// Waits up to `budgetMs` for `key` to appear in `map` (filled by a consumer callback) and takes it out. The deadline is
// ABSOLUTE and fixed before the first look (now + budget); a budget that is not a positive finite number is refused
// loudly, so a deadline can never silently be "already past". Returns the value, or throws with how long it waited.
export async function takeWhenItArrives(map, key, budgetMs, now = Date.now, sleep = (ms) => new Promise((r) => setTimeout(r, ms))) {
  if (!Number.isFinite(budgetMs) || budgetMs <= 0) throw new RangeError(`the wait budget must be a positive number of milliseconds, not ${budgetMs}`);
  const started = now();
  const limit = started + budgetMs;
  while (now() < limit) {
    if (map.has(key)) { const v = map.get(key); map.delete(key); return v; }
    await sleep(Math.min(20, Math.max(1, limit - now())));
  }
  throw new Error(`${String(key)} did not arrive within ${now() - started} ms`);
}

export function peakRssKiB() { return process.resourceUsage().maxRSS; } // KiB on every platform libuv supports

// A value that stays ONE whitespace-free token, so the line parses as key=value pairs.
const token = (text) => String(text).replace(/\s+/g, "_");

// One line. result=FAILED whenever the run failed or its read-back did not verify; the throughput
// counts only acknowledged-and-verified messages.
export function receiptLine(engine, workers, res, verified, client, rssKiB, build = "n/a") {
  const ok = res.stop === STOP.reached || res.stop === STOP.window;
  const rate = res.elapsed > 0 ? verified / res.elapsed : 0;
  const f = [
    `engine=${engine}`, `workers=${workers}`, `result=${ok ? "ok" : "FAILED"}`, `acked=${res.acked}`, `verified=${verified}`,
    `records/s=${rate.toFixed(0)}`, `elapsed_s=${res.elapsed.toFixed(2)}`,
    `p50_us=${res.hist.quantileUsText(0.5)}`, `p95_us=${res.hist.quantileUsText(0.95)}`, `p99_us=${res.hist.quantileUsText(0.99)}`,
    `p99_9_us=${res.hist.quantileUsText(0.999)}`, `p99_99_us=${res.hist.quantileUsText(0.9999)}`, "quantiles=bin-upper-1pct",
    `client_rss_KiB=${rssKiB}`, "client_rss_source=maxRSS", "lang=javascript", `client=${token(client)}`, `client_build=${token(build)}`,
  ];
  if (res.ambiguous) f.push("ambiguous=1");
  f.push(`stop=${res.stop}`);
  if (res.detail) f.push("detail=" + res.detail.replace(/[^A-Za-z0-9_.:,()\/-]/g, "_").slice(0, 200));
  return f.join(" ");
}
