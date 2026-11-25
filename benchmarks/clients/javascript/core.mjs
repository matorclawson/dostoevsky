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
  // The upper edge of the bin holding the q-th quantile, in whole microseconds (the Go harness's rule).
  quantileUs(q) {
    const total = this.total();
    if (total === 0) return 0;
    const rank = Math.max(Math.ceil(q * total), 1);
    let seen = 0;
    for (let i = 0; i < this.n.length; i++) {
      seen += this.n[i];
      if (seen >= rank) return Math.trunc(Math.trunc(Math.pow(1.01, i + 1) * 1e3) / 1000);
    }
    return 0;
  }
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
  const state = { next: 0, stop: null };
  const hist = new Hist();
  const records = [];
  const setStop = (kind, text = "") => { if (state.stop === null) state.stop = { kind, text }; };
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
        setStop(STOP.failed, `publish ${i}: ${e?.name ?? "Error"}: ${e?.message ?? e}`);
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
  return { acked: records.length, elapsed, stop: stop.kind, detail, hist, records };
}

// Reads every acknowledged record back in offset order; returns "" or a plain-words failure.
export async function verify(adapter, records) {
  const sorted = [...records].sort((a, b) => (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
  for (const [i, offset] of sorted) {
    let got;
    try {
      got = await adapter.read(offset);
    } catch (e) {
      return `record ${i} at ${offset}: ${e?.name ?? "Error"}: ${e?.message ?? e}`;
    }
    if (!Buffer.from(got).equals(payload(i))) return `record ${i} at ${offset}: the bytes read back differ from the bytes published`;
  }
  return "";
}

export function peakRssKiB() { return process.resourceUsage().maxRSS; } // KiB on every platform libuv supports

export function receiptLine(engine, workers, res, verified, client, rssKiB) {
  const rate = res.elapsed > 0 ? res.acked / res.elapsed : 0;
  const f = [
    `engine=${engine}`, `workers=${workers}`, `acked=${res.acked}`, `verified=${verified}`,
    `records/s=${rate.toFixed(0)}`, `elapsed_s=${res.elapsed.toFixed(2)}`,
    `p50_us=${res.hist.quantileUs(0.5)}`, `p95_us=${res.hist.quantileUs(0.95)}`, `p99_us=${res.hist.quantileUs(0.99)}`,
    `p99_9_us=${res.hist.quantileUs(0.999)}`, `p99_99_us=${res.hist.quantileUs(0.9999)}`,
    `client_rss_KiB=${rssKiB}`, "client_rss_source=maxRSS", "lang=javascript", `client=${client}`, `stop=${res.stop}`,
  ];
  if (res.detail) f.push("detail=" + res.detail.replaceAll("\n", " ").replaceAll(" ", "_").slice(0, 200));
  return f.join(" ");
}
