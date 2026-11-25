#!/usr/bin/env node
// One trial of the publisher workload from JavaScript against one engine; prints one receipt line.
//   BENCH_TOKEN=... node worker.mjs --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 [--window 600]
// The token comes from the environment, never from the command line. The server is started and
// stopped by the driver (benchmarks/embedded/cmd/serve). A publish failure is a result
// (stop=failed), never an exit code, and is never retried; internal errors exit 1.
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import * as core from "./core.mjs";

// A longer window overflows the nanosecond arithmetic (seconds times 1e9 reaches Infinity, which BigInt refuses).
export const MAX_WINDOW_SECONDS = 24 * 3600;

export async function main(argv, env = process.env, out = (s) => console.log(s), engines = null) {
  let args;
  try {
    args = parseArgs({ args: argv, options: {
      engine: { type: "string" }, addr: { type: "string" }, workers: { type: "string", default: "1" },
      target: { type: "string", default: "2000" }, window: { type: "string", default: "0" },
      "require-known-build": { type: "boolean", default: false },
      // Prints the raw engine error to stderr for a person at a terminal. run.sh never passes it; the receipt never has it.
      "debug-errors": { type: "boolean", default: false } } }).values;
  } catch (e) { console.error(e.message); return 2; }
  const workers = Number(args.workers), target = Number(args.target), windowS = Number(args.window);
  // Whole numbers a double holds exactly, and a finite window: "--workers 1e9999" or "--target 2.5" is a usage error.
  if (!args.engine || !args.addr || !Number.isSafeInteger(workers) || workers < 1 || !Number.isSafeInteger(target) || target < 1
      || !(windowS >= 0 && windowS <= MAX_WINDOW_SECONDS)) { // NaN and Infinity fail the comparison
    console.error("usage: --engine E --addr host:port [--workers N>=1, an integer] [--target N>=1, an integer] [--window S, 0 to 86400] [--require-known-build]");
    return 2;
  }
  engines ??= (await import("./adapters.mjs")).ENGINES;
  if (!(args.engine in engines)) { console.error(`unknown engine '${args.engine}' (have ${Object.keys(engines).sort().join(", ")})`); return 2; }
  const adapter = await engines[args.engine](args.addr, env.BENCH_TOKEN ?? "");
  if (args["require-known-build"] && adapter.clientBuild === "unknown") {
    await adapter.close();
    console.error("the client build is unknown (no git commit could be read); refusing to run a gated trial");
    return 2;
  }
  let res, verified = 0;
  try {
    res = await core.runWorkload(adapter, workers, target, windowS);
    if (res.stop !== core.STOP.failed || res.records.length > 0) { // also read back what a failed run acknowledged
      const why = await core.verify(adapter, res.records, args["debug-errors"] ? (where, e) => console.error("debug:", where, e) : undefined);
      if (why) { res.stop = core.STOP.verify; res.detail = why; } else verified = res.acked;
    }
  } finally {
    await adapter.close();
  }
  out(core.receiptLine(`javascript-${args.engine}`, workers, res, verified, adapter.clientName, core.peakRssKiB(), adapter.clientBuild ?? "n/a"));
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).then((code) => process.exit(code), (e) => { console.error(e); process.exit(1); });
}
