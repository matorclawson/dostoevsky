// One adapter per engine: publish(Buffer) -> BigInt offset (returned only after the engine's durable
// acknowledgment, never retried), read(BigInt) -> bytes, close(). Third-party packages are imported
// only by the adapter that needs them, so the rest of the worker runs on Node alone.
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AmbiguousOutcome, takeWhenItArrives } from "./core.mjs";

const TOPIC = "events";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Terminal-only diagnostics, switched on by the worker's --debug-errors: raw engine detail on stderr, never in a receipt.
export const debug = { on: false, log: (...a) => { if (debug.on) console.error("debug:", ...a); } };

// '<package>-<version>' of an INSTALLED package, read from its own package.json. A package's
// "exports" map may hide ./package.json (@nats-io/jetstream does), so the package is resolved by its
// entry point and its directory found by walking up to the package.json that names it.
export function version(pkg) {
  let dir = path.dirname(fileURLToPath(import.meta.resolve(pkg)));
  for (;;) {
    const file = path.join(dir, "package.json");
    if (existsSync(file)) {
      const j = JSON.parse(readFileSync(file, "utf8"));
      if (j.name === pkg) return `${pkg}-${j.version}`;
    }
    const up = path.dirname(dir);
    if (up === dir) throw new Error(`cannot find the installed package ${pkg}`);
    dir = up;
  }
}

// Our own client: its name and version come from its package.json, its build from git.
const OWN_CLIENT_DIR = fileURLToPath(new URL("../../../clients/javascript/", import.meta.url));
export function ownClientName() {
  const j = JSON.parse(readFileSync(path.join(OWN_CLIENT_DIR, "package.json"), "utf8"));
  return `${j.name}-${j.version}`;
}
// The git commit of the checkout holding `dir`, "-dirty" when it has uncommitted changes there, or "unknown".
export function buildId(dir = OWN_CLIENT_DIR) {
  try {
    const git = (...a) => execFileSync("git", ["-C", dir, ...a], { encoding: "utf8", timeout: 10000, stdio: ["ignore", "pipe", "ignore"] }).trim(); // git's own error text is discarded
    return git("rev-parse", "--short=12", "HEAD") + (git("status", "--porcelain", "--untracked-files=no", "--", ".") ? "-dirty" : "");
  } catch { return "unknown"; }
}

// An engine's timeout means no answer in time: the message may have been stored. Say so.
const ambiguousIfTimeout = (e) => (/timeout|timed out/i.test(`${e?.name} ${e?.message}`) || e?.code === "ETIMEDOUT")
  ? Object.assign(new AmbiguousOutcome("no answer before the deadline"), { cause: e }) : e;

// What each engine's client reports as its identity, without connecting to anything.
export function identity(engine) {
  switch (engine) {
    case "dostoevsky": return ownClientName();
    case "kafka": return version("kafkajs");
    case "nats": return version("@nats-io/transport-node") + "+" + version("@nats-io/jetstream");
    case "redis": return version("ioredis");
    default: throw new Error(`unknown engine ${engine}`);
  }
}

export async function dostoevsky(addr, token) {
  const { Client } = await import("../../../clients/javascript/index.js");
  const c = new Client(`http://${addr}`, token);
  return {
    clientName: ownClientName(),
    clientBuild: buildId(),
    publish: async (data) => (await c.publish(TOPIC, data)).offset,
    read: async (offset) => {
      const r = await c.read(TOPIC, 0, offset);
      if (r === null) throw new Error(`offset ${offset} is past the end`);
      return r.value;
    },
    close: async () => {},
  };
}

// The two kafkajs clients. A consumer INHERITS the retry settings of the client it is created from, so the read-back
// consumer must come from a client of its own: the producer's client has retries 0 (a publish is never retried), and a
// consumer group on a fresh broker needs retries, because the group coordinator (the __consumer_offsets topic) is
// created lazily and the first FindCoordinator is answered "not available". The second rehearsal failed exactly there.
export function kafkaClientConfigs(addr, logLevel) {
  return {
    writer: { brokers: [addr], clientId: "bench", logLevel, retry: { retries: 0 } },
    reader: { brokers: [addr], clientId: "bench-verify", logLevel }, // kafkajs's default retries
  };
}

// kafkajs: acks=all (-1), no idempotence, no retries, one message per send.
export async function kafka(addr, token) {
  const { Kafka, logLevel } = await import("kafkajs");
  const cfg = kafkaClientConfigs(addr, logLevel.NOTHING);
  const k = new Kafka(cfg.writer);
  const readerClient = new Kafka(cfg.reader);
  const producer = k.producer({ allowAutoTopicCreation: false, idempotent: false, retry: { retries: 0 } });
  await producer.connect();
  let consumer = null;
  const loaded = new Map();
  return {
    clientName: identity("kafka"),
    publish: async (data) => {
      try {
        const [meta] = await producer.send({ topic: TOPIC, acks: -1, timeout: 60000, messages: [{ value: data }] });
        return BigInt(meta.baseOffset);
      } catch (e) { throw ambiguousIfTimeout(e); }
    },
    read: async (offset) => {
      if (consumer === null) {
        // Read-back is not a publish: the no-retry rule is about acknowledged writes. This consumer belongs to the reader
        // client (default retries), not the producer's client (retries 0), which it would otherwise inherit.
        consumer = readerClient.consumer({ groupId: "verify" });
        // Do not read until the group has actually been joined and a partition assigned; a crash is an error with its
        // cause, not a silent wait. (A "negative timeout" warning from kafkajs's own heartbeat timer is harmless noise.)
        const joined = joinWatcher(consumer, 60000);
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
        await consumer.run({ eachMessage: async ({ message }) => { loaded.set(BigInt(message.offset), message.value); } });
        await joined;
        consumer.seek({ topic: TOPIC, partition: 0, offset: "0" }); // from the beginning, whatever the group's own position
      }
      try {
        return await takeWhenItArrives(loaded, offset, 60000);
      } catch (e) {
        debug.log(`read of offset ${offset} failed: ${e.message}; ${loaded.size} messages are waiting in memory`);
        throw new Error(`offset ${offset} not found within the verification deadline`);
      }
    },
    close: async () => { if (consumer) await consumer.disconnect(); await producer.disconnect(); },
  };
}

// Resolves when the consumer has joined its group and been assigned a partition. kafkajs reports a failure as CRASH with
// `restart`: true when it will start the consumer again itself (a coordinator not yet elected on a fresh broker is
// the usual cause), so that crash only waits, within the budget, for the next join; a crash with restart false is
// terminal and rejects at once. On a timeout the error carries the last crash's type, so a receipt can tell
// "no coordinator yet" from "topic not found". Call it BEFORE consumer.connect(): the listeners must exist first.
export function joinWatcher(consumer, budgetMs) {
  const { GROUP_JOIN, CRASH, REBALANCING, FETCH_START } = consumer.events;
  let timer, last = null;
  const joined = new Promise((resolve, reject) => {
    consumer.on(GROUP_JOIN, (e) => { debug.log("group join", JSON.stringify(e.payload.memberAssignment)); resolve(); });
    consumer.on(CRASH, (e) => {
      debug.log("consumer crash", e.payload.restart ? "(restart scheduled)" : "(terminal)", e.payload.error);
      if (e.payload.restart === false) reject(e.payload.error); else last = e.payload.error;
    });
    consumer.on(REBALANCING, (e) => debug.log("rebalancing", e.payload.groupId));
    consumer.on(FETCH_START, () => debug.log("fetch start"));
    timer = setTimeout(() => {
      const err = new Error("consumer group not joined within the budget");
      if (last) { err.type = last.type ?? last.name; err.code = last.code; }
      reject(err);
    }, budgetMs);
  });
  return joined.finally(() => clearTimeout(timer));
}

// @nats-io/transport-node + @nats-io/jetstream (the v3 client).
export async function nats(addr, token) {
  const { connect } = await import("@nats-io/transport-node");
  const { jetstream, jetstreamManager } = await import("@nats-io/jetstream");
  const nc = await connect({ servers: addr, token, reconnect: false });
  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({ name: "EVENTS", subjects: [TOPIC], storage: "file", num_replicas: 1 });
  const js = jetstream(nc);
  return {
    clientName: identity("nats"),
    publish: async (data) => {
      try { return BigInt((await js.publish(TOPIC, data, { timeout: 60000 })).seq); } catch (e) { throw ambiguousIfTimeout(e); }
    },
    read: async (offset) => (await jsm.streams.getMessage("EVENTS", { seq: Number(offset) })).data,
    close: async () => { await nc.close(); },
  };
}

// A stream ID is "<milliseconds>-<sequence>"; packed into one BigInt as the Go harness does.
export function packId(id) {
  const [ms, seq] = String(id).split("-");
  if (BigInt(seq) >= 1n << 16n) throw new Error(`stream id ${id}: sequence does not fit`);
  return (BigInt(ms) << 16n) | BigInt(seq);
}
export const unpackId = (offset) => `${offset >> 16n}-${offset & 0xffffn}`;

// ioredis: XADD on a stream, appendfsync always on the server, no client retries or reconnects.
export async function redis(addr, token) {
  const { default: Redis } = await import("ioredis");
  const [host, port] = [addr.slice(0, addr.lastIndexOf(":")), Number(addr.slice(addr.lastIndexOf(":") + 1))];
  const r = new Redis({ host, port, password: token, maxRetriesPerRequest: 0, retryStrategy: () => null, enableAutoPipelining: false, commandTimeout: 60000 });
  return {
    clientName: identity("redis"),
    publish: async (data) => {
      try { return packId(await r.xadd(TOPIC, "*", "d", data)); } catch (e) { throw ambiguousIfTimeout(e); }
    },
    read: async (offset) => {
      const rows = await r.xrangeBuffer(TOPIC, unpackId(offset), unpackId(offset));
      if (rows.length !== 1) throw new Error(`stream id ${unpackId(offset)}: ${rows.length} entries`);
      return rows[0][1][1];
    },
    close: async () => { r.disconnect(); },
  };
}

export const ENGINES = { dostoevsky, kafka, nats, redis };
