// One adapter per engine: publish(Buffer) -> BigInt offset (returned only after the engine's durable
// acknowledgment, never retried), read(BigInt) -> bytes, close(). Third-party packages are imported
// only by the adapter that needs them, so the rest of the worker runs on Node alone.
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AmbiguousOutcome } from "./core.mjs";

const TOPIC = "events";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
    return git("rev-parse", "--short=12", "HEAD") + (git("status", "--porcelain", "--", ".") ? "-dirty" : "");
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

// kafkajs: acks=all (-1), no idempotence, no retries, one message per send.
export async function kafka(addr, token) {
  const { Kafka, logLevel } = await import("kafkajs");
  const k = new Kafka({ brokers: [addr], clientId: "bench", logLevel: logLevel.NOTHING, retry: { retries: 0 } });
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
        consumer = k.consumer({ groupId: "verify", retry: { retries: 0 } });
        await consumer.connect();
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
        await consumer.run({ eachMessage: async ({ message }) => { loaded.set(BigInt(message.offset), message.value); } });
      }
      for (const deadline = Date.now() + 60000; Date.now() < deadline; await sleep(20)) {
        if (loaded.has(offset)) { const v = loaded.get(offset); loaded.delete(offset); return v; }
      }
      throw new Error(`offset ${offset} not found within the verification deadline`);
    },
    close: async () => { if (consumer) await consumer.disconnect(); await producer.disconnect(); },
  };
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
