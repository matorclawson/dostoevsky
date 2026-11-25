// One adapter per engine: publish(Buffer) -> BigInt offset (returned only after the engine's durable
// acknowledgment, never retried), read(BigInt) -> bytes, close(). Third-party packages are imported
// only by the adapter that needs them, so the rest of the worker runs on Node alone.
import { createRequire } from "node:module";

const TOPIC = "events";
const require = createRequire(import.meta.url);
const version = (pkg) => `${pkg}-${require(`${pkg}/package.json`).version}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function dostoevsky(addr, token) {
  const { Client } = await import("../../../clients/javascript/index.js");
  const c = new Client(`http://${addr}`, token);
  return {
    clientName: "dostoevsky-client-0.1.0(fetch,keep-alive)",
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
    clientName: version("kafkajs"),
    publish: async (data) => {
      const [meta] = await producer.send({ topic: TOPIC, acks: -1, timeout: 60000, messages: [{ value: data }] });
      return BigInt(meta.baseOffset);
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
    clientName: version("@nats-io/transport-node") + "+" + version("@nats-io/jetstream"),
    publish: async (data) => BigInt((await js.publish(TOPIC, data, { timeout: 60000 })).seq),
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
    clientName: version("ioredis"),
    publish: async (data) => packId(await r.xadd(TOPIC, "*", "d", data)),
    read: async (offset) => {
      const rows = await r.xrangeBuffer(TOPIC, unpackId(offset), unpackId(offset));
      if (rows.length !== 1) throw new Error(`stream id ${unpackId(offset)}: ${rows.length} entries`);
      return rows[0][1][1];
    },
    close: async () => { r.disconnect(); },
  };
}

export const ENGINES = { dostoevsky, kafka, nats, redis };
