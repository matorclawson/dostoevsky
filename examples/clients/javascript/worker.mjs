import { Client } from "../../../clients/javascript/index.js";

const client = new Client(process.env.DOSTOEVSKY_URL ?? "http://127.0.0.1:8787", process.env.DOSTOEVSKY_TOKEN);
const encode = (text) => new TextEncoder().encode(text);
const ack = await client.publish("jobs", encode("task from javascript"), { key: "jobs+λ", id: "javascript-demo" });
const group = "javascript-worker";
const { next } = await client.position("jobs", ack.partition, group);
for (let offset = next; offset <= ack.offset; offset++) {
  const job = await client.read("jobs", ack.partition, offset, 1000);
  if (!job) throw new Error("missing job");
  console.log(job.id, new TextDecoder().decode(job.value));
  await client.commit("jobs", ack.partition, group, offset, encode(job.id));
}
