import { Client, type StreamRecord } from "../../../clients/javascript/index.js";

// Pass environment values from the launcher to keep this example free of Node type dependencies.
declare const process: { env: Record<string, string | undefined> };
const token = process.env.DOSTOEVSKY_TOKEN;
if (!token) throw new Error("Set DOSTOEVSKY_TOKEN");
const client = new Client(process.env.DOSTOEVSKY_URL ?? "http://127.0.0.1:8787", token);
const encode = (text: string) => new TextEncoder().encode(text);
const ack = await client.publish("jobs", encode("task from typescript"), {key: "jobs+λ", id: "typescript-demo"});
const group = "typescript-worker";
const {next} = await client.position("jobs", ack.partition, group);
for (let offset = next; offset <= ack.offset; offset++) {
  const job: StreamRecord | null = await client.read("jobs", ack.partition, offset, 1000);
  if (!job) throw new Error("missing job");
  console.log(job.id, new TextDecoder().decode(job.value));
  await client.commit("jobs", ack.partition, group, offset, encode(job.id));
}
