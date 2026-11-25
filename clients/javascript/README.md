# dostoevsky-client (JavaScript and TypeScript)

No dependencies. Node 18 or newer (uses the built-in `fetch`). Types ship in `index.d.ts`. Offsets are `bigint`.

```sh
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes go run ./cmd/dostoevsky-gateway &   # from the repository root
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes node examples/clients/javascript/worker.mjs
# TypeScript: node --experimental-strip-types examples/clients/typescript/worker.mts   (Node 22.6+; on 24+ the flag is not needed)
```

```js
import { Client } from "dostoevsky-client";
const c = new Client("http://127.0.0.1:8787", token);
const ack = await c.publish("jobs", new TextEncoder().encode("payload"), { key: "k", id: "once" });
const rec = await c.read("jobs", ack.partition, ack.offset, 1000); // null when nothing arrives in time
await c.commit("jobs", ack.partition, "workers", ack.offset);
```

Errors throw `ClientError` with `status` and `code` (`conflict`, `expired`, `limit`, ...).

## Native client (preview)

`dostoevsky-client/native` speaks the binary [wire protocol](https://github.com/rodriveracom/dostoevsky/blob/main/docs/wire-v1.md). **No released Dostoevsky server speaks it yet**, so it is tested only against the protocol's test vectors and a scripted server. No speed claim is made.

```js
import { NativeClient } from "dostoevsky-client/native";
const c = await NativeClient.connect({ host: "127.0.0.1", port: 8788 }, token);
const acks = await Promise.all(jobs.map((job) => c.publish("jobs", job))); // in flight together, appended in this order
c.close();
```

The methods are those of `Client`. Calls beyond the server's window wait their turn, and their bytes stay in memory until sent. Nothing is retried and there is no reconnect: after `ConnectionLost`, connect again; if `sent` is true the request may have taken effect.

Test (no server needed): `cd clients/javascript && npm test`
