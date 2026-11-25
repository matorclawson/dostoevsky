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

Test (no server needed): `cd clients/javascript && npm test`
