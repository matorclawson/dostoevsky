# Use Dostoevsky from Python, JavaScript, TypeScript, Java and Rust

One Go process owns the durable journal. Clients in any of these languages publish to it and consume from it over a small HTTP API: binary payloads, idempotent publish, bounded long polling and durable consumer checkpoints. Each client is one thin package that depends on nothing beyond its language's standard library.

## Quickstart (five lines each)

Start the gateway first, from the repository root, in its own terminal. It listens on loopback, port 8787:

```sh
export DOSTOEVSKY_TOKEN=$(go run ./cmd/dostoevsky-gateway -gen-token)   # 32 random bytes; keep it secret
make dev-gateway
```

Then, in a second terminal with the same `DOSTOEVSKY_TOKEN` exported, pick your language. The token is read from the environment, never typed on a command line (arguments are visible to every user of the machine). Each command publishes a job, reads it back, and saves a checkpoint.

**Python** (3.10+)
```sh
make dev-client-python                      # runs examples/clients/python/worker.py
python3 -m pip install ./clients/python     # use it in your project
python3 -c "import os; from dostoevsky_client import Client; print(Client('http://127.0.0.1:8787', os.environ['DOSTOEVSKY_TOKEN']).publish('jobs', b'hi'))"
```
```python
c = Client(url, token); ack = c.publish("jobs", b"hi", id="once"); rec = c.read("jobs", ack.partition, ack.offset, wait_ms=1000)
```

**JavaScript** (Node 18+)
```sh
make dev-client-javascript                  # runs examples/clients/javascript/worker.mjs
npm install ./clients/javascript            # use it in your project
```
```js
import { Client } from "dostoevsky-client";
const c = new Client(url, token); const ack = await c.publish("jobs", new TextEncoder().encode("hi"), { id: "once" });
```

**TypeScript** (Node 22.18+, or 24; no compiler needed to run it)
```sh
make dev-client-typescript                  # runs examples/clients/typescript/worker.mts
npm install ./clients/javascript            # the same package ships its types
```
```ts
import { Client, type StreamRecord } from "dostoevsky-client";
const rec: StreamRecord | null = await new Client(url, token).read("jobs", 0, 0n, 1000);
```

**Java** (17+, a JDK only; no Maven)
```sh
make dev-client-java                        # compiles and runs examples/clients/java/Worker.java
cp clients/java/src/main/java/io/github/rodriveracom/dostoevsky/Client.java your/src/io/github/rodriveracom/dostoevsky/   # one file
```
```java
var c = new Client(url, token); var ack = c.publish("jobs", "hi".getBytes(UTF_8), "", "once");
```

**Rust** (stable Cargo; zero crates, builds offline)
```sh
make dev-client-rust                        # runs examples/clients/rust
# Cargo.toml: dostoevsky-client = { path = "/path/to/dostoevsky/clients/rust" }
```
```rust
let c = dostoevsky_client::Client::new("http://127.0.0.1:8787", &token); let ack = c.publish("jobs", b"hi", "", "once")?;
```

Each example publishes a job with a stable ID, processes records in order, and saves its own consumer group's cursor with its state. Run an example twice to see deduplication (the same offset comes back) and checkpoint recovery (it resumes where it stopped).

## Package names

| Ecosystem | Name to install or import | Status |
|---|---|---|
| PyPI | install `dostoevsky-client`, import `dostoevsky_client` (the bare `dostoevsky` on PyPI is an unrelated package) | not published |
| npm | `dostoevsky-client` | not published |
| crates.io | `dostoevsky-client` (library `dostoevsky_client`) | not published |
| Maven | `io.github.rodriveracom:dostoevsky-client`, package `io.github.rodriveracom.dostoevsky` | not published |

Until publication, install from this checkout as shown above.

## Security: read this before exposing the gateway

The token grants access to **every** topic and operation. So:

- The gateway listens on loopback by default and **refuses** any other address unless you give it a certificate and key (`-tls-cert`, `-tls-key`).
- Make the token with `-gen-token` (32 bytes from the operating system's random source). A token of 16 bytes is the enforced minimum; a guessable one is still guessable.
- Python, JavaScript and Java refuse to send the token over plain `http://` to anything but loopback. The Rust client has no TLS yet, so it talks to loopback only; for a remote gateway put a local TLS-terminating proxy in front of it, or wait for the opt-in `tls` feature, which will be its own reviewed change.
- Responses are bounded to 1 MiB in every client, and redirects are never followed.

## Embedding in a Go application

Applications with a Go HTTP server can mount the handler on their existing Node or Mesh:

```go
handler, err := clienthttp.New(store, token)
if err != nil {
    return err
}
mux.Handle(clienthttp.Prefix, handler)
```

Import `github.com/rodriveracom/dostoevsky/clienthttp`. A Mesh handles partition-owner routing for the client. The standalone gateway uses one local node, wildcard topics, one partition per topic by default, and the directory `.dev/client-data`; choose `-data`, `-listen` and `-partitions` at startup. Keep the partition count fixed for a given journal. To serve an existing application journal, mount the handler in the process that owns it.

## Wire contract, version 1

The prefix is `/dostoevsky/client/v1/`. Send `Authorization: Bearer <token>`. Encode query parameters as UTF-8 URL form values and send payloads and checkpoint state as raw bytes. Offsets are signed 64-bit integers (Python integers, JavaScript `bigint`). A cursor is partition-local.

| Request | Query | Success |
|---|---|---|
| POST publish | topic, optional key and id | 200, empty body. `Dostoevsky-Partition` and `Dostoevsky-Offset` response headers identify the durable record. |
| GET read | topic, partition, offset, optional wait_ms (0 to 30000) | 200 with the raw value, receipt headers, `Dostoevsky-Key`, `Dostoevsky-ID` and `Dostoevsky-Time` (UTC RFC 3339). Key and ID use percent escapes with a literal plus preserved. 204 means the read reached the end, including expiry of a bounded wait. An empty value is still a 200. |
| GET position | topic, partition, group | 200 with a decimal `Dostoevsky-Next` header and the raw state body. New groups start at zero. |
| POST commit | topic, partition, group, expected | 204 after the cursor moves from expected to expected + 1 and the state body is stored in the same commit. |

Publish and commit bodies are limited to 1 MiB. An optional ID deduplicates an identical publish within its partition while the record is retained. After an ambiguous publish outcome, retry with the same topic, key, ID and bytes. After an ambiguous commit outcome, reread the position before retrying. An external side effect performed before commit needs its own idempotency strategy.

API failures have a short machine-readable body: 400 invalid, 401 unauthorized, 409 conflict or wrong_owner, 410 expired, 413 limit, 503 closed, 504 timeout, 500 storage_failure, 507 journal_full (the journal is at its size cap: retention or a larger cap is needed, retrying alone will not help). Transport failures are distinct from an HTTP acknowledgement. A long poll holds one open request per waiting client. These clients cross HTTP; their speed is not the embedded Go engine's speed, and no throughput claim here applies to them.

## Verify interoperability

`make dev-client-interop` builds the gateway, starts it on an ephemeral loopback port with a disposable journal, and checks that it refuses plaintext off loopback. It then seeds a 256-byte record holding every byte value, runs each example, reads back every publication and group position, restarts the gateway on the same journal, reruns each example, and checks for duplicate offsets and for a stale checkpoint being refused with 409. For a subset: `make dev-client-interop CLIENT_LANGUAGES="python java"`. It needs only the toolchains of the languages you select.

Registry publication (PyPI, npm, crates.io, Maven Central) is a separate decision and has not been made. Until then, install from this checkout as shown above.
