# dostoevsky-client (Rust)

Synchronous, **zero dependencies**: standard library only, so Cargo fetches nothing and builds offline. Plain `http://` only (HTTP/1.1 keep-alive over a small pool of connections; a `Client` can be shared between threads; `close()` drops the pool), **and only to a loopback address**: the client refuses any other host before sending the whole-store token. There is no TLS yet: for a remote gateway, put a local TLS-terminating proxy in front, or wait for the opt-in `tls` feature (a later, separately reviewed PR; never on by default). Rust 1.80 or newer.

```sh
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes go run ./cmd/dostoevsky-gateway &   # from the repository root
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes cargo run --offline --manifest-path examples/clients/rust/Cargo.toml
```

```rust
let c = dostoevsky_client::Client::new("http://127.0.0.1:8787", &token);
let ack = c.publish("jobs", b"payload", "k", "once")?;           // same id + payload again returns the same offset
let rec = c.read("jobs", ack.partition, ack.offset, 1000)?;       // None when nothing arrives in time
c.commit("jobs", ack.partition, "workers", ack.offset, b"")?;
```

A call that fails on a reused connection before any reply is repeated once on a fresh one only when that is safe: reads, positions, and publishes that carry an `id`. An id-less publish or a commit is never repeated for you.

Errors are `Error::Api(status, word)` (`conflict`, `expired`, `limit`, ...), `Error::Transport` or `Error::InvalidResponse`. Responses are capped at 1 MiB.

## Native protocol client (preview)

`dostoevsky_client::native::NativeClient` speaks the binary wire protocol (`docs/wire-v1.md`) on one connection with many requests in flight. **No released server speaks it yet.** Plaintext only, to a loopback address or a unix socket (the standard library has no TLS). One client can be shared between threads; it runs one extra thread to read replies.

```rust
use dostoevsky_client::native::NativeClient;
let c = NativeClient::connect("127.0.0.1:8788", &token)?;          // or NativeClient::connect_unix(path, &token)
let pending = c.publish_nowait("jobs", b"payload", "k", "once")?;  // pipelined; pending.wait() gives the receipt
let ack = pending.wait()?;
let rec = c.read("jobs", ack.partition, ack.offset, std::time::Duration::from_secs(1))?;
```

Nothing is retried and there is no reconnect. `NativeError::ConnectionLost { sent: true }` means the request may have taken effect. A reply is accepted only within 40 s of its request, and an expired request ends the connection within about a second even if nobody waits for it. The 5 s connect-and-handshake limit does not cover name resolution or the unix-socket connect itself.

Test (no server needed, local stub): `cargo test --offline --manifest-path clients/rust/Cargo.toml`
