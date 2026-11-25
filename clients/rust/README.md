# dostoevsky-client (Rust)

Synchronous, **zero dependencies**: standard library only, so Cargo fetches nothing and builds offline. Plain `http://` only (HTTP/1.1, one connection per call), **and only to a loopback address**: the client refuses any other host before sending the whole-store token. There is no TLS yet: for a remote gateway, put a local TLS-terminating proxy in front, or wait for the opt-in `tls` feature (a later, separately reviewed PR; never on by default). Rust 1.80 or newer.

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

Errors are `Error::Api(status, word)` (`conflict`, `expired`, `limit`, ...), `Error::Transport` or `Error::InvalidResponse`. Responses are capped at 1 MiB.

Test (no server needed, local stub): `cargo test --offline --manifest-path clients/rust/Cargo.toml`
