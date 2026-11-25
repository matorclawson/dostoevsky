# Rust worker for the language client benchmarks

One trial of the publisher workload from Rust against one engine: 512-byte records, N synchronous producer threads, acknowledged only after the engine's durable acknowledgment, no retries, every acknowledged record read back, one receipt line. Same contract, stop reasons and line format as the Python, JavaScript and Java workers (`../receipt_examples*.txt`); the server is started by `benchmarks/embedded/cmd/serve`.

Two crates in two workspaces (cargo resolves every member of a workspace before building any of them, and the worker's dependencies need a lockfile that exists only after the dependency check), so the part that needs no libraries is tested offline:

- `core/` (standard library only): the workload, the 1% histogram checked against `../hist_vector.json` (47 index cases including bin-edge neighbours), the receipt line, the command line. `cargo test --offline --manifest-path benchmarks/clients/rust/Cargo.toml` needs nothing downloaded.
- `worker/`: the binary and the adapters: our client through the gateway (the dependency-free `clients/rust`), `rdkafka` (acks=all, retries=0, no idempotence, one message per batch; librdkafka is built from the C source bundled in `rdkafka-sys`, so it needs a C toolchain such as Xcode's clang, and not the `cmake-build` feature), `async-nats` (JetStream, on a small tokio runtime, each producer thread blocking on its own publish) and `redis` (raw `XADD` and `XRANGE`, one blocking connection per producer thread).

Dependencies (exact versions, `default-features = false`): rdkafka 0.39.0, async-nats 0.52.0, redis 1.8.1, tokio 1.54.1: each the newest release at least 14 days old when chosen. **`worker/Cargo.lock` is committed** (143 packages: 140 from crates.io and our three). It was generated once on ENG-UK (`cargo generate-lockfile`, the only online step), then 20 crates that cargo had resolved to releases younger than 14 days were pinned to the newest older compatible release (`cargo update -p NAME --precise VERSION`; `url` must come before `form_urlencoded`, which the newer `url` requires), and every crate was checked: its `.crate` SHA-256 against both the lock and crates.io, license, age, build scripts, native sources (`DEPS-rust-worker-v3.md` in the evidence record; the `jetstream` feature of async-nats added nine small crates to the first checked set, all 42 days old or more). Builds run `--offline --locked`, so a changed lock is a failed build. The client names in the receipt come from these pins and from the client's own manifest, never from typed strings.

Run, from the repository root:

```sh
cargo build --release --offline --locked --manifest-path benchmarks/clients/rust/worker/Cargo.toml
BENCH_TOKEN=<token from serve's READY line> BENCH_REPO=. benchmarks/clients/rust/worker/target/release/worker \
  --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 --window 600 --require-known-build
```

The token comes from `BENCH_TOKEN`, never the command line. A publish failure is a result (`stop=failed`, `result=FAILED`), never an exit code; a timeout is `ambiguous=1`; engine error text never reaches the line (a class name and a fixed word). The histogram uses the platform's `ln` and `powf`; if they ever disagree with the vector on a bin edge, the core test says which value.
