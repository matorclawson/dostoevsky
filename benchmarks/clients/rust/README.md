# Rust worker for the language client benchmarks

One trial of the publisher workload from Rust against one engine: 512-byte records, N synchronous producer threads, acknowledged only after the engine's durable acknowledgment, no retries, every acknowledged record read back, one receipt line. Same contract, stop reasons and line format as the Python, JavaScript and Java workers (`../receipt_examples*.txt`); the server is started by `benchmarks/embedded/cmd/serve`.

Two crates in one workspace, so the part that needs no libraries is tested offline:

- `core/` (standard library only): the workload, the 1% histogram checked against `../hist_vector.json` (47 index cases including bin-edge neighbours), the receipt line, the command line. `cargo test --offline --manifest-path benchmarks/clients/rust/Cargo.toml -p dostoevsky-bench-core` needs nothing downloaded.
- `worker/`: the binary and the adapters: our client through the gateway (the dependency-free `clients/rust`), `rdkafka` (acks=all, retries=0, no idempotence, one message per batch; librdkafka is built from the C source bundled in `rdkafka-sys`, so it needs a C toolchain such as Xcode's clang, and not the `cmake-build` feature), `async-nats` (JetStream, on a small tokio runtime, each producer thread blocking on its own publish) and `redis` (raw `XADD` and `XRANGE`, one blocking connection per producer thread).

Dependencies (exact versions, `default-features = false`): rdkafka 0.39.0, async-nats 0.52.0, redis 1.8.1, tokio 1.54.1: each the newest release at least 14 days old when chosen. **`Cargo.lock` is not committed yet**: it is generated once (`cargo generate-lockfile`, the only online step), every crate in it is checked (checksum against crates.io, license, age, build scripts), and only then committed; builds after that run `--offline --locked`. The client names in the receipt come from these pins and from the client's own manifest, never from typed strings.

Run, from the repository root:

```sh
cargo build --release --offline --locked --manifest-path benchmarks/clients/rust/Cargo.toml -p dostoevsky-bench-worker
BENCH_TOKEN=<token from serve's READY line> BENCH_REPO=. benchmarks/clients/rust/target/release/worker \
  --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 --window 600 --require-known-build
```

The token comes from `BENCH_TOKEN`, never the command line. A publish failure is a result (`stop=failed`, `result=FAILED`), never an exit code; a timeout is `ambiguous=1`; engine error text never reaches the line (a class name and a fixed word). The histogram uses the platform's `ln` and `powf`; if they ever disagree with the vector on a bin edge, the core test says which value.
