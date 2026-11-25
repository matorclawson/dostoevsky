# Language client benchmarks

Dostoevsky through its gateway, from each language's client, against that language's standard clients for Kafka, NATS JetStream and Redis Streams, on the workload of the Go comparison (512-byte records, 1, 8 and 32 producers, acknowledged only after the engine's durable acknowledgment, no retries, every acknowledged record read back). The plan and its rulings: `PLAN-language-benchmarks-v1.md` in the evidence record. Dostoevsky crosses HTTP/1.1 on loopback; the others use their native protocols, and every chart says so. The control rows (Go embedded and Go through the gateway, labeled "gateway end to end (HTTP, process boundary)") come from `benchmarks/comparison/run.sh`.

| piece | where |
|---|---|
| the server driver, one implementation for the Go comparison and these runs | `benchmarks/embedded/cmd/serve` |
| a worker per language, same line format, same stop reasons | `python/`, `javascript/`, `java/` (Rust: not yet) |
| the shared latency histogram and its vector | `benchmarks/embedded/internal/hist`, `hist_vector.json` |
| the receipt lines every worker writes, parsed by the chart tool's tests | `receipt_examples*.txt` |
| the trial driver | `run.sh` (self-test: `sh benchmarks/clients/run_test.sh`) |

## Build, once per host (from the repository root)

```sh
(cd benchmarks/embedded && CGO_ENABLED=0 go build -mod=readonly -o ../../.dev/clients/serve ./cmd/serve)
(cd benchmarks/embedded && go build -mod=readonly -o <path>/nats-server github.com/nats-io/nats-server/v2)   # v2.15.3, the pinned module
python3 -m venv .dev/bench-venv && .dev/bench-venv/bin/pip install --require-hashes -r benchmarks/clients/python/requirements.txt
(cd benchmarks/clients/javascript && npm ci --ignore-scripts)
sh benchmarks/clients/java/build.sh
```

## Run

```sh
CLIENT_LANGS="python javascript java" CLIENT_ENGINES="dostoevsky kafka nats redis" \
BENCH_NATS_SERVER=<path>/nats-server BENCH_REDIS_SERVER=<path>/redis-server BENCH_GATEWAY_SERVER=<path>/dostoevsky-gateway \
BENCH_KAFKA_HOME=<path>/kafka_2.13-4.4.0 BENCH_JAVA_HOME=<path>/jdk BENCH_TARGET=16000 BENCH_WINDOW=0 \
sh benchmarks/clients/run.sh > receipt.log
```

A smoke pass (one 200-message trial per cell and producer count) runs first and stops the run with exit 3 if anything fails, before any timed trial. Then five rounds, the starting cell rotated each round, at 1, 8 and 32 producers. `BENCH_WINDOW` (seconds, one value for every cell on the host) and `BENCH_TARGET` give the fixed-window rule: each cell stops at the first of target reached, window ended or failure, and the line says which (`stop=`). A failed trial is printed as a `trial=N engine=... FAILED stop=... detail` line, counted, and never retried; the script exits 1 at the end.

Each receipt line is the worker's line (`result=`, `acked=`, `verified=`, `records/s`, `p50_us` to `p99_99_us`, `quantiles=bin-upper-1pct`, client memory with its source, the exact client and its build) plus `server_rss_KiB`, `server_rss_source` and `total_rss_KiB` (client plus server peak, a sum of sampled or kernel peaks that can occur at different times). `VmHWM` and `ps-sampled` memory are different measurements and are never compared.

The server's token is read from serve's READY line into a shell variable, handed to the worker through `BENCH_TOKEN`, and never printed, logged or put on a command line. Not measured here: reopen (the Go comparison covers it) and publish-to-consume latency (a later open-loop mode).

## The ENG-UK rehearsal, exactly

`sh benchmarks/clients/rehearsal.sh` from the repository root: it locates redis-server, Kafka, the JDK and nats-server (v2.15.3, checked) from the runtime inventory, builds the server driver, the gateway, the hash-pinned Python venv, the JavaScript packages (`npm ci --ignore-scripts`) and the Java worker, writes `meta.txt` (git head, tool versions, sha256 of every server binary), then runs `run.sh` with one round, a 60-second window and a large target (so every trial ends at the window), all under one `.dev/clients/rehearsal-<time>.noindex` directory with `TMPDIR` inside it. Override the locations with `ZEO_RUNTIMES`, `NATS_SERVER`, `PYTHON3`, the window with `REHEARSAL_WINDOW`, the cells with `CLIENT_LANGS` and `CLIENT_ENGINES`. The first real contact of the Kafka, NATS and Redis adapters with real servers happens here; quiet-window rules apply.
