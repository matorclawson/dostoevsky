# Synchronous publishing at one, eight and 32 producers

Run from the root on Linux with `TMPDIR` pointing to the data volume and a Go toolchain compatible with benchmarks/embedded/go.mod:

```sh
GOMAXPROCS=2 make dev-bench-publish-gate
```

The runner alternates the order of two separate binaries over five rounds. Each trial starts with a fresh directory. It publishes 16,000 unique 512-byte payloads to one topic on one replica. Each producer has at most one outstanding call. Embedded JetStream 2.15.3 uses file storage, an in-process connection and SyncAlways. Dostoevsky uses its public Node.Publish and the local journal. Each trial reopens its store and verifies each acknowledged offset and its payload. The gate succeeds when median throughput is higher and median peak RSS is lower for Dostoevsky at every producer count. BENCH_COUNT overrides records per trial. The command reports p95 and p99 independently of the gate.

The measurements below come from shared Linux AMD64 with an overlay filesystem, GOMAXPROCS=2, Go 1.27.4, and CGO_ENABLED=0. RSS is the process high-water mark through ingest, including the driver. Latency runs from the synchronous publish call through its acknowledgement. The use case is continuous local durable ingest. These measurements do not establish performance after idle, across a network, with other payload sizes, asynchronous clients or replicated streams.

| Producers | Engine | Median messages/s | Median trial p99 (ms) | Median peak RSS (MiB) |
|---:|---|---:|---:|---:|
| 1 | dostoevsky | 4,005 | 0.623 | 10.06 |
| 1 | jetstream | 3,185 | 0.784 | 43.27 |
| 8 | dostoevsky | 13,252 | 1.665 | 10.84 |
| 8 | jetstream | 3,384 | 5.475 | 43.19 |
| 32 | dostoevsky | 36,422 | 3.229 | 11.11 |
| 32 | jetstream | 4,020 | 17.423 | 43.57 |

## Single-producer history

Five rotated trials per candidate, 10,000 records per trial, on the same volume. All three used the same synchronous harness.

| Candidate | Median messages/s | Median trial p99 (ms) |
|---|---:|---:|
| Before group commit | 4,150 | 0.539 |
| Previous concurrent candidate | 4,080 | 0.556 |
| This candidate | 4,107 | 0.667 |

A separate five-round screening run (5,000 records per trial) compared the data-sync prototype, the previous concurrent candidate and JetStream. The prototype improved singleton throughput from 4,092 to 4,278 messages/s, while its throughput at eight and 32 producers fell by 4.9% and 2.8% against the previous candidate. All three levels exceeded JetStream throughput. The release gate separately measures the final binary. Small differences between Dostoevsky revisions on this shared disk need replication.

[Raw trials and executable checksums](publisher-gate-results.txt)
