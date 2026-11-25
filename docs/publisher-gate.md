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

## production-03: ARM64 on a real, virtualized disk

zeo-rt ran the same gate on production-03 (delivery 4596, commit `8b5953ac`, which is main after the group-commit integration): Linux 7.0.0-34 aarch64, two Neoverse-N1 vCPUs, ext4 on a QEMU virtual disk, Go 1.26.0, `CGO_ENABLED=0`, `GOMAXPROCS=2`, native processes, one fresh directory per trial, 16,000 records of 512 bytes per trial, five alternating trials per point, every record verified after reopen. Absolute numbers are lower than on the AMD64 host above because each sync is a real flush to a virtual disk. The pattern is the same, with a larger gap at 32 producers.

| Producers | Engine | Median messages/s | Median trial p99 (ms) | Median peak RSS (MiB) | Median reopen (ms) |
|---:|---|---:|---:|---:|---:|
| 1 | dostoevsky | 880 | 3.19 | 9.31 | 199.86 |
| 1 | jetstream | 746 | 4.27 | 43.18 | 16.85 |
| 8 | dostoevsky | 2,947 | 6.99 | 9.91 | 202.85 |
| 8 | jetstream | 823 | 22.67 | 43.16 | 14.52 |
| 32 | dostoevsky | 7,764 | 9.69 | 10.06 | 203.78 |
| 32 | jetstream | 815 | 75.87 | 43.62 | 15.16 |

Dostoevsky is ahead on throughput, p99 and memory at every producer count. **JetStream reopens 12 to 14 times faster**: Dostoevsky replays its journal on open and the time does not depend on the number of producers. A second run with `GOMAXPROCS` unset (the wrapper then defaults to 2) gave 738 / 2,507 / 7,826 messages/s against 668 / 792 / 752 (`unset.log`).

![Median messages per second on production-03.](img/production-03/throughput.svg)

Receipts, host description and checksums: [explicit-2.log](receipts/production-03/explicit-2.log), [unset.log](receipts/production-03/unset.log), [HOST.log](receipts/production-03/HOST.log), [RESULT.md](receipts/production-03/RESULT.md), [SHA256SUMS](receipts/production-03/SHA256SUMS). The medians above are computed from `explicit-2.log`; `TestProduction03MediansMatchResult` checks the throughput and memory columns against the values recorded in `RESULT.md`. The other charts are in `docs/img/production-03/`.

## Single-producer history

Five rotated trials per candidate, 10,000 records per trial, on the same volume. All three used the same synchronous harness.

| Candidate | Median messages/s | Median trial p99 (ms) |
|---|---:|---:|
| Before group commit | 4,150 | 0.539 |
| Previous concurrent candidate | 4,080 | 0.556 |
| This candidate | 4,107 | 0.667 |

A separate five-round screening run (5,000 records per trial) compared the data-sync prototype, the previous concurrent candidate and JetStream. The prototype improved singleton throughput from 4,092 to 4,278 messages/s, while its throughput at eight and 32 producers fell by 4.9% and 2.8% against the previous candidate. All three levels exceeded JetStream throughput. The release gate separately measures the final binary. Small differences between Dostoevsky revisions on this shared disk need replication.

[Raw trials and executable checksums](publisher-gate-results.txt)
