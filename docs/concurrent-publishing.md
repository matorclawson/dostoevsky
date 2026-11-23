# Concurrent durable publishing

At 32 synchronous producers, Dostoevsky sustained 44,096 records/s with 11.3 MiB peak resident memory. Embedded JetStream reached 4,013 records/s with 44.2 MiB on the same host. At eight producers, the throughput advantage was 3.55×. Dostoevsky won all six paired concurrent comparisons in the longer runs.

| Producers | Engine | Records/s | Publish p99 (ms) | Peak RSS (MiB) |
|---:|---|---:|---:|---:|
| 8 | Dostoevsky | 12,972 | 2.472 | 11.1 |
| 8 | JetStream | 3,652 | 6.302 | 43.7 |
| 32 | Dostoevsky | 44,096 | 2.181 | 11.3 |
| 32 | JetStream | 4,013 | 20.027 | 44.2 |

Each column is the median of three 32,000-record trials. At 32 producers Dostoevsky ranged from 33,484 to 45,551 records/s. These results measure one local disk, shared Linux AMD64, overlay filesystem, GOMAXPROCS=2, 512-byte payloads, one topic, one replica, no producer IDs. Each producer waits for its previous acknowledgement before submitting its next record. The embedded JetStream 2.15.3 server uses SyncAlways. Its client uses the in-process connection. The Dostoevsky client calls Node.Publish. Benchmark binaries use Go 1.27.4 and CGO_ENABLED=0. The application runtime has no NATS dependency.

The timer covers publish through acknowledgement. The memory figure is the process high-water mark sampled before reopening. After each trial the harness reopens the store and verifies every payload. Networked replication, async JetStream publishing, explicit client batches, multi-topic throughput and consumer visibility are separate workloads. A separate Dostoevsky process-kill regression test checks acknowledged IDs, offsets and payloads across restart.

## Comparison with the previous candidate

Medians of five rotated 4,000-record trials per engine and concurrency level:

| Producers | Previous Dostoevsky records/s | This candidate records/s | JetStream records/s |
|---:|---:|---:|---:|
| 1 | 3,635 | 3,448 | 2,735 |
| 8 | 3,529 | 12,406 | 3,448 |
| 32 | 3,251 | 39,454 | 3,266 |

With one producer, three additional 16,000-record trials yielded 4,026 records/s and 0.507 ms p99 for the previous candidate versus 3,808 records/s and 0.630 ms p99 for this one. These runs show a singleton throughput regression of about 5%. They do not isolate its cause. Idle writes have no batching timer.

## Reproduction

Run `make dev-bench-publish` on Linux with a toolchain satisfying the nested benchmark module. Set TMPDIR to the filesystem under test. To include the previous Dostoevsky build, set BENCH_BASELINE to the absolute path of its corresponding embedded benchmark binary. The runner rotates engine order and defaults GOMAXPROCS to 2. Raw trials and binary checksums are in [concurrent-publishing-results.txt](concurrent-publishing-results.txt). In these receipts, fast denotes the shipped candidate and baseline denotes the previous performance candidate.
