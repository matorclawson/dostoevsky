# Embedded performance measurements

These measurements describe the previous performance candidate. The [concurrent publishing comparison](concurrent-publishing.md) covers the automatic group commit implementation.

On shared Linux AMD64, we compared synchronous publication to one file-backed stream in two separately compiled Go processes. Each process runs one engine. JetStream v2.15.3 uses the v1.56.0 Go client, in-process connections, no network listener, one replica and SyncAlways. Dostoevsky uses a direct Publish call, one partition and its normal sync-before-return path. Each producer waits for the acknowledgment before its next publish. Neither client sets an idempotency ID. The 512-byte payload contains a unique record number. Following a clean shutdown and reopen, the harness reads each acknowledged position and verifies the full payload. This workload measures publish latency. Subscriber visibility, redelivery, forced restart, replication and independent consumer progress need separate workloads.

## How to reproduce

On Linux, set TMPDIR to a directory on the intended data volume and run `make dev-bench-embedded` with Go 1.26 or newer. The default is 2,000 records per run, GOMAXPROCS=2 and three trials for each engine and producer count. BENCH_COUNT and GOMAXPROCS can be overridden. The benchmark dependencies live in a nested module. The root module still builds with the Go standard library.

We used Go 1.27.4 and an overlay filesystem. Peak RSS is the process high-water mark through ingest. Live Go heap is sampled after GC. Cold open, GC and readback are outside the timed publish loop. The readback rate measures individual direct record lookups in acknowledgment order indexed by the input ID, with byte comparison. It does not measure consumer delivery throughput.

| Producers | Engine | Records/s | Publish p99 | Peak RSS |
|---:|---|---:|---:|---:|
| 1 | Dostoevsky | 4,117 | 0.49 ms | 9.64 MiB |
| 1 | JetStream | 2,727 | 1.71 ms | 22.02 MiB |
| 8 | Dostoevsky | 3,461 | 5.32 ms | 10.14 MiB |
| 8 | JetStream | 3,519 | 5.89 ms | 22.41 MiB |

Each cell shows the median of three runs of the packaged code. At eight producers JetStream led on median throughput across all three rounds of harness development. The ordering for p99 changed between rounds. Keep individual runs when judging small differences. The results file includes the two pilot rounds and the final round labeled embedded-release.

## Memory with many topics

We compared the previous full 0.3.0 candidate to this revision using the same Go 1.24.4 burst harness, GOMAXPROCS=2, four producers, explicit batches of 128, 128-byte values and one unique ID per record. Use `make dev-burst BURST_RECORDS=1000000 BURST_TOPICS=1000000` and the same TMPDIR to reproduce the high-cardinality run.

| One million one-record topics | Previous candidate | Packaged revision |
|---|---:|---:|
| Post-GC live heap | 193.7 MiB | 163.4 MiB |
| Peak RSS through verification | 461.2 MiB | 375.6 MiB |
| Ingest records/s | 89,390 | 114,295 |
| Reopen | 6.43 s | 5.40 s |

One run per revision in this comparison. Both verified all one million IDs and payloads after reopen. The high-cardinality benefit comes from storing the first location inline and allocating the tail index and waiter state on demand. With 100,000 records on a single topic the live heap stayed at 6.1 MiB in both versions, and median peak RSS in the shorter runs increased from 23.4 to 24.2 MiB. Peak memory includes recovery and verification in the burst harness. The head-to-head publisher reports peak RSS through ingest only.

See [individual measurements](embedded-performance-results.txt).
