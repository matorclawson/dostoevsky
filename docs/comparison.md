# Comparison with other engines

Dostoevsky against six other ways to make a record durable before acknowledging it, measured on one host with one workload. Every number on this page is a median of five trials copied from [`RESULT.md`](receipts/production-03-comparison/RESULT.md) in the run's receipts; the per-trial lines are in [`comparison.log`](receipts/production-03-comparison/comparison.log), the host and package inventory in [`HOST.log`](receipts/production-03-comparison/HOST.log) and [`ENGINE-VERSIONS.log`](receipts/production-03-comparison/ENGINE-VERSIONS.log), and every file is covered by [`SHA256SUMS`](receipts/production-03-comparison/SHA256SUMS). The per-engine settings and where each version came from are in [`ENGINES.md`](../benchmarks/comparison/ENGINES.md).

## What was measured

Synchronous publishers (1, 8 and 32), each with one outstanding call; 16,000 records of 512 bytes per trial; five rounds with the engine order rotated; every acknowledged record read back and verified after a restart (105 of 105 trials verified, none failed, none retried). The host is production-03: two Neoverse-N1 vCPUs, 3.7 GiB of RAM, Linux 7.0.0-34 aarch64, ext4 on a QEMU virtual disk, Go 1.26.0, `GOMAXPROCS=2`, every engine native and one at a time.

Dostoevsky, embedded JetStream and SQLite run inside the benchmark process. Redis and Valkey are servers reached over a unix socket; Kafka and RabbitMQ are servers reached over loopback.

Reproduce: `make result LOG=docs/receipts/production-03-comparison/comparison.log ENGINES=dostoevsky,jetstream,sqlite,redis,valkey,kafka,rabbitmq` prints the medians below, and `make plots` redraws the charts from the same log. Running the comparison itself is `sh benchmarks/comparison/run.sh` on a Linux host with the engines installed as [`ENGINES.md`](../benchmarks/comparison/ENGINES.md) describes.

## Results

### Throughput (messages per second, median)

![Median messages per second at 1, 8 and 32 producers for seven engines. At 32 producers: Dostoevsky 18,412, Valkey 9,419, Redis 8,384, RabbitMQ 5,551, Kafka 3,168, JetStream 826, SQLite 783.](img/comparison/throughput.svg)

| Engine | 1 producer | 8 producers | 32 producers |
|---|---:|---:|---:|
| Dostoevsky `6f35c74b` | 833 | 5,315 | 18,412 |
| SQLite 3.54 (WAL, FULL, single writer) | 799 | 792 | 783 |
| Embedded JetStream 2.15.3 (SyncAlways) | 739 | 803 | 826 |
| Valkey 9.2 (AOF always) | 721 | 2,783 | 9,419 |
| Redis 8.12 (AOF always) | 693 | 2,625 | 8,384 |
| RabbitMQ 4.4 quorum queue (durability not verified) | 439 | 2,006 | 5,551 |
| Kafka 4.4 (acks=all, flush inferred) | 332 | 1,080 | 3,168 |

### p99 publish latency (milliseconds, median of per-trial p99)

![p99 publish latency in milliseconds at 1, 8 and 32 producers for seven engines.](img/comparison/p99-latency.svg)

| Engine | 1 producer | 8 producers | 32 producers |
|---|---:|---:|---:|
| Dostoevsky `6f35c74b` | 4.0 | 4.0 | 4.4 |
| SQLite 3.54 (WAL, FULL, single writer) | 3.0 | 42.7 | 189.2 |
| Embedded JetStream 2.15.3 (SyncAlways) | 4.0 | 23.1 | 70.4 |
| Valkey 9.2 (AOF always) | 3.9 | 6.7 | 6.8 |
| Redis 8.12 (AOF always) | 4.4 | 7.4 | 8.9 |
| RabbitMQ 4.4 quorum queue (durability not verified) | 5.5 | 8.2 | 11.3 |
| Kafka 4.4 (acks=all, flush inferred) | 7.9 | 16.1 | 20.4 |

### Memory (MiB, sum of sampled per-process peaks, client plus server)

![Peak memory in MiB at 1, 8 and 32 producers for seven engines.](img/comparison/memory.svg)

| Engine | 1 producer | 8 producers | 32 producers |
|---|---:|---:|---:|
| Dostoevsky `6f35c74b` | 9.7 | 9.8 | 10.2 |
| SQLite 3.54 (WAL, FULL, single writer) | 16.5 | 16.6 | 16.9 |
| Embedded JetStream 2.15.3 (SyncAlways) | 43.1 | 43.1 | 43.4 |
| Valkey 9.2 (AOF always) | 34.7 | 34.4 | 34.7 |
| Redis 8.12 (AOF always) | 31.6 | 31.1 | 31.6 |
| RabbitMQ 4.4 quorum queue (durability not verified) | 301.0 | 311.1 | 310.7 |
| Kafka 4.4 (acks=all, flush inferred) | 503.2 | 486.9 | 468.4 |

### Reopen (milliseconds, median; definitions differ, see below)

![Time to reopen at 1, 8 and 32 producers for seven engines.](img/comparison/reopen.svg)

| Engine | 1 producer | 8 producers | 32 producers |
|---|---:|---:|---:|
| Dostoevsky `6f35c74b` | 213.5 | 212.3 | 210.1 |
| SQLite 3.54 (WAL, FULL, single writer) | 0.61 | 0.68 | 0.63 |
| Embedded JetStream 2.15.3 (SyncAlways) | 18.0 | 14.5 | 18.8 |
| Valkey 9.2 (AOF always) | 85.1 | 85.4 | 84.4 |
| Redis 8.12 (AOF always) | 84.5 | 83.7 | 86.0 |
| RabbitMQ 4.4 quorum queue (durability not verified) | 4,678 | 4,677 | 4,501 |
| Kafka 4.4 (acks=all, flush inferred) | 4,685 | 4,539 | 4,393 |

## Where each engine wins

- **Throughput.** Dostoevsky is first at every producer count: 18,412 messages/s at 32 producers against 9,419 for Valkey (1.95x), 8,384 for Redis, 5,551 for RabbitMQ, 3,168 for Kafka, 826 for JetStream and 783 for SQLite. At 8 producers it is 5,315 against 2,783 for Valkey and 2,625 for Redis. At **1 producer the gap is small**: 833 against 799 for SQLite, 739 for JetStream, 721 for Valkey and 693 for Redis. Five trials cannot separate 833 from 799 with confidence, and earlier windows on the same host measured Dostoevsky at one producer between 752 and 833 ([A/B receipts](receipts/ab-pr19-1p/RESULT.md)), so do not read a lead at one producer. Dostoevsky, Redis and Valkey share a sync between parallel writers and scale with producers; JetStream (as configured) and SQLite (one writer, one transaction per publish) stay at about 800 messages/s at every count.
- **p99 latency.** **SQLite is best at 1 producer**: 3.0 ms against 3.9 for Valkey, 4.0 for Dostoevsky and 4.0 for JetStream. At 8 and 32 producers Dostoevsky is best (4.0 and 4.4 ms against 6.7 and 6.8 ms for Valkey, the next); SQLite's p99 grows to 42.7 ms and 189.2 ms and JetStream's to 23.1 ms and 70.4 ms as producers are added.
- **Memory.** Dostoevsky is lowest at every count (9.7 to 10.2 MiB, in the benchmark process), then SQLite (16.5 to 16.9 MiB), Redis and Valkey (31 to 35 MiB, client plus server), JetStream (43.1 to 43.4 MiB), RabbitMQ (about 301 to 311 MiB) and Kafka (468 to 503 MiB). Kafka's heap is fixed at 1 GiB and RabbitMQ's watermark at 1 GiB; both figures include their JVM or Erlang VM.
- **Reopen.** **SQLite (under 1 ms) and JetStream (about 15 to 19 ms) reopen faster than Dostoevsky (about 210 ms).** Dostoevsky scans the whole journal on open, so its reopen time grows with the retained journal; the 16,000-record journals here are small. Redis and Valkey (about 84 to 86 ms) restart a server and replay their append-only file, which is a different operation, and Kafka and RabbitMQ (4.4 to 4.7 s) restart a server to readiness. If an application reopens often, this column matters.
- **Redis against Valkey.** Valkey is ahead at 8 and 32 producers (2,783 against 2,625 and 9,419 against 8,384 messages/s, with a lower p99 at both) and Redis is behind by 4 percent at 1 producer (693 against 721). Both are within the same tier.
- **Kafka and RabbitMQ** have the lowest throughput at 1 producer (332 and 439 messages/s), the highest memory and the slowest restart. At 32 producers both are ahead of JetStream and SQLite (3,168 and 5,551 against 826 and 783). This run used one node of each, so the replication and failover they offer was not exercised.

## How to read the numbers

- **Memory** is client plus server processes, each a sum of per-process peaks sampled every 50 ms. The peaks can occur at different times, shared pages are counted once per process, and a spike shorter than the sampling interval in a child process can be missed. The in-process engines report the harness process alone.
- **Reopen** does not mean the same thing for every engine: Dostoevsky, JetStream and SQLite reopen in process; Redis and Valkey include server restart and append-only file replay; Kafka and RabbitMQ measure server restart to readiness. No ranking should be drawn across those definitions; the in-process three are comparable with each other.
- **Durability** is the setting each engine ran with, listed on each chart. Kafka's fsync per message is inferred from `log.flush.interval.messages=1`; Kafka's documentation does not say it gives an fsync after every message. RabbitMQ's durability is the vendor default and is **not verified**: its documentation says confirms follow replication to a quorum and never says the log is synced first. RabbitMQ ran on the Erlang/OTP 27.3.4.20 prebuilt from Hex.pm, whose checksum matched Hex.pm's build index (no independent checksum file is published). SQLite ran with one writer connection, so its numbers say little about SQLite with concurrent writers.
- **Redis and Valkey** run `appendfsync always`, which the vendors document as sharing one fsync across concurrent writes, the same effect Dostoevsky relies on.

## What this does not show

One host and one workload. Synchronous producers only, 512-byte records, one topic or queue, no network between client and server, no replication or failover, no consumers reading while publishing, and a virtual machine with a virtual disk; results on other storage and other hardware will differ. Kafka Streams was not measured (it needs a Java build toolchain that the 3.7 GiB host cannot spare), and Redpanda was not measured (it has no official arm64 package and checksum for the broker). RabbitMQ streams were excluded because their documentation says they do not fsync before a publisher confirm, so they cannot be compared at the same acknowledgment meaning. Every figure on this page can change with another version, another disk or another setting.
