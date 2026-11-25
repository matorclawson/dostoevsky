# Comparison with other engines (skeleton, not linked yet)

**This page is a skeleton.** The measurements are not here until the final seven-engine run's receipts arrive. It is not linked from the README yet. Every number on it will be copied from `RESULT.md` of that run, and every cell will link to the receipt.

## What was measured

One host (production-03), one workload, one window. Synchronous publishers (1, 8 and 32) each with one outstanding call; 16,000 records of 512 bytes per trial; five rounds with the engine order rotated; every acknowledged record verified after a restart. Engines: Dostoevsky, embedded NATS JetStream, SQLite, Redis, Valkey, Apache Kafka and RabbitMQ. In-process engines (Dostoevsky, JetStream, SQLite) have no server; the others are servers on the same host, reached over a unix socket (Redis, Valkey) or loopback (Kafka, RabbitMQ). The setup, versions and every checksum are in `benchmarks/comparison/ENGINES.md`.

## Results

*(Median throughput, p99, memory and reopen per engine and producer count, pasted from `RESULT.md`, with the charts.)*

## Where each engine wins

*(Written from the medians above: per producer count, which engine has the best throughput, p99 and memory, and where Dostoevsky is not first. No sentence says "better" without a number and a receipt.)*

## How to read the numbers

- **Memory** is client plus server processes: Kafka includes its JVM (1 GiB heap, fixed), RabbitMQ its Erlang VM (1 GiB watermark). Each figure is a sum of per-process peaks sampled every 50 ms, which can occur at different times and count shared pages once per process.
- **Reopen** does not mean the same thing for every engine: Redis and Valkey restart and replay their append-only file; Kafka and RabbitMQ restart the server; Dostoevsky, JetStream and SQLite reopen in process. No ranking is drawn across those definitions.
- **Durability** is the setting each engine ran with, listed under each chart. Kafka's fsync per message is inferred from its flush setting. RabbitMQ's durability is the vendor's default and is not verified. SQLite ran with one writer connection. Redis and Valkey, like Dostoevsky, share syncs between parallel writers.

## What this does not show

One host and one workload. Synchronous producers only, no network between client and server, no replication, no consumers reading while publishing, 512-byte records only. Kafka Streams and Redpanda were not measured (Kafka Streams needs a Java build toolchain on a 3.7 GiB host; Redpanda has no official arm64 package or checksum for its broker). The host is a virtual machine with a virtual disk; results on other storage will differ.
