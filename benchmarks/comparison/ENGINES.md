# Engines, versions, settings and what is verified

Status as of 24 November 2026. This file is the source for every chart footer. A hash in the "checksum source" column is never filled from a summarized web page: zeo-rt checks each downloaded archive against the vendor's own checksum file on the host and records the result here before a run. "Not verified" means the vendor's documentation does not state the property, and the chart bar says so.

Host: production-03, Ubuntu 26.04.1 (codename resolute) arm64, 2 Neoverse-N1 vCPUs, 3.7 GiB RAM, no swap, ext4 on a QEMU virtual disk. Every engine runs natively, one at a time, with data on the ext4 disk (never `/tmp`, which is tmpfs), and is removed at the end of the run.

## Measured engines

| Engine | Version | Where it comes from | Checksum source (zeo-rt verifies) | Architecture |
|---|---|---|---|---|
| Dostoevsky | the commit in the receipt | built from this repository, `CGO_ENABLED=0` | commit and tree in the receipt | in process |
| NATS JetStream | nats-server v2.15.3 | embedded in the benchmark binary | `benchmarks/embedded/go.sum` | in process |
| SQLite (single writer) | 3.54.1, bundled in modernc.org/sqlite v1.63.0 (pure Go) | Go module | `benchmarks/embedded/go.sum` | in process |
| Redis | 8.12.0 | source tarball `http://download.redis.io/releases/redis-8.12.0.tar.gz`, built with `make` into `/opt/bench/redis-8.12.0` (Redis ships no arm64 binary) | `https://raw.githubusercontent.com/redis/redis-hashes/master/README` (sha256) | server over a unix socket |
| Valkey | 9.2.0 | `https://download.valkey.io/releases/valkey-9.2.0-noble-arm64.tar.gz` (built for Ubuntu 24.04; built from source if it will not start on 26.04) | the `.sha256` file beside the tarball | server over a unix socket |
| Apache Kafka (KRaft, one node) | 4.4.0 | `https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz` | `https://downloads.apache.org/kafka/4.4.0/kafka_2.13-4.4.0.tgz.sha512` | server, loopback |
| RabbitMQ (quorum queue, one node) | 4.4.1 | `https://github.com/rabbitmq/rabbitmq-server/releases/download/v4.4.1/rabbitmq-server-generic-unix-4.4.1.tar.xz` | the release asset digest and the vendor's signature file | server, loopback |
| JDK for Kafka | Eclipse Temurin 21.0.13.1+2 | `https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13.1%2B2/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.13.1_2.tar.gz` into `/opt/bench/jdk-21` | `...tar.gz.sha256.txt` beside it; the research pass read `8c72816a6b308842d5ed1f13e5826b69c567da4784f7238d7e800efa71fe54b1` there, to be re-checked | n/a |
| Erlang/OTP for RabbitMQ | Ubuntu `erlang` 1:27.3.4.6+dfsg-1ubuntu0.2 (supported range 27 to 29) | apt on the host, purged at the end; arm64 availability still to be confirmed | package index | n/a |
| Faust (fold workload) | faust-streaming 0.15.4, Python 3.12.13 | PyPI into a disposable venv with `pip install --require-hashes` | the hash file zeo-rt generates | n/a |

## Not measured

- **Redpanda: dropped for now.** The official documentation describes `.deb` and `.rpm` packages from Redpanda's repository, and neither the documentation nor GitHub releases show an arm64 package, a download URL or a checksum for the broker (the GitHub release v26.2.2 carries only `rpk` archives). Without an official arm64 package and checksum there is nothing to extract into `/opt/bench`, so it is not run. It returns if zeo-rt finds an official arm64 package with a vendor checksum. Even then it could only appear as a "default flush policy" bar: the documentation describes fsync following `flush.ms` and `flush.bytes`, and no setting that forces an fsync per produce.
- **Kafka Streams: deferred.** It needs a Java build toolchain on a 3.7 GiB host. The README says it was not measured.
- **RabbitMQ streams: excluded.** RabbitMQ's streams page states that streams do not explicitly fsync before a publisher confirm, so they cannot be compared at the same acknowledgment meaning.

## Durability settings and their documentation

| Engine | Setting | Official page | Verified |
|---|---|---|---|
| Redis | `appendonly yes`, `appendfsync always` | https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/ ("fsync every time new commands are appended to the AOF"; the page adds that `always` supports group commit, so parallel writes can share one fsync) | Yes. Redis therefore also shares syncs between concurrent writers |
| Valkey | same two settings | https://valkey.io/topics/persistence/ (same wording, including a single write and fsync before the replies) | Yes for the settings; the page is not labeled with a version |
| Kafka | `log.flush.interval.messages=1`, client `acks=all`, `min.insync.replicas=1`, `default.replication.factor=1` | https://kafka.apache.org/44/configuration/broker-configs/ and https://kafka.apache.org/44/configuration/producer-configs/ | The settings are documented. The page does not say that `1` gives an fsync after every message; that is inferred from the definition, so the bar says "fsync per message inferred from log.flush.interval.messages=1" |
| Kafka | KRaft single node: `bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties`, then `kafka-server-start.sh` | https://kafka.apache.org/quickstart | Yes for the commands. The values of `process.roles`, `node.id`, `controller.quorum.bootstrap.servers`, `listeners` and `controller.listener.names` are not stated there, so the config file cites https://kafka.apache.org/44/operations/kraft/ line by line before the run |
| RabbitMQ | quorum queue, publisher confirms | https://www.rabbitmq.com/docs/quorum-queues ("Publisher confirms will only be issued once a published message has been successfully replicated to a quorum of members") | **Not verified**: the page never says the log is fsynced before the confirm and documents no switch to disable fsync. The bar says "durability per vendor default, not verified" |
| SQLite (single writer) | `journal_mode=WAL`, `synchronous=FULL`, one writer connection, one transaction per publish | https://www.sqlite.org/pragma.html#pragma_synchronous and https://www.sqlite.org/wal.html ("Writers sync the WAL on every transaction commit if PRAGMA synchronous is set to FULL") | Yes |
| Dostoevsky | fdatasync before acknowledgment (full fsync for the first write) | the crash tests in `crash_batch_test.go`, `publish_queue_test.go`, `retention_fault_test.go` and `fence_test.go` | Yes, by those tests |
| JetStream | `SyncAlways` | the embedded server's options in `benchmarks/embedded/cmd/jet` | Yes |

## Memory

`peakRSS_KiB` is the harness process alone (its `ru_maxrss`). For a server engine the receipt also carries `server_rss_KiB`, the sum over every process in the server's tree of that process's highest sampled `VmHWM` (read from `/proc` every 50 ms), and `total_rss_KiB`, the two added. It is a **sum of sampled per-process peaks**: the peaks can occur at different times, so the sum can exceed any real moment, shared pages are counted once per process, and a spike shorter than the sampling interval in a child process can be missed. If `/proc` is unreadable or the server process was never measured, the trial fails; a missing measurement is never reported as zero. In-process engines have no server, so their total is `peakRSS_KiB`.

## Trial hygiene

- Verification is sequential: Dostoevsky, SQLite and Redis read each acknowledged record back by its offset or ID after the restart; Kafka consumes the partition from the start; RabbitMQ fetches the queue one message at a time. The memory guard is checked between reads.
- Kafka's topic (one partition, replication factor one) is created, and its leader confirmed, before the timed span; auto-creation is off. RabbitMQ's EPMD is started by the harness on the trial's private port, stopped with the trial and included in the memory total; messages are published `mandatory`, and a returned message is a failure.
- `result.sh` fails closed: it refuses a log with a malformed line, a repeated trial number, a FAILED trial or a missing engine and producer group, unless `RESULT_ALLOW_INCOMPLETE=1` lists those problems first.

- Every trial removes its data directory and stops its server on every path, including failures; a failed trial is printed as FAILED and never retried.
- Redis and Valkey listen on a unix socket inside the trial's private directory (no TCP port), the harness checks that the process answering is the launched child, and go-redis command retries are disabled so an ambiguous `XADD` is a failure, not a second entry.
- The harness aborts a trial, as a failure, when the host's available memory falls below 300 MiB before or during the trial.
- SQLite is measured with one writer connection and one transaction per publish (concurrent producers queue in Go), and the chart label says "single writer". Its timed reopen ends after one index-seek probe, not a table scan.
- The comparison build runs with `-mod=readonly`. It writes to the Go module and build caches and the binaries to `.dev/embedded`.
