# Dostoevsky changelog

Dostoevsky is an MIT-licensed, pure-Go library for durable messaging and stream processing. Applications can start with a local directory and distribute partitions across application instances when needed. The runtime uses the Go standard library and builds with `CGO_ENABLED=0`.

**Repository status.** The complete 0.2.3 source was merged into this repository on **22 November 2026 at 00:28:38 UTC** (PR #1, before the move to `rodriveracom/dostoevsky`). The versions below follow the six source packages delivered during development.

## 0.1.0 — Embedded topics, durable storage, and stream processing

- **Added an embedded Go module.** Reviewed the supplied Faust source and its word-count example, then implemented keyed streams and recoverable processor state in Go. A single-process application opens a data directory. Kafka, Flink, RocksDB, and a separately deployed broker are unnecessary.
- **Added topics, partitions, and replay.** Producers publish keyed records to named topics. FNV-1a maps a key to a partition. Each partition assigns contiguous, zero-based offsets. Consumers can read an exact offset, replay from the beginning, or wait for new data with context cancellation.
- **Made producer retries idempotent.** An optional message ID identifies a publication within its partition. Retrying the same key, ID, and payload returns the original offset and timestamp. Reusing the ID with conflicting content returns `ErrConflict`.
- **Built the durable journal.** Each node owns a checksummed, append-only file and acknowledges writes after file sync. An exclusive lock prevents two local writers from opening the same directory. Startup reconstructs indexes, truncates an incomplete final frame, and reports checksum corruption.
- **Added independent consumer groups.** Each group tracks its next offset and processor state per partition. `Commit` compares the expected cursor and writes the new state and cursor together. Competing commits against the same cursor produce one winner.
- **Added `Fold` and `Pipe`.** `Fold` runs a typed Go reducer and restores its state after restart. `Pipe` transforms one input into one output, publishes it, and checkpoints the input. A deterministic output ID makes retries safe across the failure window between publication and checkpointing.
- **Added distributed routing.** `Mesh` routes publish, read, position, and commit operations to a partition’s owner over an HTTP handler embedded in the application. Peer requests carry a bearer token, intended owner, and topology fingerprint. The configuration permits adding topics on reopen and protects existing partition assignments.
- **Shipped two runnable examples.** `examples/wordcount` preserves counts across process restarts. `examples/mesh` runs two application processes that publish to each other’s partitions. Both were exercised locally, including delivery in both directions.

## 0.1.1 — Development, release, and deployment commands

- **Added a top-level Makefile.** `make help` lists the commands. `make dev` checks formatting, runs `go vet`, tests with cgo disabled, runs the race detector, and builds the examples. Demo binaries and journals live under `.dev/`.
- **Added source release packaging.** `make release` produces a versioned tarball and SHA-256 checksum. `make release-check` verifies the checksum and reruns offline tests from an extracted copy.
- **Added deployment for applications embedding Dostoevsky.** `make deploy-build` builds a selected Go `main` package for Linux AMD64 or ARM64. `make deploy` installs the executable atomically into an explicit destination. The host application controls its journal, credentials, and restart.
- **Verified the deployment workflow with a separate Go application.** Built and ran the installed AMD64 executable, repeated the installation, and cross-compiled a statically linked ARM64 executable.

## 0.2.0 — Keyed notifications, batch reads, and storage interfaces

- **Added `Notifications[K]`.** A reader subscribes to a key before checking durable storage. After a successful commit, the writer signals that key and wakes the current subscribers. Channels are allocated lazily, active subscriptions are capped, and releasing the last subscriber removes the key. This supports database-backed readers without constant polling.
- **Added `Node.ReadBatch`.** Readers request a page with both record-count and byte limits and receive its resume cursor. A waiting read returns as soon as the first record is available and collects whatever else is ready. An oversized first record returns `ErrLimit`, allowing a retry at the same cursor.
- **Split storage capabilities into small Go interfaces.** Added `Publisher`, `Reader`, `Checkpointer`, `BatchReader`, and `BatchCheckpointer`, while preserving the combined `Store` interface. An application can supply its own batch reader and retain SQLite as its durable store.
- **Added `ConsumePages` and `Node.CommitBatch`.** A page consumer receives the expected prior cursor and the page’s next cursor. A SQLite adapter can commit projection output and cursor advancement in one transaction. Journal-backed consumers can advance several offsets and replace processor state in one conditional commit.
- **Added recovery and concurrency tests.** Covered subscription caps, shared wakeups, idle-key cleanup, byte and count boundaries, stale checkpoints, cancellation, and recovery of a multi-record checkpoint after reopening.
- **Added `make dev-bench-notify`.** On the tested Linux AMD64 host, repeatedly signaling a key with eight already-notified subscribers measured **13.00 ns/op and zero allocations**. This measures the signaling operation alone. End-to-end delivery latency and durable publish throughput require separate measurements.

## 0.2.1 — Public Go terminology and onboarding

- Replaced internal organization-specific terminology throughout the source, tests, benchmarks, and README. The documentation distinguishes a *consumer* that reads records, a *consumer group* that owns a checkpoint, and a *subscriber* that waits for a notification key.
- Reorganized the README around a runnable word-count example, followed by topics, keys, partitions, offsets, replay, and checkpoints.
- Added a tested Go documentation example showing how to subscribe to partition changes before checking storage.
- Rewrote the batch benchmark guidance around a portable workload: one consumer drains a backlog while another waits at the end of its partition. This was a source-compatible patch.

## 0.2.2 — OpenAI streaming and offline replay examples

- **Added a model-stream example.** `make dev-openai-text` uses Go’s `net/http` to call `POST /v1/responses` and append streamed JSON events to a Dostoevsky topic.
- **Added a hosted-agent example.** `make dev-openai-agent` calls `POST /v1/agents/sessions` directly, supplies the Agents API beta header, and records a hosted sandbox task through turn completion. Both examples use the standard library without an OpenAI SDK.
- **Added offline replay.** `make dev-openai-replay` reads recorded events from offset zero without credentials or another billable API request. An interrupted stream leaves a replayable prefix and reports incomplete termination.
- **Verified the HTTP behavior with local fixtures.** Tests cover both request paths, streamed event parsing, and a disconnect before completion. **Live OpenAI execution remains unverified.**

## 0.2.3 — Durable background task queue example (22 November 2026)

- **Added `examples/taskqueue`** with `make dev-task-enqueue`, `make dev-task-drain`, and `make dev-task-inspect`. A producer can submit a named job and exit. A later worker process drains the durable queue.
- **Demonstrated partitioned workers and acknowledgments.** Four partitions share one consumer group, with one worker goroutine per partition. The worker publishes a result, retry, or dead letter before committing the input offset.
- **Demonstrated three job outcomes end to end.** A SHA-256 task succeeds on its first attempt. A simulated transient failure succeeds on attempt two. A persistent failure produces a dead letter on attempt three with the original input and failure reason. Unknown tasks go directly to dead letters.
- **Tested the critical restart window.** After outcome publication and before acknowledgment, an interruption causes the handler to run again on restart. Stable IDs deduplicate the example’s retry and result publications. External effects such as sending email still require idempotency at the destination.
- **Module path is now `github.com/rodriveracom/dostoevsky`.** Dostoevsky is a standalone community project. Update your import paths and `go.mod`; GitHub redirects the old repository URL.
- README: added install and contribute sections.

## Verification and operating limits at 0.2.3

The final source package passed `make release` and `make release-check`, including formatting, static analysis, unit tests, race detection, checksum verification, and offline tests from the extracted release. The full ZIP’s SHA-256 is `589e885ab11a813ceb0b8120efef7d33796a15c11c8fc31367226d322ce131ea`.

| Area | Current boundary |
|---|---|
| Availability | Each partition has one statically assigned owner and one durable copy. An owner outage makes its partitions unavailable. Disk loss requires restoring a backup. |
| Storage growth | The journal retains history indefinitely. Startup scans it to rebuild indexes, and writes are serialized per node. |
| Consumer coordination | Applications assign workers to partitions. The task queue example implements immediate retries. Dynamic balancing, worker leases, and scheduled delivery remain outside its scope. |
| Batch transport | `Node.ReadBatch` and database-backed `BatchReader` implementations support pages. `Mesh` uses the single-record peer protocol. |
| Performance evidence | The proposed **100 durable publishes/s for one hour** test, with two checkpointing consumer groups, remains an acceptance target. This changelog records no completed run of that benchmark. |

## 0.3.0 candidate — Burst publishing, dynamic topics, retention and telemetry (unreleased)

- Documented durable claims, recipient batching, retry scheduling and separate body storage in `docs/delivery-workload.md`.
- Added a mergeable, fixed-memory latency histogram. The burst harness now reports percentile upper bounds using worker-local counters and avoids retaining and sorting every batch latency.
- Added `Node.PublishBatch` for up to 1,024 local-owner records and a 4 MiB budget. New journal frames share a sync. Stable IDs support retry after a partially written batch. Success does not provide an atomic multi-record transaction.
- Added opt-in dynamic topic names through a wildcard manifest, with independent partition offsets and topic-hashed placement. The mode is exclusive with named-topic declarations.
- Reduced per-stream allocations by creating wake channels and checkpoint maps lazily and storing the first producer ID inline. Added concurrent read locking.
- Added a reproducible burst, topic-cardinality, checkpointing and recovery workload through `make dev-burst`. Local measurements and limitations are recorded in `docs/burst-results.txt`.
- Added a mergeable, versioned HyperLogLog sketch with configurable precision and binary serialization for approximate distinct-count telemetry.
- Added `Node.Backup`, which copies and syncs a consistent journal into a new directory while holding the node lock.
- Added explicit per-partition `Node.Retain` with checks against existing consumer checkpoints, absolute offset floors, `ErrExpired` for old reads and stable directory locking. Retention rewrites and atomically replaces the journal. Producer IDs expire with deleted records.
- Added recovery of retained offsets and consumer state on reopen. Recovery scans the retained journal. Independent startup snapshots, an on-disk index, automatic retention scheduling, leases and delivery scheduling remain future work.
- Added tests for sketches, backup, batch retries, partial frames, notifications, concurrent access, retention, consumer checkpoints and reopening.
