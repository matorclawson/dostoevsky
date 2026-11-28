# Design: Dostoevsky against Apache Kafka, v1

Companion to the workflow-streams design (v2, which revises v1 sha256 `cc4236c8…`). Written for external review. 28 November 2026.
Builds on the external candidate (pull request 84, head `ed99753f` with review fixes; approved for merge). "Have" below means present in its source.

Every number is marked **measured** (with its source) or **proposed**.

## Summary (one page)

**Kafka's game** is a replicated, partitioned log that many teams share: it survives machine loss, scales by adding brokers and
partitions, balances consumers automatically, and comes with a large ecosystem. **Dostoevsky today is a single-copy log on one
machine.** It cannot be offered as a Kafka replacement where losing a machine must not lose data.

**Where we can win:** one small static binary with no JVM and no dependencies, far less memory than Kafka (not the least of all brokers measured), fast start, low tail latency without
batching, retry-safe publishing by ID, and consumer state committed with the offset.

**What we measured, and its limits.** On a 2-vCPU Linux ARM64 server, one copy, fsync before acknowledgment, one record per
request: our HTTP gateway had higher throughput than Kafka at 8 and 32 producers in Python, JavaScript and Java, lower at one
producer in JavaScript, and used 13 to 36 MiB of server memory against Kafka's 567 to 645 MiB. **That setting removes Kafka's main
strength, batching**, and Kafka's normal durability comes from replication, not from fsync. We have not measured either engine
batched. We expect Kafka to be far ahead there until we batch.

**Our own ARM64 measurements of the native server** (candidate `ed99753f`, Python clients, one record per request,
512 bytes, durable acknowledgment, 5 rounds; medians in records/s):

| Producers | Dostoevsky | Redis Streams | JetStream | Kafka |
|---:|---:|---:|---:|---:|
| 1 | 619 | 587 | 468 | 215 |
| 8 | 2,268 | 2,128 | 626 | 302 |
| 32 | 5,059 | 4,306 | 712 | 281 |

- Faster than JetStream and Kafka in these unbatched runs (in an earlier run, one of our trials at 1 producer fell inside
  JetStream's range).
- p99 lower than Redis at 32 producers (13.4 against 52.9 ms).
- **No throughput difference established against Redis** at 1, 8 or 32 producers: the trial ranges overlap. Redis ran without
  publish deduplication.
- **Not the smallest memory:** JetStream and Redis used less than Dostoevsky in the same runs (18 MiB ready and up to 32 MiB
  peak, against JetStream's 14 and 28 and Redis's 8 and 11).

**To compete we must add, in this order:** (1) replication and automatic failover; (2) batched and compressed publish and fetch;
(3) server-side consumer group membership and assignment; (4) retention by time and size; (5) metrics, administration and
per-client authorization. Compaction and transactions follow.

**We deliberately do not play:** very large clusters, the Connect and Streams ecosystems, tiered storage, a schema registry.

**Kafka wire protocol:** recommendation is **not now**. Revisit after replication, as a separate optional bridge process.

**Claim rule.** We say "beats Kafka" only for the exact cells of a pre-registered benchmark where every trial wins (section 7).

**Decisions to be made:** replication design (section 6), the wire-protocol recommendation (section 5), the
not-play list (section 4), and the benchmark rule (section 7).

## 1. What Kafka's game is

| Capability | Why users pick Kafka for it |
|---|---|
| Durable, partitioned, ordered log | the core: many producers, replay, many independent readers |
| Replication and failover | a broker can die without losing acknowledged data (with suitable settings) and without an operator |
| Consumer groups with rebalancing | add or remove consumers and partitions move by themselves |
| Retention and compaction | keep by time or size, or keep the latest value per key |
| Idempotent producer and transactions | exactly-once processing inside Kafka |
| Batching, compression, zero-copy fetch | very high throughput per broker |
| Connect, Streams, client libraries | integrations that already exist |
| Tooling and operations | CLI, metrics, ACLs, quotas, many people who know it |
| The wire protocol | other systems (including competitors) speak it, so adoption is cheap |

## 2. Parity table (candidate source)

| Capability | State | Evidence and limit |
|---|---|---|
| Durable ordered log | **have** | `journal.go`, `segmented_node.go`; fsync before acknowledgment; one copy |
| Partitions | **partial** | key hashes to a fixed partition (`node.go`); the count is fixed for the life of the topic |
| Read by offset, replay, long poll | **have** | `Node.Read`, `ReadBatch`, `Await` |
| Consumer offsets | **have** | `Position`, `Commit`, `CommitBatch`; state commits with the offset |
| Group membership and rebalancing | **missing** | a group is a named checkpoint per partition; the application assigns partitions |
| Retention | **partial** | `Retain` with explicit floors; refuses to pass a group; no time or size policy; a journal size cap refuses publishes |
| Compaction | **missing** | none |
| Replication, failover | **missing** | `Mesh` (`peers.go`) places partitions on nodes over HTTP; each has one copy |
| Idempotent publish | **have, different** | by message ID, for as long as the record is retained |
| Transactions, exactly-once | **missing** | `PublishBatch` is not atomic; `Pipe` repeats safely using derived IDs |
| Batched publish on the wire | **missing** | the wire protocol publishes one message per request, pipelined within a window; the library has `PublishBatch` |
| Compression, record headers | **missing** | a message is key, value, ID |
| Stream processing | **partial** | `Fold` and `Pipe`: per partition, no joins or windows |
| Connectors | **missing** | none |
| Client libraries | **partial** | Go (embedded), Python, JavaScript/TypeScript, Java, Rust; not published to package registries |
| Security | **partial** | TLS; one token that grants the whole store; no per-client permissions |
| Quotas | **partial** | server-wide and per-connection resource bounds; none per client identity |
| Metrics, admin API | **missing** | no metrics endpoint; no operations to list or manage topics and groups over the wire |
| Offline tools | **have** | repair, verify, migrate, backup |
| Tiered storage, schema registry | **missing** | none |

## 3. What we must match, in order (proposed)

1. **Replication and failover.** Without it the comparison is not real (section 6).
2. **Batching and compression**, end to end: wire operation, atomic batch append, fetch of many records. Standard library
   compression only, or none; a new dependency needs a separate decision.
3. **Group coordination:** membership, heartbeats, partition assignment, fencing of a removed member's commits.
4. **Retention policies** by age and size, per topic.
5. **Operations:** metrics, topic and group administration, per-client credentials with read/write rights per topic, quotas.
6. **Later:** key compaction; transactions across partitions of one node; adding partitions.

## 4. What we beat, and what we do not play

### Beat (each needs a measurement before it is claimed)

| Axis | What is measured today | Still to measure |
|---|---|---|
| Server memory | **measured:** less than Kafka (18 to 32 MiB against 394 to 424, native server, ARM64). **JetStream and Redis used less than ours** in the same runs. In an earlier HTTP-gateway baseline ours was the smallest, 13 to 36 MiB (host log sha256 `f94a43ce…`) | long histories; replicated; batched; why ready memory varied between runs |
| Tail latency, unbatched | **measured:** p99 4.5 against Kafka 10.3 ms at 1 producer and 13.4 against 177 ms at 32; lower than Redis at 32 (52.9 ms), not established at 1 or 8 | batched; at fixed rates |
| Throughput, unbatched, one copy, fsync | **measured:** faster than JetStream and Kafka in these runs; no difference established against Redis. HTTP gateway (earlier): above Kafka at 8 and 32 producers, below at 1 producer in JavaScript (270 against 351) | batched; Redpanda |
| Start and restart | **measured, ARM64:** segmented storage reopens in 8.6 to 12.4 ms with the store's files evicted from cache, 160,000 to 10M records; the older format takes 2.0 s at 1M | **Kafka start and restart: never measured by us** |
| One binary, no JVM, no dependencies | a property of the source | deployment steps and files, counted side by side |
| Operations cost | nothing measured | a written task list (install, upgrade, add a node, replace a failed node) timed on both |

JetStream restarts faster than our strict scanner on the legacy format. It is not Kafka, but the claim "fastest boot" is not ours today.

### Do not play (and why)

- **Hundreds of brokers or very high partition counts.** Our memory goal and single-log design do not fit, and our users are small teams.
- **Kafka Connect and Kafka Streams parity.** An ecosystem is years of other people's work. We offer clear client APIs instead.
- **Tiered storage and a schema registry.** They add services; the product is one binary.

## 5. Should we speak the Kafka wire protocol?

**For:** existing producers, consumers, connectors and tools would work by changing an address. It is the cheapest route to adoption,
and other brokers have taken it.

**Against:** it is many request types, each versioned; consumers expect group coordination, idempotent producers and, for some,
transactions, so partial support fails in confusing ways; it would shape our storage and group semantics around Kafka's; and it
adds a large attack surface and binary size to a product whose point is being small.

**Recommendation (proposed): no for now.** Build our own protocol's batching and groups first. After replication, consider a
**separate optional bridge process** limited to a declared subset (produce and fetch without transactions), versioned and tested
against real client libraries, never inside the core binary. Ask users before building it: we have no demand data.

## 6. Replication design sketch (proposed; nothing built)

- **One consensus group replicates one node's whole log** to three nodes (Raft). Publishes, commits, retention floors and topology
  are all log entries, so a follower's state is the replay of the same log. One group keeps one fsync stream and today's ordering.
- **Acknowledge** after the entry is synced on a majority and applied by the leader. Group commit and pipelined appends share syncs.
- **Terms in the log format:** every entry carries term and index; the segmented format needs this before it becomes a default.
- **Snapshots** are sealed segments plus the segment checkpoint; producer-ID filters and indexes are rebuilt, not replicated.
- **Clients** are redirected to the leader; a publish with an ID is safe to retry after a leader change.
- **Scale-out later:** several groups, partitions placed across them. No cross-group transactions.
- **Standard library only** means writing Raft ourselves. **That is the largest risk in this document.** Required before any claim:
  a model-checked or exhaustively simulated implementation, a deterministic fault simulator (reordering, loss, duplication, torn
  writes, disk full, clock jumps), and kill and partition tests on three real machines with an external history checker.
- **Open questions:** one group per node against one per partition; synchronous quorum fsync against replicated page cache
  as an option (Kafka's usual trade); witness nodes for two-machine users; whether to adopt an audited external Raft library and
  give up "no dependencies".

## 7. Benchmark plan (proposed), in the shape of the OpenMessaging Benchmark

**Scorecard (the same cells as in the workflow design).** Fresh durable publishes at 1 and 32
producers; backlog drain while under load; reopen with 1M and 10M records. Measures: acknowledgments/s; publish p99 and
consumer-visible p99; memory at boot and steady; disk bytes per record; records recovered. Every peer result is published with
that system's acknowledgment, replication and failure settings. A third track measures the cost of combining: streams alone, the
workflow module idle, and the module busy; a streams-only user must pay no measurable penalty. The 8-producer cells and the
OpenMessaging shape below come in addition.

**Engines:** Dostoevsky; Apache Kafka (KRaft, version pinned); Redpanda (version pinned). The same client language and an
OpenMessaging driver for each; ours needs writing on the Java native client.

**Hardware:** (a) the smallest ARM server we use (2 vCPU, under 4 GiB, local disk); (b) three identical such servers on one private
network. We do not have (b) as a dedicated set today; it must be provisioned.

**Durability profiles, never mixed in one table**
- **P1, sync before acknowledgment:** one copy on (a); a synced majority on (b). Each engine is configured for it and the syncs are
  verified by tracing, not read from configuration.
- **P2, each vendor's usual replicated default**, on (b) only, reported with what it can lose.

**Workloads:** 1 KiB values (and 512 B for continuity); 1 topic with 1 and with 16 partitions; 1, 8 and 32 producers with matching
consumers; **unbatched and batched** (the same linger and batch size where an engine has them). Each cell runs at maximum
sustainable rate and at a fixed rate (half the lowest maximum among the engines).

**Run:** 5 minutes warm-up, 15 minutes measured, 5 rounds, engine order rotated, fresh data directories; one longer run with a
consumer stopped, then draining its backlog.

**Report:** publish and end-to-end latency (p50, p99, p99.9), throughput, peak resident memory, CPU seconds per million messages,
disk bytes per message, time to ready on empty and on loaded data, backlog drain rate. On (b): time to resume after killing the
leader, and a count of acknowledged messages lost (must be zero under P1).

**Pass rule, fixed before the run:** a cell is won on throughput only if every one of our trials is above every trial of the
competitor; on a latency percentile only if every one of ours is below; otherwise "no difference established". Kafka and Redpanda
are judged separately. "Beats Kafka" with no qualifier is used only if every cell of both profiles, batched and unbatched, is won
on throughput and p99 with no more memory. Otherwise we publish the table of cells won and lost. Failed trials are published, not
retried.

## 8. Order of work and conditions (proposed; no dates)

| Step | Condition before the next |
|---|---|
| 0. Merge the candidate | tests, review, acceptance measurements |
| 1. Batched publish and fetch, atomic batch | our cells against our own unbatched numbers; crash tests |
| 2. Metrics and administration | used by the benchmark itself |
| 3. Replication | simulator and three-machine fault tests with zero acknowledged loss |
| 4. Group coordination | rebalance under member kill without duplicate commits being accepted |
| 5. Retention policies, authorization | tests; security review |
| 6. The OpenMessaging comparison, published in full | section 7 |

Single-node benchmarks may run after step 1; **no replacement claim before step 3.**

## 9. Risks

- Replication in the standard library could take longer than everything else together.
- Each addition (groups, metrics, authorization) costs memory and binary size; the small-footprint claim must be re-measured at
  every step, with a stated budget.
- Unbatched results flatter us. Publishing them without the batched ones would mislead.
- The workflow module (companion design) competes for the same people and the same log format.
