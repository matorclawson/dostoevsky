# Design: workflows against Temporal, v1

Companion to the workflow-streams design (written against v1, original text sha256 `cc4236c8…`, "the v1 design"; updated for the
changes that go into v2). For external review. November 27, 2026.

**How to read the numbers.** **[measured]** has a receipt named beside it. **[documented]** is what Temporal's own documentation says,
read on 27 November 2026, not tested by us. Everything else is **[proposed]**. We have **not run Temporal**, and nothing in this
document is a performance comparison. Pull request 84, which this builds on, is merged.

---

## Summary (one page)

**Temporal's game** is workflows written as ordinary code that survive failures, because the SDK replays the recorded history
through that code. Around it: eight SDK languages, signals, queries, updates, schedules, search, a web UI, a CLI, and a
server that scales out over an external database.

**Where we are.** Ephemeral is a small durable engine for workflows declared as **data** (a forward-only step graph). It has
activities, retries, timers, signals, child and parallel steps and exact-attribute lookup. It has no workflow code, queries,
updates, schedules, loops, UI, network server, authentication or replication. The v1 module keeps that behavior on Dostoevsky's
journal; it does not exist yet.

**We cannot beat Temporal at Temporal's whole game with this plan, and should not say we will.** The reachable claim is narrower:
*for teams whose process is a bounded sequence of steps, one small binary does it with far less to operate.* Even that claim is
unproven until measured, and it has two honest limits:

1. Temporal also ships a **single-binary development server** [documented], which its documentation says is not for production.
   "One binary" distinguishes us only once ours is supported for production.
2. v1 is **one node with no replication**. A production Temporal cluster survives node loss; we would not.

**Recommendation on the key fork.** Keep **workflows as data** as the engine contract. Do not build a code-with-replay SDK now.
Spend the effort on what the step graph lacks (bounded loops, dynamic fan-out, access to earlier results, queries, schedules)
and on a network protocol usable from any language. Ask reviewers whether implementing Temporal's server API, so that
Temporal's own SDKs can connect, is worth studying later.

**Order of work:** the v1 dependencies; a server with authentication; the five step-graph gaps; operator tooling; benchmark;
only then any claim against Temporal. Replication is shared with the Kafka design and is required before any claim about availability.

**Decisions asked:** (1) data, not code, for v1 and the next version; (2) "beat Temporal" is limited to named, measured cells on
named hardware, plus footprint and operating cost; (3) no automatic migration from Temporal is promised.

---

## 1. Temporal's game [documented]

| Area | What Temporal provides |
|---|---|
| Programming model | Workflow code; on replay it "has to make the same decisions when given the same history". Activity results are reused, not re-run. |
| SDKs | Go, Java, PHP, Python, TypeScript, .NET, Ruby, Rust |
| Activities | Retried by default with **unlimited** attempts (1 s initial interval, coefficient 2.0); documentation expects them to be idempotent |
| Messages | Signals (asynchronous writes), queries (read-only, no history), updates (synchronous, tracked writes) |
| Long histories | Warning after 10,240 events; terminated above 51,200 events, 2,000 updates or 10,000 signals; continue-as-new starts a fresh history |
| Changing running code | Patching; worker versioning |
| Time and routing | Timers, schedules with overlap policies, task queues, child workflows |
| Visibility | Search and listing; on SQL since 1.20; Elasticsearch **optional**, recommended for production |
| Persistence | Cassandra, PostgreSQL or MySQL; SQLite for development and testing only |
| Server | Four services (frontend, history, matching, worker); MIT license |
| Tools | Web UI, CLI, development server |
| Multi-cluster | Replication marked **experimental**, not strongly consistent |
| Load testing | `omes`, mainly for the Temporal team's own use |

A correction to the framing we were given: a self-hosted Temporal does **not** require Elasticsearch, and not Cassandra
specifically. A fair comparison is the server plus one PostgreSQL.

Sources: [persistence](https://docs.temporal.io/temporal-service/persistence),
[dev server](https://docs.temporal.io/cli/server), [events](https://docs.temporal.io/workflow-execution/event),
[continue-as-new](https://docs.temporal.io/workflow-execution/continue-as-new),
[messages](https://docs.temporal.io/encyclopedia/workflow-message-passing),
[retries](https://docs.temporal.io/encyclopedia/retry-policies), [workflows](https://docs.temporal.io/workflows),
[patching](https://docs.temporal.io/patching), [schedules](https://docs.temporal.io/schedule),
[task queues](https://docs.temporal.io/task-queue), [server](https://docs.temporal.io/temporal-service/temporal-server),
[multi-cluster](https://docs.temporal.io/temporal-service/multi-cluster-replication),
[omes](https://github.com/temporalio/omes). The worker-versioning and workflow-limits pages were not read.

## 1a. Licenses

The Restate server is licensed under the Business Source License 1.1, which its own text says is not an Open Source license; Dostoevsky is MIT. (Restate license text: <https://github.com/restatedev/restate/blob/85c633357c22800d7e689e7506c19fdfb4c01a66/LICENSE>, checked November 28, 2026.)

## 2. Parity table

"Today" is Ephemeral v0.3.0 engine code (main `dbf27040`, from source). "v1" is the proposed module in the v1 design.

| Capability | Today | v1 [proposed] | Evidence and gap |
|---|---|---|---|
| Workflows as code with replay | Missing | Missing | Definitions are `Step` data (`engine.go`) |
| Step graph: sequence, switch, child, parallel | Have | Same | `graph.go`. Forward jumps only; parallel is a fixed list of up to 64 |
| Loops, dynamic fan-out | Missing | Missing | Graph validation refuses backward jumps |
| Data flow between steps | Partial | Same | Each step receives only the previous value; earlier results are not addressable |
| Activities with durable leases | Have | Same | `Poll`, `Finish`, `Heartbeat` |
| Retries | Partial | Same | Default is **one** attempt; delays fixed or listed; no exponential policy or per-error rules (permanent failure is explicit) |
| Activity and workflow timeouts | Partial | Same | Lease expiry only; no overall activity or workflow timeout |
| Heartbeat details, cooperative cancel | Partial | Same | Heartbeat carries no data; a canceled run's worker learns when heartbeat or finish fails |
| Timers | Have | Same | Advance only when the queue is polled |
| Signals | Have | Same | Buffered, deduplicated by ID |
| Queries | Partial | Same | Read run state and history; no workflow-defined query |
| Updates | Missing | Missing | |
| Child workflows | Have | Same | Parent waits; no detached child |
| Continue-as-new | Missing | Missing | Matters only once loops exist |
| Versioning | Have, different | Same | Immutable (name, version); a run keeps its version. Moving a live run to new logic is not possible |
| Schedules, cron | Missing | Missing | |
| Visibility and search | Partial | Partial | Exact string attribute plus state; keyset pages; no ranges, counts or full text |
| Task queues | Partial | Same | Queue is per run (children may differ); an activity step cannot name its own; no long poll |
| SDKs | Go embedded only | Go embedded, network protocol | Other languages today run the CLI and exchange JSON lines |
| Network server, authentication, TLS | Missing | Server from PR 72/84; its security review is still open | Workflow operations on the wire are not designed |
| Namespaces, multi-tenancy | Missing | Missing | |
| Web UI | Missing | Missing | |
| CLI | Partial | Same | JSON requests on stdin; first error ends the process |
| Retention and purge | Missing | Missing | History is kept forever |
| Encryption at rest | Missing | Missing | |
| Replication, multi-cluster | Missing | Missing | One node |

## 3. Match, beat, or not play [proposed]

**Must match** to be a credible choice for production: network protocol with authentication and TLS; clients for Python and
TypeScript first, then Java and Rust; retry policies with exponential backoff; activity and workflow timeouts; schedules; a way
to see and cancel runs (CLI first); retention and purge; backup and restore; metrics.

**Aim to beat**, each only after measurement (section 6):

| Claim to test | Why it is plausible | What is known now |
|---|---|---|
| Footprint and operating cost | No external database or search cluster; one process | Publishing only, not workflows: the HTTP gateway peaked at 13 to 36 MiB in 120 s trials **[measured]** (source `d7b967a`, `HOST.log` sha256 `f94a43ce…0ac1ca`); the native server used more memory than JetStream and Redis in a later publishing comparison (table in the Kafka design, same directory), so no "smallest" claim is made. Says nothing about long histories |
| Start and activity-dispatch latency | No network hop to a database; one durable write per transition | None for the combined product |
| Cold start | Open a directory, no schema service | For the stream core only, not for this design: segmented storage reopened in per-size medians of 8.6 to 12.4 ms in our run on Linux ARM64 (candidate `ed99753f`, store files evicted from the page cache, directory entries still cached, 160,000 to 10M records, 512-byte values). Excludes workflow state, which has not been measured |
| Workflows next to streams | One server can serve both | Design only. They use **separate stores** with no shared commit; a stream-triggered workflow starts through `StartOnce` keyed by (topic, partition, offset) |

For scale, the only workflow figure we have: Ephemeral on SQLite completed 160.8 one-activity runs per second, p99 10.48 ms,
sequential, in-process, one run on Linux ARM64 with 2 vCPU **[measured]** (source `c2e078a2`, `BENCH.log` sha256 `4d253617…`).
It cannot be set against any Temporal figure.

**Deliberately not playing** in this horizon:

- **Code-with-replay SDKs** (section 4).
- **Horizontal scale of one cluster.** One transition log and one owner is the v1 design.
- **Multi-cluster active-passive** as Temporal offers it; replication within one cluster comes first, with the Kafka design.
- **A search engine.** Exact-match attributes and state only.

## 4. The key fork: step graph or code with replay

| | Workflows as data (today) | Code with replay (Temporal's model) |
|---|---|---|
| Expressiveness | Limited to what the graph has | A full programming language |
| Determinism rules for users | None | Required; breaking them fails replay |
| Changing logic | New definition version; old runs finish on old | Patching or worker versioning |
| Any language | Anything that speaks the protocol; workers only run activities | One SDK per language, each with a replay runtime |
| What we must build | A few step kinds, a protocol, thin clients | Replay runtime, determinism sandbox or rules, and its tests, per language |
| History growth | Bounded by the graph (until loops are added) | Needs continue-as-new |
| Temporal users moving over | Rewrite workflow logic | Still a rewrite, unless the APIs match |

**Recommendation: stay with data.** The engine stays small and testable, thin clients are cheap, and users write no
deterministic code. The cost is real: some processes will not fit, and we should say so to those users.

Five additions, each needing its own design: (1) named earlier results as step input; (2) bounded loops with a required
iteration limit; (3) fan-out over a list computed at run time, with a limit; (4) a query that reads named values; (5) schedules.
Each adds durable state, so the v1 size rule (no transition may become impossible) must hold for each.

**A third option, not recommended for now:** implement Temporal's server API so Temporal's own SDKs connect. Replay runs in
the SDK, so we would get code workflows without writing SDKs. The price is a large protocol surface that we have not
inventoried, a moving target maintained by someone else, and a dependency on gRPC and protocol buffers that conflicts with the
standard-library-only core. A study should come before any promise.

## 5. Migration for Temporal users [proposed]

There is no automatic path, and running histories cannot be imported: they record the decisions of their code.

1. Choose processes that are bounded step sequences. Keep the rest on Temporal.
2. Keep activity code; wrap it in a poll-and-finish worker.
3. Rewrite the workflow as a definition.
4. Start new executions here; let running ones finish there. Route callers by where an execution started.

| In Temporal | Here | Trap |
|---|---|---|
| Activity retry (unlimited by default) | `Attempts`, `retry_delays_ms` | **Default is one attempt.** A migrated activity without explicit `Attempts` is not retried |
| Signal | `Signal` with an ID | A step waits for one name |
| Child workflow | `child` step | Parent always waits |
| `Promise.all` and similar | `parallel` step | Fixed list, up to 64 |
| `if` / `switch` | `switch` step | One string field |
| Query | `Describe`, `History` | No custom logic |
| Update, schedule, loop, continue-as-new, patching | none | Blocks migration until built, except patching, which versions replace only for new runs |

## 6. Benchmark plan [proposed]

**Systems.** Our server with the workflow module; self-hosted Temporal (version pinned at run time) with PostgreSQL, no
Elasticsearch; Ephemeral on SQLite as a baseline. Not the Temporal development server, which is not meant for production.

**Hardware.** The smallest ARM server we use (2 vCPU, under 4 GiB), local ext4, nothing else running, all server and database
components on that host. If Temporal cannot run its workload there, report that and repeat both on the next size up; do not report a win by
default. The load generator runs on a second host for both.

**Equal durability.** PostgreSQL `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`; our server as shipped, with every
acknowledgment after sync. One node each: this compares no availability. Weaker settings are never compared.

**Workloads** (the Temporal and Ephemeral track), written once per system with the same work:

1. Start, three no-op activities in sequence, complete.
2. Start, wait for a signal, resume, complete.
3. Heartbeats from many long activities while workload 1 runs.
4. A burst of timers becoming due together.

**Retained state is varied independently of load:** none; 1 million completed executions; 1 million executions waiting on timers.

**Metrics**, measured by the load generator so both are counted the same way:

- Completed workflows per second at 1, 8 and 32 concurrent starters.
- Activity schedule-to-start latency p99 and workflow completion latency p50 and p99.
- Durable writes (syncs) per completed workflow.
- Full-service memory (all server and database processes, sum of sampled peaks, with that caveat), CPU and disk bytes per
  completed workflow.
- Recovery time after a kill, until workers can claim activities again.
- State transitions per second, **reported only as each system defines it and never compared**: the units differ.

Every published peer result states its acknowledgment, replication and failure settings. A separate cost-of-combining track
(streams alone, workflow module idle, module busy) belongs to the v2 design, not to this comparison.

**Pass rule, fixed before the run.** 5 rotated rounds of 120 s after warm-up. A cell is won only if every one of our trials is
better than every Temporal trial; overlap is "no difference established". Any lost, duplicated or wrongly failed execution
voids our result. "Beats Temporal on this hardware" may be said only if throughput and both latency p99s are won in every
workload and concurrency; otherwise we list cells won and lost. Failed trials are published, not retried. Temporal's settings
are published and open to correction; defaults tuned for large clusters (for example shard count) are set for a small host by
a rule written before the run.

## 7. Dependencies and risks

First milestone: a crash-tested activity lifecycle, a measured cost per completed workflow, and evidence that a
streams-only user pays no measurable penalty. Order: the dependency steps of the v2 workflow design (section 3); wire operations for workflows and authentication; clients; step-graph additions; operator
tooling; benchmark. No dates: the first dependency, pull request 84, has merged, and the workflow bounds are not yet measured.

- **Footprint against indexes.** Run state, per-run history and attribute indexes will add memory and disk to the stream core.
- **One node.** Until replication exists, say "single node" wherever we compare.
- **Long polling.** Dispatch latency depends on waking waiting workers; today's API only offers a next-due hint.
- **Expressiveness.** If users need loops and code, the recommendation in section 4 is wrong for them.

## 8. Questions for reviewers

1. Is workflows-as-data enough to be worth choosing over Temporal, and for whom?
2. Do the five additions keep the engine's guarantees and the size rule? Which first?
3. Is a Temporal-API study worth its cost, given the standard-library-only core?
4. What is the smallest availability story (replication of one transition log) that makes production use reasonable?
5. Is the benchmark fair to Temporal? What would its authors object to?
