# Design: workflow streams v2 (Dostoevsky + Ephemeral as one product)

Revision of v1 (original text sha256 `cc4236c8…`; the copy beside this was edited for publication) after external review, 28 November 2026. Sections on Ephemeral come from a source
reading of Ephemeral `dbf27040`. Statements about Dostoevsky in 1.4, 2 and 5 describe main `93870fe2`. No code is merged by
this note. Statements about today's behavior come from source or from named receipts; everything about the combined product is
a proposal and has not been built or measured.

**Changed from v1:** workflows get their own store (1.5); one writer with group commit and no lock held across publishing (1.6);
an explicit lost-reply contract (1.7); stream-triggered workflows (1.8); three scorecards instead of a Redis race (4); a first
milestone (3).

## 0. Decisions

**Decided:** one record per transition; one owner per workflow store; attempts-bounded redelivery (never
"at-least-once": Ephemeral's default is one attempt); a single workflow partition in v1.

**Carried from v1:** activities and workflow state are not built on consumer groups or `Fold` (1.2).

**Still open:** recovery of a torn group (1.6); whether to add a caller-chosen poll ID and a `Finish`
receipt (1.7). The first slice keeps today's behavior exactly, so it can be compared with Ephemeral; the poll ID is
recommended before any network client is called production-ready.

### Corrections to the framing

1. **"At-least-once activities" is not what Ephemeral does by default.** `Attempts` unset or 0 means one attempt. If that worker dies, the lease expires and the workflow is marked `failed`; the activity may or may not have had its effect. Redelivery happens only up to `Attempts` (engine.go `retry`). The design must keep this exact rule and must not advertise unconditional at-least-once.
2. **Activities cannot be literal consumer-group work.** A Dostoevsky group has one cursor per partition, committed in order. It has no per-message lease, acknowledgment, delay or out-of-order completion. The ready queue and lease table are new module state.
3. **There are no scheduled messages to map timers to.** Dostoevsky has no delayed delivery. Timers need a deadline index in the module.

## 1. Mapping

### 1.1 Atomicity: what the core has and lacks

Ephemeral commits state, pending work and history of a transition in one SQLite transaction. Dostoevsky today has:

- **One record or one commit is atomic.** `PublishBatch` is not: after a crash it can persist a prefix, or leave a frame that
  fails its checksum. `Commit` writes only a group's cursor and state.
- **No delayed delivery, no per-message acknowledgment or lease, no secondary index.**
- **One owner process per directory** (exclusive lock).

Proposal: the workflow module is the only writer of its log. Each accepted transition is one record holding every effect, and
its effects become visible only after the durable acknowledgment. Replay applies recorded effects and decides nothing again.
How concurrent commands share a sync is 1.6. (v1 said the module decides and publishes "under its lock" and also that group
commit shares a sync; both cannot hold. v2 removes the lock across publishing.)

**Limits (proposed).** A record value is at most 1 MiB. Client-supplied payloads (input, result, signal value, definition) are
limited **when accepted**, with room for the record's own fields. Records hold each payload once. Values the engine derives are
not written again: the join array, the input handed to each child and the set of runs reached by a cancel are computed when the
record is applied, by the same deterministic function live and on replay. So an accepted run can always be cancelled, expired
and joined. Derived history events must get the same sequence numbers on every replay. The payload limit is a compatibility
break. This narrows "replay decides nothing": replay still chooses nothing (no clock, randomness or ID generation), but it does
expand recorded decisions.

The workflow log is in a store of its own (1.5) that no publisher writes. Appending to user streams atomically with a transition
is not offered.

### 1.2 Concept by concept

| Workflow concept | Proposed on Dostoevsky | Note |
|---|---|---|
| History | the effects inside transition records, with a per-run index | v1 uses one partition, which keeps Ephemeral's global, ordered event cursor. Not a topic per run: every partition costs memory for its life. |
| Run state | state for every run, closed ones included, kept for as long as Ephemeral keeps it today (forever). Live runs in memory, rebuilt from transition records and saved in the module's own snapshots, each naming the transition offset it covers (storage checkpoints do not hold it); closed runs, signal IDs, `StartOnce` fingerprints and history behind an index that need not be in memory | Ephemeral keeps closed runs and answers for them (`StartOnce` replay, repeated signal, `Describe`, `History`, `Find`, `List`). **Not `Fold`**: one sequential reducer per partition, state at most 1 MiB. |
| Activities | a ready queue per queue name, with a durable lease, token, attempt count and due time | **Not a consumer group**: one cursor per partition, so a slow or retrying activity would block those behind it. As in Ephemeral, an idle poll writes nothing; a poll with due work writes, applying up to 32 transitions and possibly one lease; each heartbeat is a durable write. |
| Timers | a due time in run state, with an in-memory ordered index rebuilt on open | Firing is a transition. Ephemeral advances timers only when the queue is polled (as are expired leases, child starts and joins); the compatibility layer keeps that. A general "scheduled message" can be built on the same index later. |
| Signals | effects of a transition record, deduplicated by (run, signal key) with a content check | Today's ID rule is close (same ID and different content is refused) but is scoped to a partition and ends at retention. Workflow records of a live run must be exempt from retention. |
| Approvals | signals that a step waits for | No new primitive. |
| Child and parallel steps | one transition creates the children and the parent's join state | One record carries it all; parent and children share the log partition. |
| Plain streams | unchanged | Publish, consumer groups, `Fold` and `Pipe` stay as they are. |

Not in v1: workflows across a `Mesh`. A transition is atomic on one node only.

### 1.3 Ephemeral compatibility layer

The Go package can keep its API over the embedded module. The CLI is used by workers as separate processes on one database;
here only one process may own the directory, so the CLI must talk to the running server or hold the directory alone. This is a
behavior change. The compatibility `Open(path)` opens a workflow store directory; no stream store is needed. Backup and
recovery are per store: **a backup of both is not a consistent cut**, and restoring them to different times can replay or skip
stream triggers. A workflow that must emit a stream event does it from an activity, with the usual repeat risk. Ephemeral
builds with system SQLite through cgo or, with `CGO_ENABLED=0` as in the release
binary, with the modernc SQLite module. The combined core takes neither, so reading old databases needs a separate tool (5).

### 1.4 Ephemeral's side of the mapping

#### Proposed shape

One owner process. One ordered **transition log** topic. For each API call or due deadline the module, under its single writer: reads its in-memory projection, decides, then publishes **one record holding every effect** of that decision, and answers only after the durable acknowledgment. The projection is the fold of that log. Replay applies recorded effects and decides nothing again: times, lease tokens and child IDs are written into the record.

Why one record: an Ephemeral transition is one SQLite transaction spanning run state, history events, signal consumption, attributes, child creation and the parent wake-up. In Dostoevsky a single journal entry is the atomic unit. `PublishBatch` is documented as able to leave part of a batch, and `Commit` writes only a group's offset and state, so neither can carry a multi-effect transition.

Why not `Fold` as it stands: fold state is limited to 1 MiB per group and partition and is rewritten on each commit. It cannot hold all runs. "State via Fold" should mean the same pure-reducer idea, with an in-memory projection plus periodic snapshots (proposed).

Why decide before append: Ephemeral rejects synchronously (stale lease, signal ID conflict, start conflict, run closed). Appending commands and validating them later would change the API.

| Ephemeral | In the stream engine | Exists in Dostoevsky today? |
|---|---|---|
| History (`events`, per run, `seq`) | The effects inside transition records; per-run index for `History(id, after, limit)` | Log yes; per-run index no |
| Definitions (immutable name and version) | Records in the transition log, folded into a map | Log yes; map no |
| Run state (`runs`) | Projection | No |
| Activity ready queue, order (due, id) | Ordered ready set per queue in the module | No |
| Lease, token, heartbeat | Lease table; lease grant and each heartbeat are records | No |
| Retry delay, timer | Deadline index per queue; firing is a transition | No |
| Signal | A transition record; buffered unconsumed signals in the projection | Publish yes; per-run buffer no |
| Signal ID dedup, StartOnce fingerprint | Module state kept for the life of the run | `Message.ID` is not enough: scoped to topic partition and expires on retention |
| Child, parallel | Parent record carries the child creations; join waits for all terminal | No |
| Attributes, `Find`, `FindInState`, `List` | Ordered secondary indexes with keyset pagination | No |
| `NextDue` | Head of the deadline index | No |

#### Constraints this puts on the core

- v1 uses a single transition partition, so parent and child atomicity, queue order and today's global history cursor follow
  directly. Partitioning by root run ID is an open question (6).
- **Size limits.** Value 1 MiB, frame 4 MiB. Ephemeral sets no payload limit of its own. A 64-way parallel step passes the same input to every child, so the record must hold the input once and reference it. A hard input and result limit would be a compatibility break and must be stated.
- **Retention.** `Retain` drops records and their IDs. Workflow records for live runs, receipts and signals must be snapshotted first. Proposed rule for v1: no retention on workflow logs until snapshot and restore are tested.
- **Heartbeat cost.** Each heartbeat becomes a durable append, as it is a durable write today.

### 1.5 A separate store for workflows

By default workflows live in **their own segmented store directory**; one server process may serve both. Reason: space is
reclaimed only by whole prefix segments, so a single retained workflow record in a shared log would keep expired stream segments
on disk. Separate directories also give independent retention, recovery and backup.

**A streams-only deployment pays nothing it can avoid:** no workflow indexes are allocated, no scheduler goroutine starts, no
workflow file is opened. This is measured, not assumed (4, cost of combining).

Consequence: **streams and workflows share no commit.** Nothing is atomic across the two stores (1.8).

### 1.6 One writer, group commit, no lock across publishing

- One workflow writer goroutine owns the staged state and is the only appender.
- It takes the first waiting command **immediately** (no gathering delay), then drains whatever else is already waiting, up to a
  bound on commands and bytes.
- It validates the group **in order against staged state**, so a conflict between two commands of the same group is seen: the
  second sees the first's effect.
- It writes one record per accepted transition, one sync for the group, then applies them and replies. Refused commands are
  answered without a record.
- **No reply that depends on staged state before the sync, rejections included:** a "signal ID conflict" against a record
  that then fails to become durable would be wrong. Reads see only durable state.
- A group is several records, so after a crash a prefix may survive. That is consistent: each transition depends only on
  earlier ones, and none was acknowledged.
- **If the outcome of a write is uncertain** (error from write or sync), the writer fences itself: it answers nothing as
  successful, accepts no further commands, and recovers its state from disk before serving again.

**Acceptance:** two workers race for one activity while unrelated runs advance in the same group; exactly one gets the lease.

**Open item, may need a storage change (from source):** a crash with out-of-order page writeback can leave a complete but
damaged frame in an unacknowledged group, and recovery then stops with a corruption error and changes nothing. **A workflow
store must not need an operator after an ordinary crash during a group.** It needs a way to tell an unacknowledged group from
damage to acknowledged data (for example a group commit marker), with a crash test, before workflows ship.

### 1.7 When a reply is lost

**Rule.** A rejection changed nothing. A lost reply, a crash before the caller saw the reply, or an uncertain-write error means
the operation **may have committed**. What a caller does then:

| Operation | Today, if it had committed and is repeated | Contract for the first slice | Proposed addition (not in the first slice) |
|---|---|---|---|
| `StartOnce` | returns not-created | Retry the same bytes | none |
| `Start` | fails (run ID exists) | Not retry-safe; use `StartOnce` | none |
| `Poll` | the lease exists but its token cannot be read back (`Describe` has no token); a repeat gets nothing for that run | Wait for expiry. If attempts remain the activity is leased again; **with the default single attempt the run fails although the activity may never have run** | Network protocol: optional caller-chosen poll ID, recorded with the lease; a repeat while the lease is live returns the same lease |
| `Finish` (success or failure), `FailPermanent` | rejected as `stale activity lease`, the same answer as for an expired lease | Reconcile with `History`: find `ActivityStarted` for the task's step and attempt, then the first later `ActivityCompleted`, `ActivityFailed`, `ActivityTimedOut` or `WorkflowCancelled` for that step, ignoring other events. `ActivityCompleted` or `ActivityFailed` carrying the caller's bytes means accepted; `ActivityTimedOut` or `WorkflowCancelled` means not accepted; none yet means not accepted so far | Record (run, token, digest of the outcome); an identical repeat succeeds, a different one conflicts. Expired and never-accepted tokens stay stale |
| `Signal` | same ID, name and value: success, even after the run closed | Retry the same ID and bytes | none |
| `Heartbeat` | extends again while the lease is live | Retry; a stale answer means stop work | none |
| `Cancel` | success | Retry | none |
| `Register` | identical definition: success | Retry | none |
| `SetAttributes` | same values, but a second `AttributesUpdated` event | Retry is safe for values, not for history | none |

The `History` reconciliation is sound today because jumps are forward only, so a step runs once per run. **Loops (design B)
would break it**; they need an execution number in the task and history.

Recommendation: the first slice keeps today's behavior exactly, so it can be compared with Ephemeral. Add the poll ID before
any network client is called production-ready: a network makes lost replies ordinary.

**First end-to-end slice: `StartOnce`, `Poll`, `Finish`, `History`**

For each durable write: kill the server after the sync and before the reply, restart, reconnect, retry, compare.

| Kill point | Expected after retry (same as Ephemeral today) |
|---|---|
| after `StartOnce` creation | not-created; one run; one `WorkflowStarted` |
| after `Poll` grants a lease, attempts = 1 | no task; after expiry the run is `failed` |
| after `Poll` grants a lease, attempts = 2 | no task until expiry; then a lease with attempt 2 |
| after `Poll` applied due transitions but granted nothing | transitions present once |
| after `Finish` success | stale-lease rejection; `History` shows one `ActivityCompleted` with the bytes sent |
| after `Finish` failure with attempts left | stale-lease rejection; one `ActivityFailed` and one `RetryScheduled` |

Also kill **before the record is written**: the retry behaves as a first call. A process kill **between write and sync** usually
leaves the bytes in the page cache, so either outcome is correct there; the test must accept both and require that exactly one
holds. Only a power-loss test could show what the sync adds, and none is planned. The comparison side needs no kill hooks in
Ephemeral: make the call, discard its result, close, reopen and retry, through the public API. These go into the conformance
suite (Ephemeral pull request 20, merged).

### 1.8 Workflows triggered by a stream

Proposed: the consumer calls `StartOnce` and commits its cursor only after it succeeds.

- **Run ID** from (trigger name, topic, partition, offset). The trigger name keeps two triggers on one topic apart.
- **Every compared field must be reproducible**, because `StartOnce` compares exact bytes: definition name and version,
  queue, input and attributes. Input must be JSON, and stream values are arbitrary bytes, so the encoding is fixed
  (proposed: a JSON object holding the bytes in base64, one canonical spelling).
- **Trap:** if the trigger's definition version, queue or encoding changes between the start and the redelivery, the retry
  gets `ErrStartConflict` and the consumer stops at that offset. A trigger's settings must therefore be immutable; changing
  them means a new trigger name with an explicit starting offset (which can start a second run for offsets already handled).
- Offsets identify a record only within one stream store. Recreating the stream store can repeat offsets with new content;
  that must be refused or the trigger renamed.
- The receipt lives as long as the run, independent of stream retention.

Crash tests: (1) the consumer dies after the start succeeds and before its cursor commit: one run, retry returns not-created,
cursor then advances; (2) the same with the start's reply lost.

## 2. Guarantees carried over, with their tests

Existing tests use two in-process handles on one SQLite file, and some execute SQL directly. They cannot be run unchanged against a new engine. **Proposed first step:** rewrite them as a conformance suite that uses only the public API, run it green on the SQLite engine, then require the same suite on the new one.

| Guarantee (exact wording to keep) | Existing test | Gap to close |
|---|---|---|
| A committed transition survives process kill; an uncommitted one leaves no trace | `TestKilledWriter` (one case, uses internal SQL) | Kill at every append boundary of every operation; torn final record |
| Open resumes ready, leased, waiting and timed runs | `TestRecovery`, `TestParallelSwitchRecovery` | Reopen while a timer and a retry delay are pending |
| At most one live lease per activity | `TestCompetingWorkers`, `TestTwoWorkersOneLease` (20 rounds) | Many workers over the server protocol |
| Expired lease: redelivered only while attempts remain, otherwise the run fails | `TestRecovery`, `TestFixedBackoffAndRetryValidation` | Explicit test of the default single attempt |
| A stale or expired token cannot finish, heartbeat or fail permanently | `TestRecovery`, `TestRetryCalendar`, `TestCoordinationOps` | Stale heartbeat; stale token after reopen |
| Heartbeat extends a live lease | none in Go; `examples/demo.py` calls it once | Needs a real test |
| Retry delays: per attempt or fixed; permanent failure closes the run | `TestRetryCalendar`, `TestFixedBackoffAndRetryValidation` | none known |
| Timers fire no earlier than due, when someone polls | `TestRecovery` (1 ms), `TestNextDueEmptyAndOrdered` | Not-early assertion; clock step backwards. Behavior under clock jumps is untested today |
| Approvals: a signal sent before its wait is buffered; same ID and content is a no-op, different content conflicts | `TestRecovery`, `TestVersionFailureAndSignals` | Several buffered signals of one name consumed in arrival order (code does this, no test) |
| Definition versions are immutable | `TestVersionFailureAndSignals` | none known |
| `StartOnce`: one creation for identical bytes; any difference conflicts; a failed start binds nothing | `TestStartOnceReplay`, `TestStartOnceExactBytes`, `TestStartOnceComparedFields`, `TestStartOnceFailureLeavesNoReceipt` | Kill between append and reply, then retry |
| Child and parallel: results in declaration order; join waits for all children to end; any failed or cancelled child fails the parent | `TestParallelSwitchRecovery`, `TestChildFailureCancellationAndValidation` | Mixed success and failure in one parallel group; kill between last child and join |
| Cancel reaches active descendants | `TestChildFailureCancellationAndValidation` | none known |
| Switch routing, forward jumps only | `TestParallelSwitchRecovery`, `TestChildFailureCancellationAndValidation` | none known |
| Scoped, paginated lookup | `TestScopedRecoveryPages` | `List` pagination and `History` paging have no direct test |
| A definite refusal changes nothing; a lost reply may hide a completed operation (contract in 1.7) | `TestVersionFailureAndSignals` (invalid attribute) | Kill after each durable write, before its reply; journal full and sync failure during a transition |
| Idle poll does not wait on a writer | `TestIdlePollDuringWrite` (uses SQLite locking) | Restate as a latency bound or drop it, explicitly |

Not guaranteed today, so not to be claimed after the merge: exactly-once external effects, workers or timers without polling, authentication, encryption, purge, replication, power-loss testing.

### Where "exactly" cannot hold

- **Several processes on one database.** Today processes can open the same file (the tests do this with two handles). A Dostoevsky directory has one open writer. Multi-process users must move to the server protocol.
- **`Open(path)`** takes a database file today; Dostoevsky opens a directory.
- **Durability outside Linux.** Dostoevsky syncs with `fdatasync` on Linux and `File.Sync` elsewhere; Ephemeral relies on SQLite `synchronous=FULL`. Equivalence is not established on any platform by test here.
- **Reads after open (segmented storage).** Sealed segments are not all checksummed at startup. A history read can report
  corruption after a successful open; the compatibility layer must return that error.
- **Failed append.** The Dostoevsky journal records a failure and refuses further writes. The compatibility layer must surface that, not retry silently.

### 2.1 Test locations and what must commit together

| Test file (root of the repository unless shown) | Tests |
|---|---|
| `crash_test.go` | `TestKilledWriter` |
| `engine_test.go` | `TestRecovery`, `TestCompetingWorkers`, `TestVersionFailureAndSignals` |
| `coordination_test.go` | `TestStartOnceReplay`, `TestIdlePollDuringWrite`, `TestRetryCalendar`, `TestScopedRecoveryPages`, `TestStartOnceExactBytes`, `TestFixedBackoffAndRetryValidation`, `TestTwoWorkersOneLease`, `TestNextDueEmptyAndOrdered`, `TestOldDatabaseResumes` |
| `start_once_test.go` | `TestStartOnceComparedFields`, `TestStartOnceFailureLeavesNoReceipt` |
| `graph_test.go` | `TestParallelSwitchRecovery`, `TestChildFailureCancellationAndValidation` |
| `cmd/ephemeral/main_test.go` | `TestCoordinationOps` |
| `examples/` (run by `make examples`) | approval JSON lines, `demo.py`, `parallel.py`, `retry` |

Must be one atomic transition each:

| Operation or event | Effects that commit together |
|---|---|
| Start / StartOnce | run, `WorkflowStarted`, attributes and event, first scheduled step (including any immediate switch, child creation or completion), and for StartOnce the fingerprint |
| Poll grants a lease | token, attempt count, expiry, `ActivityStarted` |
| Finish success | `ActivityCompleted`, every following immediate step, new children, parent wake-up |
| Finish failure, lease expiry, permanent failure | the failure or timeout event, retry schedule or `WorkflowFailed`, token cleared, parent wake-up |
| Signal | signal ID and content, `SignalReceived`, and if waiting: consumption and the advance |
| Timer fired | `TimerFired` and the advance |
| Join | `ChildrenJoined` and the advance, or `WorkflowFailed` and parent wake-up |
| Cancel | the whole active descendant tree and the parent wake-up |

One `Poll` may apply up to 32 due transitions (timers, expiries, child starts, joins) and then grant a lease, all in one SQLite transaction today. Splitting these into separate records is acceptable only if each stays atomic by itself. Cancel of a large tree has no size bound and may not fit one 1 MiB record if written out; see the rule in 1.1.

## 3. Dependencies, order and first milestone

This design builds on pull request 84 (head `ed99753f`, merged). Our ARM64 measurements of it are used below.

1. **Conformance suite** for today's Ephemeral guarantees through the public API: Ephemeral pull requests 20 and 21 are
   merged (defaults, heartbeat, lease across reopen, signal order, repeats on closed runs, paging, timers, parallel failure,
   and lost-reply cases for `StartOnce`, `Poll` and `Finish`); a tightening of those tests is open. Still missing:
   kill-during-write cases, clock steps, storage failures during a transition.
2. **Candidate merged** (pull request 84).
3. **Bounds on segmented storage measured by us** (stream core only): per-size medians of 8.6 to 12.4 ms to reopen with the
   store's files evicted from cache, 160,000 to 10M records, about 7 MiB resident; memory grew with history when all of it was
   kept (24 to 68 MiB over 5M records). Segment checkpoints hold no application state, so workflow reopen needs its own
   test and bound: load the module snapshot and replay after it, with 16k to 10M transitions and many closed runs.
4. **Workflow store:** the separate directory, the writer of 1.6, transition records, module snapshots, each with crash tests.
5. **Work queue with leases, due-time index, on-disk index for closed runs.** The candidate's ID filters index producer IDs only.
6. **Workflow module**, then the compatibility layer, then migration.

**First milestone (nothing broader is claimed before it):**
- a crash-tested activity lifecycle (the slice of 1.7);
- measured cost per completed workflow: durable writes, bytes, CPU, latency;
- evidence that a streams-only user pays no measurable penalty.

No dates are offered for 4 to 6 until the workflow bounds in 3 are measured.

## 4. Scorecards

Redis is kept only as a regression series; it does not decide anything here. An embedded figure (such as 42,026
acknowledgments per second at 1 KiB, measured by the external reviewers) ranks nothing against a server, and the gain it was reported with did not appear on [our ARM64 host](../../benchmarks/fresh/README.md). Detailed benchmark rules are in the companion documents
against Kafka and Temporal.

Common rules: pass rule fixed before the run (a cell is won only if every one of our trials beats every peer trial; otherwise "no
difference established"); engine order rotated; failed trials published, not retried; **every peer result is published with that
system's acknowledgment, replication and failure settings** and what they can lose; same hardware.

| Track | Workloads | Measures |
|---|---|---|
| **Kafka** | fresh durable publishes at 1 and 32 producers; backlog drain while under load; reopen at 1M and 10M records | acknowledgments/s; publish p99 and consumer-visible p99; memory at boot and steady; disk bytes per record; records recovered |
| **Temporal and Ephemeral** | start plus three activities; wait for a signal and resume; heartbeats under load; a burst of timers | completed workflows/s; schedule-to-start p99 and completion p99; durable writes per completion; memory of the whole service; time from restart until workers can claim |
| **Cost of combining** | streams alone; workflow module present and idle; workflow module busy | the stream measures of the Kafka track in all three states, against streams alone |

**Open runs and closed history are varied independently**, including 1 million completed runs and 1 million waiting timers.

What we know today, all of it about publishing only and none of it about workflows.

**Our own ARM64 measurements of the native server** (candidate `ed99753f`, Python clients, one record per request,
512 bytes, durable acknowledgment, 5 rounds; medians in records/s). Dostoevsky ran **with segmented storage, which is opt-in
and not the default**:

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
- **Not the smallest memory:** JetStream and Redis used less than Dostoevsky in the same runs (median ready 18.2 MiB and
  largest per-cell median peak 31.6 MiB, against JetStream's 13.9 and 28.3 and Redis's 7.5 and 10.8; 512-byte records with
  32-character IDs, 2,048 per cell).

Ephemeral's only workflow figure is its serial benchmark: 160.8 one-activity runs per second on a 2-vCPU Linux ARM64 host
(its README).

## 5. Migration for existing Ephemeral users

How many people use Ephemeral, and how, is not known.

Proposed, in order:

1. **v0.3.0 stays as it is.** No retag, no format change, SQLite files keep working with the released binary.
2. **Same API, new engine, new major step.** The compatibility layer keeps the Go method set, the error values `ErrMissing` and `ErrStartConflict`, and the JSON-lines CLI operations. Because storage and process model change, it ships as a clearly versioned break, not a patch upgrade.
3. **Default path: drain.** Keep the old binary for existing runs, start new runs on the new engine, retire the SQLite database when no run is non-terminal. Needs no converter. Limit: long timers and open approvals can keep it alive for a long time, and run IDs and `StartOnce` receipts do not carry across, so a caller retrying a start on the new engine would create it again. Callers must route by where the run was started.
4. **Optional importer, only if a user asks.** A separate binary (it needs the SQLite dependency, which the new core must not take): requires the old writer stopped; copies definitions, runs, history with original sequence and time, buffered signals, attributes, children and start fingerprints; drops live leases so they expire under the normal attempt rule (with the default single attempt this fails the run, so the importer must refuse or require leases to be finished first); verifies counts and per-run state; never modifies the source. Rollback is to discard the new directory, valid only until the new engine takes its first write.

Also to settle: module path (`github.com/rodriveracom/ephemeral` is imported by name; a compatibility package there can forward to the new core), and Go version (Ephemeral requires 1.25, Dostoevsky 1.22).

### 5.1 Surface to keep and data on disk

Go: `Open`, `Store.Close`, `Version`; `Register`, `Start`, `StartOnce`, `Signal`, `Cancel`, `Poll`, `Finish`, `FailPermanent`, `Heartbeat`, `SetAttributes`; `Describe`, `History`, `List`, `Find`, `FindInState`, `NextDue`; types `Step`, `Child`, `Task`, `Event`, `Description`; errors `ErrMissing`, `ErrStartConflict`. Other errors are unnamed strings.

CLI: `ephemeral DATABASE`, JSON requests on stdin, one JSON reply per line, `--version`. Operations: register, start, start-once, signal, cancel, poll, finish, fail-permanent, heartbeat, attributes, describe, history, find, list, next-due. Replies use Go field names (`Run`, `State`, `Sequence`). Unknown request fields are rejected. The first error goes to stderr and the process exits nonzero.

There are **no transaction hooks** into a caller's own database: no exported method takes or returns a transaction or connection.

On disk: one SQLite database in WAL mode (with sidecar files). Tables `definitions`, `runs`, `events`, `attrs`, `signals`, `children`, and since v0.3.0 `start_requests`. v0.3.0 also replaces index `ready` with `runnable_due` on open. There is **no schema version number**. Tags in the repository: v0.2.1, v0.2.2, v0.3.0. `TestOldDatabaseResumes` simulates a 0.2.1 database by removing the newer objects; it does not open a file written by the old binary.

### 5.2 What cannot survive unchanged on a journal

1. Several processes opening the same store directly.
2. One database file as the argument to `Open` and as the unit users copy or inspect. Direct SQL access by users, if any do it, ends; this was never an API.
3. Unbounded transaction size (large cancel trees, 64-way fan-out with large input).
4. Payloads over the stream's value limit.
5. History and indexes held on disk and read on demand: unless the module keeps them on disk, memory grows with history. This depends on the reopen and history-memory work.

## 6. Open questions for review

1. **Staged validation and one record per transition** (including what apply derives): its size limit,
   recovery, and what `Retain` may drop. Payload limits and retention for workflow logs.
2. **A per-run index** (in the workflow store) for `describe`, `history` and attribute `find` that does not grow memory with closed runs.
3. **Live-run state in module snapshots** (not storage checkpoints): format, how large, how often, how published atomically, and
   its effect on the reopen bound.
4. **Leases and due times**: durable writes per poll, group commit under many idle pollers, wall-clock steps.
5. **The server and CLI shape** that replaces shared-database access.
6. **Review of this mapping**, especially the choice not to build activities on consumer groups or state on `Fold`.
7. **What a single partition costs** wide fan-out, measured; partitioning by root run ID stays later work.
8. **Torn groups** (1.6): can recovery discard an unacknowledged group safely?
9. **Size bounds and derived effects**: do acceptance-time limits and values computed on apply guarantee that no run becomes
   impossible to cancel or join, with the same history on every replay?

Open risk, plainly: "fastest" and "smallest footprint" pull against each other once run state and indexes exist. The memory bound
in section 3 has to be restated for workflows and measured; it is not inherited from the stream core.
