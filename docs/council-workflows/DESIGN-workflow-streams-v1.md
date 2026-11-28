# Design: workflow streams v1 (Dostoevsky + Ephemeral as one product)

**Superseded by v2.** Sections on Ephemeral come from a source reading of Ephemeral `dbf27040`; nothing was built or run for them.
No code is merged by this note. Statements about today's behavior come from source or from the named receipts;
everything about the combined product is a proposal and has not been built or measured.

## 0. Decisions this note asks for

1. **Each workflow transition is one ordinary record** that holds all its effects. One record is already all-or-nothing, so
   version 1 needs no new journal format (1.1). An earlier draft proposed a new frame kind; one record per transition is simpler.
2. **Activities and workflow state are not built on today's consumer groups or `Fold`.** This departs from the originally proposed mapping (1.2).
3. **One process owns the data.** Ephemeral allows several processes to use one database; the combined product would not (1.3).
4. **"Beat Redis" means publishing**, with a fixed pass rule. Workflow throughput is measured and reported separately (4).
   Today Redis has the higher publish throughput in every cell we measured.
5. **Say "attempts-bounded redelivery", never "at-least-once".** Ephemeral's default is one attempt.

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

Proposal: the workflow module is the only writer of its log. Under its lock it decides, publishes one record holding every
effect, and applies it to its in-memory projection only after the durable acknowledgment. Replay applies recorded effects and
decides nothing again. A failed append stops the journal; the module must stop with it and reopen, never apply or retry.
Group commit still shares one sync among concurrent transitions.

**Limits (proposed).** A record value is at most 1 MiB. Client-supplied payloads (input, result, signal value, definition) are
limited **when accepted**, with room for the record's own fields. Records hold each payload once. Values the engine derives are
not written again: the join array, the input handed to each child and the set of runs reached by a cancel are computed when the
record is applied, by the same deterministic function live and on replay. So an accepted run can always be cancelled, expired
and joined. Derived history events must get the same sequence numbers on every replay. The payload limit is a compatibility
break. This narrows "replay decides nothing": replay still chooses nothing (no clock, randomness or ID generation), but it does
expand recorded decisions.

The workflow log's topic name is reserved: ordinary publishers must not be able to write it. A new frame kind would be needed
only to append to user streams atomically with a transition; v1 does not offer that.

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
behavior change. Ephemeral builds with system SQLite through cgo or, with `CGO_ENABLED=0` as in the release
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
| A failed operation changes nothing | `TestVersionFailureAndSignals` (invalid attribute) | Journal full and sync failure during a transition |
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

## 3. Dependencies, in order (historical)

**Written before pull request 84 merged:** it describes the candidate as it stood on 27 November, when its figures were
the external reviewers' own and not yet reproduced.

Nothing workflow-related starts before 1 and 2 are in.

1. **PR 84 merged**: tests, review and acceptance measurements.
2. **Memory and reopen bounds on segmented storage, measured by us.** The external reviewers report a 2.64 ms warm-cache reopen at 10M
   512-byte records, and 28,822 ack/s at 52.1 MiB against the legacy engine's 29,854 at 459.9 MiB with 10M retained IDs, 32
   producers. Our own measurement of the reopen step alone (PR 81, same-host A/B, ARM64, legacy format): 160,000 records 946 ms on
   main, 230 ms with PR 81, embedded JetStream 83.5 ms. **Condition for workflows:** reopen time and memory after open measured at
   16k, 160k, 1M and 10M records, cold and warm cache, with a stated bound that holds. Segmented storage stays opt-in until its
   throughput condition is met, so the workflow module must say which storage it requires; proposed: segmented.
   **These bounds are for the stream core.** A segment checkpoint holds no application state. Workflow reopen is a separate bound:
   load the module snapshot and replay the log after it, measured with 16k to 10M transitions and with many closed runs. Without
   module snapshots it replays the whole workflow log, so the cost grows with history.
3. **Segmented storage fit for workflow logs**: one-record atomicity and torn-tail recovery confirmed for it by review and crash
   tests; what its prefix retention may remove (workflow logs of live and closed runs must stay); rollover latency (the external reviewers
   saw 8 to 18 ms on first acknowledgments), which every lease and finish would feel.
4. **Native server** security decision (inside PR 84).
5. **Conformance suite** from Ephemeral's guarantees, green on the SQLite engine (2), then the **transition record** with crash
   tests at every append. Writing the suite can start now.
6. **Work queue with leases, the due-time index, and the on-disk index for closed runs.** The segmented ID filters and indexes
   index producer IDs only; they are not a run index. Includes module snapshots and their crash tests.
7. **Workflow module**, then the compatibility layer, then migration.

No date is offered for 5 (after the suite) to 7 until PR 84 is decided and 2 is measured.

## 4. Performance plan against Redis Streams (historical)

Superseded by v2 section 4 for pass rules and tracks. Kept for its baseline, durability settings and controls, which still
apply where v2 does not name a replacement. Later measurements of the native server are in v2.

### 4.1 Where we are (measured; Dostoevsky over HTTP/1.1, not the native server)

Baseline run, one Linux ARM64 host (2 vCPU), ext4, 512-byte records, durable acknowledgment, 5 rounds of 120 s. Median records/s
(median p99 ms). Source d7b967a, HOST.log sha256 `f94a43ce...0ac1ca`.

| Client | Producers | Dostoevsky | Redis 8.12.0 |
|---|---:|---:|---:|
| Python | 1 / 8 / 32 | 561 (4.44) / 1,908 (9.46) / 2,825 (26.88) | 625 (4.27) / 2,395 (7.83) / 5,505 (14.94) |
| JavaScript | 1 / 8 / 32 | 270 (7.16) / 1,309 (22.47) / 2,181 (51.83) | 643 (4.10) / 2,756 (6.74) / 8,249 (8.06) |
| Java | 1 / 8 / 32 | 490 (5.15) / 1,980 (10.34) / 3,403 (23.15) | 709 (3.90) / 3,218 (5.92) / 9,046 (8.56) |
| Rust (client stalled; fixed since) | 1 / 8 / 32 | 23 / 185 / 735 | 735 / 3,218 / 10,299 |

Redis is ahead in every cell, by up to about 3.8 times outside the stalled Rust rows. The Go control in the same run: embedded
(no network) 933 / 5,858 / 18,988; through the HTTP gateway 655 / 2,686 / 6,550. The gap between those two is the cost of the
server path on this host. Redis was not run with the Go client, so **embedded figures are not a comparison with Redis**; they only
bound what a server over this journal might reach. Dostoevsky's server memory was the smallest in all 36 comparisons (13 to
36 MiB peak) in 120 s trials; that says nothing about long histories.

The Rust fix (PR 74) was measured separately, Dostoevsky only: 587 / 2,419 / 6,617. Not a head-to-head with the table.

### 4.2 Levers, in the order to try them

Each is an A/B against main on the same host with a rule fixed before the run. None has a promised gain.

1. **Native binary protocol and clients** (in PR 84). Replaces HTTP parsing and base64 or JSON work. CPU per message through the
   gateway was about 200 to 400 µs against about 31 to 58 µs embedded (PR 64 A/B, 2 vCPU host), so the server path is where the
   time is. The external reviewers report Python 1 and 32 producers at 1,310 and 6,486 ack/s against JetStream's 1,022 and 1,743 and Kafka's
   620 and 790 (shared AMD64, not reproduced), and that Redis won its 32-producer group-of-16 comparison. Our reproduction,
   including Redis on ARM64, is in progress.
2. **Pipelining and reply coalescing**, so one connection can keep many publishes in flight and replies share writes.
3. **Binary records**: the candidate's segmented storage already uses binary frames, so this is no longer separate work. Its
   effect on server throughput against Redis has not been measured by us.
4. **Profile-led write path work** (allocation, locking, syncs).
5. Not counted: the group-commit gather change (PR 64) was measured and did not meet its rule.

At one producer both servers wait for one sync per record, so per-request overhead is all that differs; **my judgment** is that a
tie is more likely than a win there.

### 4.3 Measurement design

- **Host:** the smallest ARM server we use (2 vCPU, under 4 GiB), local ext4, nothing else running.
- **Equal durability:** Redis `appendonly yes`, `appendfsync always`; Dostoevsky as shipped. Weaker settings are never compared.
- **Matrix:** native protocol, Python, JavaScript, Java, Rust, plus one Go load generator that drives both servers; 1, 8 and 32
  producers; 512 bytes; unbatched and pipelined measured and labeled separately; 5 rotated rounds of 120 s.
- **Pass rule per cell, fixed in advance:** throughput is won only if every Dostoevsky trial is faster than every Redis trial;
  p99 only if every Dostoevsky p99 is lower. Anything overlapping is "no difference established". "Beats Redis" requires all
  cells of both; otherwise we list the cells won and lost.
- **Reported with it:** server CPU per message, server memory (sum of per-process sampled peaks, with that caveat), disk bytes
  per record, and the same figures after preloading 1M records (10M where disk allows).
- **Workflow benchmark (once the module exists):** start, three activities, complete; and one with a signal wait. For Redis the
  harness must do the same durable work (state and history changed atomically with the acknowledgment, through a script or
  transaction); comparing a workflow with a bare stream append would be dishonest. Also against Ephemeral on SQLite, so users
  know what the move costs or gains. Today's only Ephemeral figure is its one-run serial benchmark (160.8 runs/s on a 2 vCPU
  Linux ARM64 host, from its README), which is not comparable with any number above.
- Failed trials are published, not retried.

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

1. **One record per transition with decide-before-append** (including what apply derives), against a command log with
   deterministic apply; its size limit,
   recovery, and what `Retain` may drop. Payload limits and retention for workflow logs.
2. **A per-run index** for `describe`, `history` and attribute `find` that does not grow memory with closed runs.
3. **Live-run state in module snapshots** (not storage checkpoints): format, how large, how often, how published atomically, and
   its effect on the reopen bound.
4. **Leases and due times**: durable writes per poll, group commit under many idle pollers, wall-clock steps.
5. **Single owner process**: is the break acceptable, and what server and CLI shape replaces shared-database access.
6. **Review of this mapping**, especially the choice not to build activities on consumer groups or state on `Fold`.
7. **Partitioning by root run ID**, and what a single partition costs the throughput of wide fan-out.
8. **Wording**: attempts-bounded redelivery, not "at-least-once".
9. **Size bounds and derived effects**: do acceptance-time limits and values computed on apply guarantee that no run becomes
   impossible to cancel or join, with the same history on every replay?

Open risk, plainly: speed and a small footprint pull against each other once run state and indexes exist. The memory bound
in section 3 has to be restated for workflows and measured; it is not inherited from the stream core.
