# Rust write path against the Go engine

**Frozen benchmark record.** The code here is exactly what was measured and is not maintained. It is superseded by the
faithful Rust port of the engine, and it is not part of that port's workspace or its format and lint checks. On Rust 1.100
`cargo fmt --check` fails, and `cargo clippy -- -D warnings` reports three errors (a constant-size `chunks_exact`, a manual
`is_multiple_of` and a very complex type); both are left as they are.

A prototype of the segmented log's write path in Rust (`benchmarks/rustcore/dostoevsky-core`), measured against
the Go engine on one machine. Results and receipts are in `RESULT.md` and `receipts/`.

## What is compared

| | Go | Rust |
| --- | --- | --- |
| Code under test | The engine's public API: `OpenSegmented`, `Publish`, `PublishBatch`, `Read`, `ReadBatch` | `dostoevsky_core::Store`: `open`, `publish`, `publish_batch`, `read` |
| On-disk format | Segmented, frame version 3 | The same bytes |
| Acknowledgment | After `pwrite` and `fdatasync` of the active segment | The same |
| Group commit | An isolated caller writes inline; callers that arrive during a write are written as one group with one sync; up to 64 queued; 100 µs gather | The same policy, with one OS thread per producer and one drainer thread |
| Rollover | At 8 MiB of appended tail: stage, sync, rename, sync the directory, publish the range file; then write index and filter sidecars | The same, without the sidecars |
| Harness | `benchmarks/rustcore/goab` | `benchmarks/rustcore/dostoevsky-core/src/bin/rsab.rs` |

Both harnesses run the producers inside the engine's process, take the same arguments, and
print one JSON object. `run.sh` drives both.

## What the Rust prototype leaves out

It is a write path, not an engine. It has no idempotency IDs or ID index, writes no index or
filter sidecar files (the Go engine treats them as hints and rebuilds them), and has no
retention, no commit API (commit frames are replayed and carried through checkpoints), no
multi-partition topics, no legacy journal, no workflows and no server.

**This biases the comparison towards Rust on memory and on per-record CPU**, because the Go
engine carries state and checks the prototype does not. It biases reads of old data against
Rust, which rebuilds a sealed segment's index by scanning it.

The Rust prototype uses only the standard library, so it blocks one OS thread per producer.
A Rust server would use asynchronous I/O. Read the 32-producer result with that in mind.

## Rules of the runs

The pass rule further down was stated before any run. The measurement
rules here are what `run.sh` does. `run.sh` was written before the first timed run, with one
exception: the segment trimming in the sustained stage (rule 5) was added after a first attempt
was stopped because it would have filled the test disk.

1. Same machine, same directory tree, same payload (100 bytes, empty key, empty ID, one topic
   with one partition), engines alternated within each trial.
2. Throughput: closed loop, 1 and 32 producers, 20 seconds, 3 trials each, a fresh store per
   run. A difference counts only if it holds in **every** trial.
3. Latency: time from the call to its return, per publish, in a log-linear histogram (about
   3% resolution); p50, p99, p99.9 and maximum.
4. Pauses: publishes slower than 10, 50 and 100 ms; the longest gap between two consecutive
   acknowledgments; the lowest one-second throughput. For Go also the collector's cycle count
   and total pause.
5. Sustained: 32 producers for 30 minutes at full speed. That writes more than the test disk
   holds, and neither engine reads a sealed segment while it only publishes records without
   IDs. So a helper deletes all but the newest 64 sealed segment files, with their sidecars,
   every 5 seconds, for both engines alike. The stores it leaves are not reopened.
6. Memory: resident set after open ("ready"), the median over the last third of the run
   ("steady"), and the peak.
7. Reopen: stores of 1M and 10M records, each engine on a store it wrote; process start to
   the first successful read of the newest record; cold cache (page cache dropped before each
   run) and warm; 10 runs each; median and range.
8. Conformance comes first: each engine must open the other's store, append to it, and both
   must then compute the same digest over every record; the same after a torn tail. If this
   fails, no timing is reported.

## Pass rule for porting the core to Rust

Stated before these runs. Port only if **all** hold:

1. acknowledged throughput at least equal to Go's at both 1 and 32 producers;
2. p99.9 latency at most 0.7 of Go's, **or** steady memory at most 0.6 of Go's;
3. no pause above 10 ms in 30 minutes, where Go shows a measurable count;
4. reopen at 10M records no slower than Go, cold and warm;
5. the conformance suite passes.

Otherwise the core stays in Go.

## Run

**`run.sh` is a record of how the numbers were made, not a safe tool. Run it only on an isolated, disposable Linux host.**
It is kept as it was, with these properties:

- It does not check its two arguments. `WORKDIR` and `OUTDIR` must be new, empty, disposable directories. With an empty or
  mistaken `WORKDIR` it writes `bin/` and removes `conf-*`, `w-*`, `s-*` and `r-*` paths under whatever that expands to; as
  root with an empty `WORKDIR` that means `/bin/goab` and matching names directly under `/`. It overwrites files in `OUTDIR`.
- The `reopen` and `read` stages drop the **whole host's** page cache and need root.
- It builds inside the checkout (`dostoevsky-core/target/`).
- The `sustained` stage needs `SUSTAINED_ORDER` and runs a background helper that deletes segment files. If an engine fails,
  the script stops without stopping that helper.

**Its exit status certifies nothing.** The `conform` stage writes both engines' digests to `conformance.txt` but does not
compare them, and some timed pipelines return the status of the text filter, not of the engine. Agreement in the published
receipts was checked by reading them.

```sh
benchmarks/rustcore/run.sh /path/to/new-workdir /path/to/new-outdir conform           # one stage
SUSTAINED_ORDER="go rust" benchmarks/rustcore/run.sh /path/to/new-workdir /path/to/new-outdir   # all stages
```

Stages: `env`, `conform`, `write`, `sustained`, `reopen`, `read`.

## Provenance and rounding

These files were published from an original commit (`bb439264`) that is not public; the code and receipts are byte for byte
the same, and only wording in the two documents differs. Two figures in `RESULT.md` are rounded or partial: the warm
sequential-read ratio reaches 3.95 (written as 3.9), and 28.7 records per sync is from the short runs (28.9 in the
30-minute run).
