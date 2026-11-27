# Embedded workflows, first milestone

The workflow package opens a dedicated segmented directory. A separate node must own the user streams. Each workflow store has one directory lock and one writer. Consumers can run the [billing example](../workflow/example_test.go) with `go test -v -run ExampleStore ./workflow`.

Register an immutable, versioned sequence of activity steps. StartOnce binds a run ID to exact JSON input bytes, definition, version, queue and initial attributes. Poll leases work in due-time and run-ID order. Finish succeeds only for a current, unexpired token. Successful results become input to the next step. History pages use a global sequence cursor. Repeated execution of external side effects is possible. Use the run, step and attempt when recording activity-side receipts, and choose an idempotency key spanning attempts when the external effect should occur only once.

Attempts defaults to one. On expiry, polling fails that run if its attempts are exhausted. Otherwise it schedules a retry using fixed backoff. Retrying a lost StartOnce reply with identical bytes recovers the start decision. A lost Poll reply leaves the activity leased until expiry. A repeated Finish after success returns ErrStale; inspect History to reconcile it. An idle Poll writes nothing. There is no background timer thread.

Each record has a version and carries one decision, including its timestamp and any lease token. Up to four waiting commands share a writer group. The writer takes the first command immediately. It alone can access the projection and stages commands in their arrival order. It releases all results, including dependent rejections and history reads, after the append succeeds. An uncertain append fences reads and writes until reopen. A record is limited to 256 KiB after encoding; each Poll can advance at most 32 due entries. Names are limited to 256 UTF-8 bytes. Keep the store directory exclusive to this package. Reopen replays the complete history.

## Review and release boundary

This is an experimental embedded API. Closed runs, input, history and start receipts are resident. A durable on-disk historical index, snapshots, signals, parallel graphs, stable network receipts, migration and replication are subsequent work. Idle polls share the writer and can wait behind a sync. A late retry of Finish requires a history lookup. The public conformance scenarios cover a subset of Ephemeral; a general replacement for its API requires the rest of the v2 matrix.

A short final frame recovers its valid prefix. A checksum-damaged complete frame fails closed. Recovery cannot distinguish an unacknowledged tail with that shape from corruption of acknowledged data using the current format. Do not enable production workflow traffic or automatically truncate a damaged suffix on this candidate.

## Evidence and reproduction

- `make dev-test-workflows` runs the candidate through the race detector, including ten process-kill cases, deterministic group contention, fencing, and interrupted-group images.
- `EPHEMERAL_REFERENCE_MODULE=/path/to/module make dev-test-ephemeral` runs the shared public suite. The reference must vendor the module under the path ephemeral. This run used the portable SQLite 0.3.0 copy based on c250ba20 and Go 1.27.4. The design's dbf27040 and PR 20 were not the reference checkout. The package itself uses Go's standard library and Dostoevsky.
- `DOST_WORKFLOW_BENCH_DIR=/path/on/test-volume GOMAXPROCS=4 make dev-bench-workflows` measures 512 completed three-activity runs per cell, three repetitions, no network or external activity work. CPU is process user plus system time, disk is logical growth, and append groups count data commits for this no-rollover workload. Filesystem metadata syncs at lifecycle boundaries are outside the timing.
- See [raw measurements](../benchmarks/workflow/cost-ext4.txt), [workflow race tests](../benchmarks/workflow/race.txt), [SQLite reference tests](../benchmarks/workflow/sqlite-public-api.txt), and [stream-only binary comparison](../benchmarks/workflow/stream-only.txt). The stream server built with CGO_ENABLED=0, -trimpath and -buildvcs=false was byte-identical to commit 77834d8. This is a build isolation check. Co-located busy workflows and a network workflow server have yet to be measured.

## Measured cost on local ext4

Shared Linux AMD64, Go 1.24.4, GOMAXPROCS=4. Median of three short trials. Group counts exclude creation, registration and shutdown.

| Callers | Completed runs/s | Completion p99 ms | Data append groups/run | Process CPU µs/run | Logical bytes/run |
|---:|---:|---:|---:|---:|---:|
| 1 | 345.0 | 4.99 | 7.000 | 467.6 | 1647.5 |
| 8 | 1,287.4 | 13.60 | 1.752 | 205.0 | 1647.5 |
| 32 | 1,166.2 | 46.69 | 1.752 | 248.8 | 1648.2 |

Fresh-process open after seeding on warm OS cache, one observation per size. Each completed one-activity run contributes three transitions. Heap delta is measured after GC and covers the stream node as well as the workflow projection.

| Closed runs | Open ms | Retained heap delta KiB | RSS after open KiB |
|---:|---:|---:|---:|
| 0 | 0.76 | 25.2 | 7116 |
| 256 | 3.94 | 254.0 | 8868 |
| 4096 | 74.88 | 3691.3 | 14168 |
