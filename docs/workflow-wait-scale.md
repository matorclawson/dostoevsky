# Durable signal waits and workflow scale

A definition can declare a step with Kind set to "signal" and a name such as "approval". A run at this step is absent from the activity queue. It has no leased task, timer or per-run goroutine. The embedded store retains the run and its history in memory.

Call `store.Signal(runID, step, name, value)` with a zero-based step index and a valid JSON payload. A successful call durably advances the wait. The next activity receives the signal payload, or the workflow completes if the wait was its last step. Query History to inspect SignalWaiting and SignalReceived. A signal for a missing run returns workflow.ErrMissing. A signal sent early, repeated after acceptance, or addressed to the wrong step or name returns dostoevsky.ErrConflict. A caller that loses its reply can inspect history before retrying. No mailbox buffers signals sent before the matching step.

Stores using only activity definitions retain the existing transition format. The first signal definition uses a new record version so an older reader stops during replay. For an upgrade trial, use a copy and retain the original for rollback.

## Reproduce on Linux

Run from the checkout with a disposable directory on the target data disk.

```sh
mkdir -p "$PWD/bench-data"
DOST_WORKFLOW_BENCH_DIR="$PWD/bench-data" make dev-bench-workflow-scale
```

The Make target fixes GOMAXPROCS at 4. Each activity case completes 512 workflows with one-byte inputs and results. The timer includes start, each durable lease and each durable finish. Each reader-count case seeds its own directory with 100,000 acknowledged runs on a shared queue, closes the store, then starts a fresh process and opens it on a warm filesystem cache. The test verifies every run is at the expected wait and the activity queue is empty. Heap and resident memory are sampled before open and after replay and GC. RSS includes the Go runtime and allocator. The test measures a post-reopen snapshot, not a peak or a continuous idle soak.

The burst targets 1,024 distinct runs spread across the population. All signals arrive at the harness together, with 32 concurrent senders. The acknowledgment timer starts when a sender calls Signal. Worker pickup starts at common burst arrival and ends when Poll returns a durable lease to a worker. It includes producer backlog and consumer queueing. Each run ID and payload is verified for uniqueness. The test ends at delivery of the lease. No application activity or network call is timed.

## Measured results

Shared Linux AMD64, ext4, Go 1.24.4. One observation per configuration in the final run. Each reopening used a fresh process on a warm filesystem cache.

| Activities | Callers | Workflows/s | Workflow p99 ms | Logical bytes/run | Reopen 512 closed runs ms |
|---:|---:|---:|---:|---:|---:|
| 3 | 1 | 107.2 | 19.04 | 1647.5 | 18.50 |
| 3 | 8 | 523.7 | 21.21 | 1647.5 | 26.15 |
| 3 | 32 | 1899.2 | 21.21 | 1648.2 | 15.83 |
| 30 | 1 | 13.7 | 125.53 | 14622.9 | 120.07 |
| 30 | 8 | 51.6 | 205.20 | 14622.9 | 126.68 |
| 30 | 32 | 182.7 | 233.58 | 14623.6 | 134.14 |

| Polling workers | Reopen ms | Heap bytes/wait | RSS before → after (MiB) | Signal ack p99 ms | Burst to lease p50 ms | Burst to lease p99 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 588.29 | 517.1 | 6.5 → 87.7 | 6.15 | 749.81 | 1391.01 |
| 32 | 491.33 | 517.1 | 6.4 → 85.5 | 10.83 | 103.72 | 193.07 |

The 100,000-wait result applies to first-step waits and one-byte input on a single queue. Prior history, payload size, and allocator behavior affect capacity. A one-GiB limit and cold-cache restart were not exercised. The difference between acknowledgment time and burst drain time motivates measuring the number of durable leases acquired per disk sync.

[Raw final run](../benchmarks/workflow-wait/final-linux.txt). Process-kill and replay tests ran under the workflow race suite. Three pre-existing Unix-socket listener tests in the full repository suite were blocked by the host. No external engine was benchmarked.
