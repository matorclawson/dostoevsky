# Workflow commit boundary

Review branch council/workflow-commit-boundary against aecbb08. This change covers single-owner embedded workflows. Stream publishing keeps its existing durable-write path.

## Ordering and recovery

Under the existing directory lock the workflow writer stages a group, appends its records and syncs the segment data. It extends a running SHA-256 hash with the newly written bytes. The hash includes the store generation and each segment ID. A new journal.v1 witness contains the generation, last committed segment, exclusive byte offset, cumulative digest, and a checksum over the witness. The writer syncs the temporary witness, renames it over journal.v1, syncs the directory, then releases the replies. An append group therefore normally incurs three sync operations in the absence of rollover. Logical disk growth per run does not measure physical I/O amplification from metadata replacement.

Open checks the witness against WORKFLOW-ID, streams through the committed prefix, and compares its digest before touching segment lengths. A damaged or missing committed prefix, a mismatched identity or a damaged witness stops recovery. The last valid witness bounds recovery: bytes past its committed end are discarded on open, even when they are complete frames. Only one successor segment can exist beyond that boundary. On rollover recovery the required segment range is narrowed durably before removing that successor. Tail truncation is synced. Repeating recovery after interruption gives the same committed prefix.

If the disk write or witness publication returns an uncertain error, the writer fences itself. If the process dies after witness publication and before the caller receives a reply, the group may already have committed. Use the existing workflow lost-reply contract.

## Using the candidate

Use workflow.Open on a **copy** when evaluating an existing store. Opening a valid pre-witness directory automatically adopts it under the directory lock after strict read-only scans of all retained segments. A damaged legacy tail fails validation. The upgrade changes journal.v1 and adds WORKFLOW-ID. Retain the full old directory for rollback to the old binary. Keep the witness, identity, RANGE and segments from the same stopped-store copy. A stale same-store witness from a different backup can identify an older valid prefix.

## Measurements

Five alternating paired runs on shared Linux AMD64 and ext4. Each cell completes 512 workflows with three sequential activities. Both versions use seven transition records per run. The candidate widens the writer group when commands are already waiting, while limiting encoded bytes and records. A separate three-round run with the original four-command cap is included to expose the cost of publishing a witness.

| Callers | Baseline runs/s | Candidate runs/s | Baseline p99 ms | Candidate p99 ms | Candidate groups/run |
|---:|---:|---:|---:|---:|---:|
| 1 | 354 | 112 | 5.61 | 15.84 | 7.000 |
| 8 | 1,168 | 447 | 14.86 | 38.28 | 1.750 |
| 32 | 1,264 | 1,624 | 30.68 | 26.21 | 0.439 |

Values are medians. The durability improvement carries a substantial cost when a group has few records. At 32 callers queued work shares the additional barriers. The short benchmark measures one fixture on this disk. A fresh process reopening 4,096 closed runs on warm filesystem cache had a retained heap delta of 3,736 KiB, versus 3,699 KiB at baseline (one observation each). Workflow history remains resident. Reopen hashes the committed bytes and replays the log.

## Architect verification

Run make dev-test-workflow-boundary and make dev-test-workflows. With DOST_WORKFLOW_BENCH_DIR set to a disposable directory on the target disk, run REPEATS=5 make dev-bench-workflow-boundary from the Git checkout. Review [raw throughput and latency](../benchmarks/workflow-boundary/paired-ext4.txt), [four-command comparison](../benchmarks/workflow-boundary/four-command-ext4.txt), and [fault receipts](../benchmarks/workflow-boundary/faults.txt).

Process-kill tests cover data sync before witness publication and publication before reply across rollover. Image tests cover short and full-length damaged tails on both sides of the boundary. Additional tests cover a missing uncommitted successor, a foreign witness, damage in a committed sealed segment, a damaged witness, byte-bounded group writes, and legacy adoption. Process death and constructed disk images were exercised; physical power interruption was not.

Keep this as a correctness candidate pending low-concurrency latency work and independent target-disk review. Disk-backed closed history, snapshots and stream-triggered starts are further milestones. This experiment establishes no Kafka or Temporal comparison.
