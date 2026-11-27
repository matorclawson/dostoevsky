# Fresh durable publishing on segmented storage

This experiment compares the candidate with 8e410b7, keeping default FIFO admission. Both use an embedded node, synchronous Publish calls with fresh producer IDs, the same segmented frame format, and a data sync before each group is acknowledged. The single-producer case performs one data sync per record. A group can contain several independent producers' messages under contention.

## Reproduce

From the supplied Git history, use a Linux host with Go 1.22 or later and set TMPDIR to an existing directory on the test volume. The candidate was measured on shared Linux AMD64, Go 1.24.4, GOMAXPROCS=4, ext4 on /dev/vdb.

```sh
TMPDIR=/path/on/test-volume REPEATS=7 make dev-bench-fresh
```

The driver builds both revisions, seeds 96 sealed segments and one active segment with 64 IDs each, and reopens a fresh copy for every cell. It prepares membership hints and constructs payloads before timing. Each cell submits 4,096 unique IDs, checks their receipts and checks that the resulting offsets form the expected contiguous range. Seven paired trials alternate build order. The zero-byte workload measures an identified message with an empty value. One KiB represents application content. Acks/s includes waiting for durable completion. The driver emits trial, build, CELL, producers, payload bytes, acks/s, p95 and p99 in microseconds, allocations and allocated bytes per record, process peak RSS in KiB, and number of publish groups. RSS includes prebuilt inputs. Allocated bytes count total heap allocation traffic during the measurement.

For a source-only directory, set BASELINE_DIR to a checkout of 8e410b7. The included bundle can be cloned with `git clone -b council/fresh-publishing dostoevsky-fresh.bundle checkout`.

## Storage and correctness checks

- The writer retains up to 64 KiB of encoding scratch after use, reserves space for each complete record frame and allocates batch bookkeeping when it encounters a fresh record. Larger encoded batches are transient. The regression test checks the retained size and an earlier receipt and persisted value after reuse.
- The stage probe times WriteAt and SyncData directly on the active segment. A separate Linux primitive comparison measures O_DSYNC and WriteAt plus fdatasync on the same volume. It covers 1,024 sequential 1,072-byte writes per trial. It does not measure the broker, and the candidate uses the existing sync primitive.
- The segmented process-kill test records the first 256 receipts per run with one and 32 individual producers, kills the writer, then reads the messages and retries their IDs at the original offsets. The larger batch crash test checks more than one segment of data. See crash.txt. These tests exercise process death with an intact OS.
- The three Unix listener tests failed at socket creation with EPERM in this environment. See socket-tests.txt. race.txt includes the remaining suite.

## Median of seven runs per cell

| Producers | Bytes | Build | Acks/s | p95 µs | p99 µs | Allocations/record | Allocated B/record | Peak MiB | Groups |
|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | baseline | 2,313 | 640 | 1,321 | 8.37 | 433.4 | 11.88 | 4096 |
| 1 | 0 | candidate | 2,209 | 688 | 1,406 | 4.37 | 297.4 | 11.06 | 4096 |
| 1 | 1024 | baseline | 2,170 | 685 | 1,430 | 9.38 | 1,586.1 | 19.19 | 4096 |
| 1 | 1024 | candidate | 2,258 | 641 | 1,183 | 4.37 | 297.9 | 15.09 | 4096 |
| 32 | 0 | baseline | 46,580 | 956 | 1,795 | 3.43 | 1,129.0 | 14.02 | 132 |
| 32 | 0 | candidate | 45,293 | 984 | 1,742 | 2.89 | 855.6 | 13.66 | 131 |
| 32 | 1024 | baseline | 35,614 | 1,306 | 2,238 | 4.60 | 6,659.8 | 20.79 | 133 |
| 32 | 1024 | candidate | 42,026 | 1,041 | 1,902 | 3.89 | 1,914.4 | 19.67 | 132 |

With 32 producers and 1 KiB values, the candidate beat its paired baseline in six of seven trials, allocated 71% fewer heap bytes per record and lowered median p99 by 15%. With one producer the same payload beat baseline in four pairs. The zero-byte workloads had lower median throughput at both concurrency levels. The 95–97% of single-producer elapsed time spent in fdatasync leaves little room for a serialization change to raise isolated publish throughput on this disk. The synchronous-write primitive comparison had mixed pairwise results.

Use `make dev-test-sockets` on a Linux host that permits local Unix listeners. Use `DOST_FRESH_PROFILE_DIR=/path/to/disposable/fixture make dev-profile-fresh` with a fresh fixture copy for each run, and `DOST_SYNC_MODE_DIR=/path/on/test-volume make dev-test-sync-modes` for the primitive experiment.
