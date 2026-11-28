# Result: Rust write path against the Go engine (28 November 2026)

**Decision this supports: keep the core in Go.** The Rust prototype fails the pass rule on its
first criterion, throughput at 32 producers, in every trial.

Read `README.md` first for what is compared and what the Rust prototype leaves out.

## Where and how this ran

| | |
| --- | --- |
| Machine | KVM guest, 2 vCPU (Intel Xeon, 2.10 GHz), 8 GiB RAM, no swap. **x86-64, not the target: a 2 vCPU ARM64 server with under 4 GiB.** |
| OS | Ubuntu 24.04.5, Linux 6.18.44 |
| Filesystem | ext4 on a virtual disk (`rw,relatime,discard`); the device reports a write-back cache |
| Go | 1.24.7, default collector settings (`GOGC=100`, no memory limit) |
| Rust | 1.97.0, release build with link-time optimization, system allocator |
| Engine under test | Go: commit `f189e016`. Rust: `benchmarks/rustcore/dostoevsky-core` |
| Durability | Both acknowledge after `pwrite` and `fdatasync`. **Survival of a power loss was not tested here**; with one producer an acknowledged publish takes about 150 µs, so one sync costs at most that on this disk. |
| Load | Producers run inside the engine's process, closed loop. 100-byte value, empty key, empty ID, one topic with one partition. |
| Repeats | Throughput: 3 trials of 20 s per engine and producer count, engines alternated. Sustained: one 30-minute run per engine. Reopen: 10 runs per cell (3 for the oldest record). |

Raw output is in `receipts/`. `python3 report.py receipts` prints every table below.

## Conformance

Passed. Each engine opened a 300,000-record store written by the other and computed the same
digest over every record's offset, time, key and value. Each then appended to the other's
store under 8 producers, across rollovers, and the digests still agreed. After 9 bytes were cut
from the active segment, both engines truncated the same 129-byte frame and agreed again
(`receipts/conformance.txt`).

## Throughput and latency

| Producers | Engine | Acknowledged/s by trial | p50 (µs) | p99 (µs) | p99.9 (µs) | Max (ms) | Ready RSS (MiB) | Steady RSS (MiB) | Peak RSS (MiB) |
| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | go | 6,434, 6,536, 6,538 | 143 to 143 | 336 to 393 | 934 to 1212 | 4.4 to 7.3 | 6.4 to 6.5 | 11.4 to 11.6 | 11.5 to 11.8 |
| 1 | rust | 6,694, 6,810, 6,516 | 139 to 143 | 352 to 393 | 950 to 967 | 4.4 to 13.1 | 2.4 to 2.4 | 2.5 to 2.5 | 2.7 to 2.7 |
| 32 | go | 91,605, 92,492, 89,844 | 320 to 328 | 819 to 885 | 2228 to 2752 | 5.9 to 75.8 | 6.4 to 6.5 | 12.6 to 12.9 | 13.3 to 13.6 |
| 32 | rust | 69,841, 73,821, 66,989 | 393 to 434 | 999 to 1114 | 2359 to 2884 | 6.9 to 94.2 | 2.3 to 2.4 | 4.7 to 4.8 | 4.9 to 5.0 |

1 producer(s): Rust / Go throughput by trial: 1.04, 1.04, 1.00. Publishes over 10 ms by trial: Go [0, 0, 0], Rust [1, 0, 1].
32 producer(s): Rust / Go throughput by trial: 0.76, 0.80, 0.75. Publishes over 10 ms by trial: Go [64, 0, 32], Rust [14, 0, 97].

Rust records per sync at 32 producers: 28.7 to 28.8.

- **One producer: no difference established.** The ranges overlap (Go 6,434 to 6,538; Rust
  6,516 to 6,810). Every publish is one sync, and the sync is the cost.
- **32 producers: Go is faster in every trial.** Rust reached 75% to 80% of Go's throughput in
  the short runs and 70% over 30 minutes. Rust averaged 28.7 records per sync, so its groups
  were nearly full; it completed fewer groups per second.
- **Tail latency: no difference established.** p99.9 ranges overlap at both producer counts.

The likely reason for the 32-producer gap, which I did not isolate: the Rust prototype blocks
one OS thread per producer and wakes each one through the kernel after every group, on two
cores; the Go engine parks goroutines and wakes them in user space. A Rust server built on
asynchronous I/O would not pay this, and this prototype cannot tell you what it would achieve.

## Sustained load: 32 producers, 30 minutes

To keep the run inside the disk, a helper deleted all but the newest 64 sealed segments every
5 seconds for both engines (rule 5 in `README.md`).

| Measure | Go | Rust |
| --- | ---: | ---: |
| Seconds | 1800 | 1800 |
| Acknowledged records | 177,484,407 | 123,384,935 |
| Acknowledged per second | 98,602 | 68,547 |
| One-second throughput: lowest / median / highest | 64,272 / 99,183 / 116,605 | 43,792 / 68,644 / 90,180 |
| p50 (µs) | 295 | 418 |
| p99 (µs) | 786 | 1147 |
| p99.9 (µs) | 3146 | 3539 |
| Slowest publish (ms) | 97.0 | 99.3 |
| Publishes over 10 ms | 7,034 | 6,308 |
| Publishes over 50 ms | 800 | 416 |
| Publishes over 100 ms | 0 | 0 |
| Longest gap between acknowledgments (ms) | 96.9 | 98.4 |
| Gaps over 10 ms | 212 | 169 |
| Ready RSS (MiB) | 6.4 | 2.4 |
| RSS after 1 s (MiB) | 11.7 | 3.6 |
| Steady RSS, median of last 10 min (MiB) | 14.3 | 12.4 |
| RSS at the end (MiB) | 14.3 | 13.3 |
| Peak RSS (MiB) | 14.6 | 13.3 |
| Collector cycles | 39,507 | n/a |
| Collector pause, total (ms) | 2794.1 | n/a |
| Collector pause, longest of last 256 (µs) | 1196 | n/a |

Per million publishes over 10 ms: Go 39.63, Rust 51.12.

- **Pauses are not the collector's.** Go ran 39,507 collections with 2.8 s of total pause,
  0.16% of the run. Both engines show publishes over 10 ms at a similar rate (40 and 51 per
  million) and the same ceiling near 97 to 99 ms. Rust has no collector, so these stalls come
  from below the engines: the disk or the hypervisor.
- **The Rust memory advantage did not last.** Rust started at 2.4 MiB against Go's 6.4, but
  grew to 12.4 MiB by the last ten minutes against Go's 14.3. I did not isolate the cause. The
  candidates are the prototype's per-call buffers and wake-up slots, allocated from 33 threads
  with the system allocator. It is not a property of the on-disk format.
- **Throughput was steady for both.** The slowest second was 65% of the median for Go and 64%
  for Rust.

## Reopen

Each engine reopened a store it wrote. Cold means the page cache was dropped before the run.

| Records | Read | Cache | Runs | Go | Rust |
| ---: | --- | --- | ---: | --- | --- |
| 1,000,000 | newest record | cold | 10 | 6.0 (4.5 to 8.0) | 4.2 (3.6 to 6.2) |
| 1,000,000 | newest record | warm | 10 | 4.7 (3.4 to 5.3) | 2.1 (1.9 to 2.3) |
| 1,000,000 | oldest record | cold | 3 | 6.2 (5.2 to 6.5) | 41.3 (39.0 to 44.3) |
| 1,000,000 | oldest record | warm | 3 | 3.9 (3.8 to 3.9) | 21.8 (21.2 to 21.9) |
| 10,000,000 | newest record | cold | 10 | 11.4 (9.8 to 14.8) | 7.2 (6.2 to 9.7) |
| 10,000,000 | newest record | warm | 10 | 7.1 (6.7 to 9.1) | 5.0 (3.9 to 5.9) |
| 10,000,000 | oldest record | cold | 3 | 13.0 (11.9 to 14.8) | 65.3 (64.9 to 67.6) |
| 10,000,000 | oldest record | warm | 3 | 8.8 (8.7 to 10.5) | 32.5 (31.7 to 33.2) |

| Records | Measure | Go | Rust |
| ---: | --- | --- | --- |
| 1,000,000 | RSS after open and one read, warm (MiB) | 7.2 (7.2 to 7.3) | 3.3 (3.3 to 3.4) |
| 1,000,000 | First publish after open (µs), 3 runs | 261.8 (226.0 to 399.4) | 320.0 (206.4 to 535.5) |
| 10,000,000 | RSS after open and one read, warm (MiB) | 7.4 (7.4 to 7.5) | 3.4 (3.4 to 3.4) |
| 10,000,000 | First publish after open (µs), 3 runs | 463.9 (443.6 to 465.3) | 401.1 (281.3 to 435.4) |

Filling the stores (batches of 1,000 records, one sync per batch), records per second: go 1,000,000: 773,458; rust 1,000,000: 1,231,279; go 10,000,000: 708,263; rust 10,000,000: 1,187,690.

- **Reading the newest record after open: Rust is faster,** 7.2 ms against 11.4 ms cold at 10M
  records, and it holds 3.4 MiB against 7.4 MiB afterwards.
- **Reading the oldest record after open: Go is 3.7 to 6.7 times faster,** because it
  reads its index sidecar files. The Rust prototype writes none and rebuilds each index by
  scanning a segment.
- Go's cold reopen here, 9.8 to 14.8 ms at 10M records, is in line with the 8.6 to 12.4 ms the
  project measured earlier on ARM64.

## Sequential read

| Engine | Cache | Records/s | MiB/s of payload | Peak RSS (MiB) |
| --- | --- | --- | --- | --- |
| go | cold (1 run) | 2,093,084 to 2,093,084 | 200 to 200 | 13.6 to 13.6 |
| go | warm (3 runs) | 2,428,167 to 2,520,817 | 232 to 240 | 30.3 to 39.1 |
| rust | cold (1 run) | 5,652,950 to 5,652,950 | 539 to 539 | 5.0 to 5.0 |
| rust | warm (3 runs) | 8,350,274 to 9,591,737 | 796 to 915 | 5.0 to 5.0 |

Rust reads 3.3 to 3.9 times faster warm. The Go engine builds one `Record` per row and copies
each value. This is the strongest finding to take back into the Go engine: a columnar fetch
(one buffer per column) removes that per-record
work without changing language.

## Against the pass rule

| # | Criterion | Result |
| --- | --- | --- |
| 1 | Throughput at least Go's at 1 and 32 producers | **Fails.** Equal at 1 producer; 0.75 to 0.80 of Go at 32 producers in every trial, 0.70 over 30 minutes |
| 2 | p99.9 at most 0.7 of Go's, or steady memory at most 0.6 of Go's | **Fails on the 30-minute run** (p99.9 ratio 1.12; memory ratio 0.87). The 20-second runs met the memory half (0.22 and 0.37) |
| 3 | No pause above 10 ms in 30 minutes | **Fails.** 6,308 publishes over 10 ms. Go had 7,034; neither is the collector |
| 4 | Reopen at 10M no slower than Go | Met for the newest record, cold and warm. Not met for the oldest record |
| 5 | Conformance | **Met** |

## What the expectation got right and wrong

The expectation before the runs was that sync dominates, so throughput barely differs, and that Rust's gain, if
any, shows in tail latency and memory.

- **Right at one producer:** throughput is the sync rate in both.
- **Wrong at 32 producers:** with group commit the sync is shared by about 29 records, and what
  is left is the cost of queueing and waking callers. There the Go runtime was ahead.
- **Tail latency:** no Rust gain seen. The tail on this machine is set by storage stalls.
- **Memory:** a Rust gain at rest (2.4 against 6.4 MiB ready), most of it gone under sustained
  load in this prototype.

## What to take from the prototype without changing language

1. **Columnar fetch.** The read gap above.
2. **Per-record cost in `PublishBatch`.** Filling the stores in batches of 1,000 ran at 0.71 to
   0.77 million records per second in Go against 1.19 to 1.23 million in Rust. Some of that is
   work the prototype skips (ID handling); a profile would say how much is not.
3. **The ready footprint.** 6.4 MiB in Go against 2.4 MiB is mostly the runtime's floor. It
   bounds how far an aim of the smallest footprint at readiness can go
   in Go.

## Limits

- Not the target machine: x86-64 with 8 GiB, not ARM64 with under 4 GiB.
- Producers are in-process. A network server adds work this does not measure.
- The Rust side is a feature-light prototype with blocking threads. It flatters Rust on memory
  and per-record CPU and penalizes it at 32 producers.
- One 30-minute run per engine; the short runs have three trials.
- Go 1.24.7. Go 1.26 changed the default collector; the
  collector was 0.16% of this run, so that is unlikely to move the result.
- The crate was moved from `rust/` to `benchmarks/rustcore/` after the timed runs, so that the
  repository's release-content test passes. The source did not change; conformance was rerun
  from the new path (`receipts/conformance-after-move.txt`).
