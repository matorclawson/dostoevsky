# Sealed index admission and hot topics

Use the default FIFO policy for general publishing. Frequency admission is an opt-in for segmented nodes whose clients frequently retry historical IDs amid scans. It charges 1,152 bytes against the existing 8 MiB ID-table budget. Both policies also limit the cache to 32 indexes. Index metadata, transient decodes and filesystem cache are outside the ID-table budget. The candidate bypasses an oversized table. Repeated lookups in such a segment can be expensive.

Call `node.EnableIndexAdmission()` after opening a segmented node. It returns false for the legacy journal. Duplicate verification uses the already decoded index and still reads the original record. Membership, offsets and receipts stay exact.

## Local experiment

Linux AMD64, Go 1.24.4, GOMAXPROCS=4, shared host, ext4 on /dev/vdb. Each cell is a fresh process using a copy of a seeded directory with 96 sealed segments of 64 identified messages and one active segment. Per cell we submitted 4,096 publishes through Node.Publish and checked each returned ID and every historical offset. Mixed retries cycle between 80 requests to eight hot segments and a scan of 48 cold segments. Shift changes all eight hot segments halfway through. Uniform chooses among all 96. Append submits fresh identified messages. Each policy got the same deterministic inputs; scheduling with 32 goroutines varies. Membership hints were prepared before timing. This exercises cache capacity by segment count; it does not exercise the 8 MiB byte ceiling, cold disk reads or boot RSS. All RSS values are process high-water marks and include startup.

The published comparison uses medians of five runs. Original is commit 1745c8a. FIFO is the revised receipt path with the original eviction policy. Frequency adds admission. Five short runs characterize this fixture and do not establish statistical equivalence on other hosts.

| Trace | Producers | Policy | Acks/s | p99 µs | Index loads/1,000 publishes | Peak MiB |
|---|---:|---|---:|---:|---:|---:|
| mixed | 1 | original | 93,134 | 47.5 | 441.4 | 14.29 |
| mixed | 1 | fifo | 91,846 | 53.8 | 441.4 | 14.36 |
| mixed | 1 | frequency | 102,610 | 52.5 | 320.1 | 14.39 |
| mixed | 32 | original | 75,693 | 1,149.3 | 448.2 | 14.70 |
| mixed | 32 | fifo | 73,989 | 1,098.9 | 456.8 | 14.81 |
| mixed | 32 | frequency | 82,692 | 895.8 | 319.6 | 14.64 |
| shift | 1 | original | 92,594 | 52.4 | 440.9 | 14.54 |
| shift | 1 | fifo | 93,417 | 52.1 | 440.9 | 14.31 |
| shift | 1 | frequency | 98,458 | 62.8 | 335.2 | 14.34 |
| shift | 32 | original | 73,969 | 1,287.3 | 442.6 | 14.75 |
| shift | 32 | fifo | 77,199 | 1,117.8 | 454.8 | 14.80 |
| shift | 32 | frequency | 85,634 | 1,003.5 | 339.4 | 14.76 |
| uniform | 1 | original | 75,741 | 69.8 | 674.8 | 14.50 |
| uniform | 1 | fifo | 78,410 | 72.8 | 674.8 | 14.34 |
| uniform | 1 | frequency | 77,112 | 65.8 | 683.1 | 14.39 |
| uniform | 32 | original | 65,841 | 1,169.7 | 675.8 | 14.76 |
| uniform | 32 | fifo | 69,388 | 1,235.3 | 675.3 | 14.81 |
| uniform | 32 | frequency | 67,145 | 1,014.0 | 680.2 | 14.75 |
| append | 1 | original | 2,522 | 851.3 | 12.9 | 11.28 |
| append | 1 | fifo | 2,442 | 956.7 | 12.9 | 11.31 |
| append | 1 | frequency | 2,435 | 883.2 | 13.2 | 11.33 |
| append | 32 | original | 50,656 | 1,381.2 | 12.9 | 13.30 |
| append | 32 | fifo | 49,967 | 1,344.4 | 12.9 | 13.24 |
| append | 32 | frequency | 48,864 | 1,299.9 | 12.9 | 13.74 |

Run `TMPDIR=/absolute/path/on/test-volume REPEATS=5 make dev-bench-admission` from a Git checkout containing the baseline. For a source ZIP, set BASELINE_DIR to an extracted copy of commit 1745c8a or clone the included Git bundle. The driver prints CELL, policy, workload, producers, acknowledged publishes/s, p99 microseconds, index loads per thousand publishes and peak RSS in KiB. ext4-amd64.txt contains raw results. linux-amd64.txt contains a separate tmpfs comparison. initial-double-lookup.txt records the discarded prototype on tmpfs.

## Optional hot-topic analytics

Create a tracker in a consumer, observe each topic with the event timestamp and query after the latest observation. The half-life controls the decay of activity. Four residents and four probation fingerprints share each bucket. Oversized names return false. A long tail or many equally active names mapping to one bucket can reduce recall. Replay and consumer redelivery both contribute observations. The tracker is process-local and callers synchronize access.

```go
hot, err := sketch.NewHotTopics(128, 24, time.Minute)
if err != nil { return err }
// For each consumed record:
hot.Observe(record.Topic, record.Time)
// When the dashboard requests a ranking:
leaders := hot.Top(10, time.Now())
```

Run `make dev-bench-hot-topics` for accuracy against an exact-key oracle, a million-distinct-name churn trace, construction memory and observation costs. hot-topics.txt contains the raw runs. The million-name test uses simulated event timestamps. It exercises analytics independently of the durable catalog. The fixed resident allocation excludes caller memory and query results. Instance-local randomized hashing can produce different rankings across runs.

The full regular and race suites each encountered three socket-listener tests blocked by host policy. The remaining suite ran under the race detector. The final capacity preflight also has a regression test covering an incoming table that would need several evictions. The timed fixture stays below that byte-pressure branch.
