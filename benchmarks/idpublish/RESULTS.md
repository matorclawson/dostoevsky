# Identified publishing results · 27 November 2026

## Decision

Keep segmented storage opt-in. The ten-million-ID scan was removed from the ordinary publish path. At one million IDs segmented led the legacy engine in both producer counts. At ten million it retained most of the legacy throughput with substantially lower RSS. The stated adoption goal required wins at both producer counts.

## Embedded comparison

Median of three fresh-process trials per cell on shared Linux AMD64 and ext4, GOMAXPROCS=2. Each trial timed 2,048 new eight-byte messages with unique IDs after one separately measured first publish. The same seeded dataset was used in both formats. Writers sent one message per call. A single writer synced immediately and overlapping producers could share a sync. The runner alternated engine order. RSS is sampled after the timed phase. Each row lists independent medians.

| Retained IDs | Producers | Engine | Ack/s | p50 ms | p95 ms | p99 ms | RSS MiB |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1000000 | 1 | segmented | 2,378 | 0.387 | 0.610 | 0.875 | 15.8 |
| 1000000 | 1 | legacy | 2,325 | 0.394 | 0.626 | 0.844 | 65.0 |
| 1000000 | 32 | segmented | 52,617 | 0.565 | 0.774 | 1.318 | 17.1 |
| 1000000 | 32 | legacy | 51,014 | 0.603 | 0.820 | 1.504 | 65.8 |
| 10000000 | 1 | segmented | 2,074 | 0.407 | 0.814 | 2.067 | 60.5 |
| 10000000 | 1 | legacy | 2,098 | 0.398 | 0.802 | 1.716 | 446.1 |
| 10000000 | 32 | segmented | 28,822 | 0.961 | 2.033 | 2.556 | 52.1 |
| 10000000 | 32 | legacy | 29,854 | 0.940 | 1.986 | 2.941 | 459.9 |

## What the filter bought

The prior segmented engine at ten million IDs reloaded 94 exact indexes for each new ID. At one producer it sustained 2.1 ack/s, with a 475 ms median and 558 ms p99. At 32 producers it sustained 1.9 ack/s with a 16.55 s p99. Each of these slow baseline cells ran 32 measured operations. The first old-format first-use upgrade took 1.91 seconds to construct hints for ten million IDs in this experiment. Offline preparation is available. A regular first publish after reopening with hints present still incurs cold filter reads.

Below are cold first-call latency, steady p99 and exact probes per call for three distinct operations at ten million IDs. The recent and oldest IDs refer to the final and first records of the seeded history. Retry trials repeatedly request that same record. These are hot-repeat results after the first sample. The source receipts for this filter-width experiment predate the small ordered-filter traversal optimization used in the paired comparison.

| Producers | Operation | First call ms | Warm p99 ms | Exact segment probes per operation |
|---:|---|---:|---:|---:|
| 1 | new | 64.03 | 6.090 | 0.00000 |
| 1 | recent | 10.17 | 0.029 | 1.00000 |
| 1 | oldest | 56.63 | 0.095 | 1.00000 |
| 32 | new | 25.20 | 2.341 | 0.00000 |
| 32 | recent | 7.22 | 0.573 | 1.00000 |
| 32 | oldest | 38.78 | 1.517 | 1.00000 |

The exploratory 24-bit-per-ID filter generated occasional costly exact-index loads at ten million IDs. With 32 producers its median new-publish p99 was 13.14 ms across three development trials. The 32-bit variant produced 2.34 ms in its corresponding trials, trading ten million additional bytes of nominal filter space for ten million IDs. These development runs had other host activity. Warm operation and migration should be benchmarked separately for an installation.

A 20,000-message burst with 512-byte payloads crossed one rollover at one producer and two at 32 producers. The first receipts in the new segments took **17.61 ms**, **10.27 ms** and **8.25 ms**, respectively. Rollover still writes checkpoint and derived index data under the writer lock. Smaller tail overhead at segment boundaries remains an opportunity.

## Broker and client measurements

Medians of three trials. Each used new single-node storage on ext4, loopback TCP, 512-byte values, unique 32-character application IDs, 64 warm-up requests per cell and 2,048 timed individual acknowledgements. Concurrency was outstanding calls. Python used native Dostoevsky, nats-py 2.16.0 and confluent-kafka 2.16.1. The warm-up created a pool of 32 native connections, including for the single-producer cell. NATS Server 2.15.3 used JetStream file storage, one replica and sync always. Kafka 4.4.0 used KRaft, one partition, one replica, all acknowledgements and flush interval one message. Its idempotent producer used the application ID as a Kafka key. Kafka producer-session semantics, JetStream's duplicate window and Dostoevsky's retained-ID lifetime differ.

| Broker | Producers | Ack/s | p50 ms | p95 ms | p99 ms | RSS at ready MiB | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| native | 1 | 1,310 | 0.683 | 1.150 | 3.020 | 6.6 | 11.2 |
| native | 32 | 6,486 | 3.895 | 10.164 | 13.976 | 6.6 | 12.2 |
| nats | 1 | 1,022 | 0.843 | 1.650 | 4.062 | 15.3 | 23.5 |
| nats | 32 | 1,743 | 15.777 | 33.543 | 80.247 | 15.3 | 27.6 |
| kafka | 1 | 620 | 1.350 | 3.074 | 5.768 | 375.2 | 403.3 |
| kafka | 32 | 790 | 36.996 | 59.278 | 70.224 | 375.2 | 404.0 |

The Python NATS adapter bridges worker threads to an asyncio loop. A second comparison used native JavaScript event-loop clients for both Dostoevsky and JetStream, with readback of every timed receipt.

| Broker | Producers | Ack/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| native | 1 | 1,359 | 0.651 | 1.028 | 2.668 |
| native | 32 | 22,285 | 1.292 | 2.195 | 4.304 |
| nats | 1 | 1,279 | 0.710 | 1.142 | 2.663 |
| nats | 32 | 1,874 | 16.069 | 24.060 | 26.277 |

Both broker comparisons used short histories. The ten-million-ID table applies to the embedded Go engines. Broker RSS measures the server process and excludes client memory and kernel page cache. These runs are evidence for the tested hardware, client and durability configurations. Three trials on shared hardware do not establish a universal ranking or a hardware-independent latency budget.

## Crash verification

Every timed Python trial restarted the broker on its existing directory after SIGKILL and verified all 4,224 acknowledged warm-up and measured records. Each broker recovered 12,672 externally recorded receipts across the three trials. The in-flight follow-up killed each broker with eight requests outstanding. The observed successes were checked by offset and payload after restart. Unanswered requests were excluded from the success count.

| Broker | Outstanding at kill | Total acknowledged and verified |
|---|---:|---:|
| native | 8 | 898 |
| nats | 8 | 902 |
| kafka | 8 | 896 |

On a separate segmented fixture starting at offset 10,070,810, all **20,480 acknowledged records** survived SIGKILL. Retrying each message returned the original offset. A separate native eight-producer test checked **20,007** receipts and exact retries. Process termination tests keep the OS and filesystem running. Power loss, target ARM64 hardware, long-term soak and histories exceeding the filter cache budget remain unmeasured.

## Verification and handoff

Full offline Go tests encountered the host's denial of Unix socket creation in three named listener tests. The race suite passed with those three tests excluded. The storage regressions cover misplaced, missing and corrupt filters, false positives, retention, exact conflict handling and warming an old store. See the receipts directory for raw rows. A first peer harness attempt raced broker shutdown and failed to connect on restart. The completed trials waited for the killed process to exit and the new listener to advertise readiness.
