# Designing a delivery service on Dostoevsky

## Delivery workload

A durable delivery scheduler tracks which recipients are due, groups compatible work by destination, and records a separate outcome for each recipient. Keeping large bodies outside the queue bounds the bytes workers need to claim and retry a batch.

## Map the workload to Go

1. Accept and store the body once. Commit the envelope, recipients and an outbox entry together in the application's durable store. Acknowledge acceptance after that transaction. Publish compact body references from the outbox, using a stable ID per recipient and attempt.
2. Start with fixed lifecycle topics such as delivery-ready, delivery-attempted and delivery-outcome. Partition by a stable dispatch key. Tenant, route, destination and sending IP can travel as fields. Reserve independent topics for streams with independent replay/checkpoint requirements. Forcing all traffic for one large destination through one serial sender can become a separate bottleneck.
3. Use bounded worker pools, bounded bytes in flight, and a durable ready-time index. Claim eligible recipients with expiring leases and fencing tokens. Apply per-domain and per-source-IP concurrency limits. Avoid a goroutine or timer for every delayed recipient. Keep retry eligibility independent of queue head position.
4. Coalesce already-ready compatible recipients within count, byte and time limits. Send small urgent batches directly. Persist an exact outcome or next retry time, then advance the consumer checkpoint. A remote SMTP acceptance followed by a crash can result in ambiguous delivery; local producer deduplication alone cannot resolve it.
5. Independently observe accept-to-durable, due-to-claim, claim-to-SMTP-response and outbox lag. A completed append measures only one stage.

These steps describe the application architecture. This candidate implements the embedded log changes. SMTP, lease fencing, scheduling, sender limits and suppression remain application work.

## Cardinality and retention

One million topics per minute means 16,667 previously unseen names per second when every name is new. A full day is 1.44 billion names. One million events per minute on a stable topic set has a different memory profile, although its retained ID and record indexes still grow. Before allowing indefinitely expanding tenancy, implement journal segments, restart snapshots, an on-disk exact index, retention coordinated with durable consumer positions, and a documented producer-ID retry horizon. Topic retirement must define how offsets and producer IDs behave when a name is reused. Add tenant admission limits before exposing dynamic creation to untrusted clients.

Approximate telemetry is appropriate for distinct active routes per rolling interval (HyperLogLog) and heavy destinations (frequent-items sketch). Rotate bounded windows and merge sketches across owners. For sparse disk indexes, a Bloom filter can reduce negative lookups; verify positive hits against the exact index before suppressing a publish. Keep delivery state, retry decisions, quotas that must never be exceeded, and deduplication exact. Approximation cannot supply the offset or payload for a replay request. No sketch was added to the write path in this candidate.

## Capacity acceptance

Run the included harness with TMPDIR on the production data volume. Compare 100k versus 1M records, stable keys versus new topics, individual sync versus bounded batches, and zero versus two checkpointing groups. Use the group's workload with BURST_TOPICS=1. Vary the payload distribution and include retries, skewed destinations, a slow receiver, mixed reads and disk pressure. Measure p99 scheduled-to-ack, consumer visibility, RSS, disk growth and restart time. Repeat through several retention cycles and test forced loss of the process during an active write. Before claiming mail throughput, load-test a controlled SMTP sink separately with identical acceptance and reconciliation criteria.
