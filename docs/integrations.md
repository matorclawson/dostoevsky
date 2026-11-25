# Integrations

## An HTTP service

A handler can publish to an embedded node directly. The response means the event is durable, and a stable, non-empty `Idempotency-Key` header becomes the message ID so a client retry cannot duplicate it (without the header there is no ID and no deduplication) (copied from [`getting_started_test.go`](../getting_started_test.go), which runs it):

<!-- snippet:http -->
```go
// An HTTP handler that publishes the request body. The response means the event is durable.
handler := func(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	rec, err := node.Publish(r.Context(), "orders", dostoevsky.Message{ID: r.Header.Get("Idempotency-Key"), Value: body})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "offset %d\n", rec.Offset)
}
```

Close the node on shutdown and give each directory exactly one owner process. Read the log from another goroutine with `Fold`, `ReadBatch` or `Read`, and wake waiting readers with `Notifications` instead of polling ([how it works](how-it-works.md#notifications)).

## Several nodes: the mesh

Run the same library in each application and mount its peer handler on your own HTTP server. Every node shares the ordered membership list and partition counts. For explicitly declared topics a partition's owner is its partition number modulo the node count; wildcard topics add a hash of the topic name to that placement. `Mesh` routes `Publish`, `Read`, `Position` and `Commit` to the owner; peer requests carry a configuration fingerprint and a shared bearer token. Run `make dev-mesh-a` and `make dev-mesh-b` in two terminals for a working pair ([`examples/mesh`](../examples/mesh/main.go)). Use HTTPS between machines. Each partition still has one durable copy: an offline owner makes its partitions unavailable, and there is no automatic failover.

## A durable task queue

[`examples/taskqueue`](../examples/taskqueue/main.go) is a producer, one worker goroutine per partition, retries with a bounded attempt count, and a dead-letter topic, with a restart test (`go test ./examples/taskqueue`). The job ID routes every retry to the same partition, and stable message IDs deduplicate repeated publications. A worker publishes the result, retry or dead letter first and then acknowledges the job by committing its offset, so after an interruption a handler may run again: use an idempotency key at any external destination. It has no worker leases or scheduled delivery. The [README section](../README.md#use-a-topic-as-a-background-task-queue) walks through it with `make` targets.

## Coming from NATS JetStream

| In JetStream | In Dostoevsky |
|---|---|
| Stream | A topic with a fixed number of partitions |
| Subject, subject wildcards | A topic name. A wildcard manifest creates topics on first use, but there is no subject hierarchy matching |
| Durable consumer, ack position | Consumer group and its checkpoint: `Position` reads it, `Commit` advances it |
| Pull consume | `Read` (optionally waiting) and `ReadBatch` |
| `Nats-Msg-Id` deduplication | `Message.ID`, exact within a partition until retention removes the record |
| Ack wait, redelivery, max deliveries | Not built in. A group re-reads from its last committed cursor; the task queue example shows attempt counting and dead letters |
| Replicas, clustering, leaf nodes | Not provided. One durable copy per partition |
| Key-value and object stores | Not provided |
| Server process and client protocol | A Go library in your process; peers talk HTTP |

**Choose NATS** when several languages or services must reach one broker over the network, when you need replication and failover, subject-based routing, or its key-value and object stores. **Choose Dostoevsky** when one Go application, or a small fixed mesh of them, needs a durable ordered log with exactly-once state in its consumers, no server to run and a small memory footprint. The [publisher gate](publisher-gate.md) compares the two on one workload, including where JetStream is faster.

## How the systems differ

This table is a summary, not a benchmark. Check each system's documentation for your version.

| | Dostoevsky | NATS JetStream | Apache Kafka | Redis Streams | SQLite as a queue |
|---|---|---|---|---|---|
| Runs as | Library in your Go process | Server (also embeddable in Go) | Broker cluster | Server | Library (a database file, plus WAL files in WAL mode) |
| Language clients | Go | Many | Many | Many | Many |
| Replication and failover | No | Yes (Raft) | Yes in a cluster (a single broker also runs) | Primary and replicas, asynchronous by default | No |
| Durability | Journal synced before acknowledgment | Configurable file sync | Replication and configurable flush | Optional append-only file (off in the default configuration) with a configurable sync policy | Write-ahead log: WAL mode with `synchronous=FULL` syncs every commit; other settings trade that away |
| Consumer groups | Yes, with state committed with the cursor | Durable consumers | Yes | Yes | You build them with tables |
| Integrated stream processing | `Fold`, `Pipe` | Consumers keep their own state | Kafka Streams library | No: scripts and data structures, not a processing engine | No: SQL over tables |
| Where it is the better choice | Embedded, low memory, one process | Networked messaging, many clients | High-volume, replicated logs, a large ecosystem | Fast in-memory structures you already run | Transactions with your other tables |

Sources: [JetStream](https://docs.nats.io/nats-concepts/jetstream); [Kafka](https://kafka.apache.org/documentation/) and its [single-broker quickstart](https://kafka.apache.org/quickstart); [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/), [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) and the [default `redis.conf` (`appendonly no`, `appendfsync`)](https://raw.githubusercontent.com/redis/redis/8.12.0/redis.conf); [SQLite WAL](https://sqlite.org/wal.html) and [`PRAGMA synchronous`](https://www.sqlite.org/pragma.html#pragma_synchronous).
