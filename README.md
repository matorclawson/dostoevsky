# Dostoevsky

Dostoevsky is an embeddable Go library for durable topics and stateful stream processing. Start with a directory and a topic manifest. To distribute partitions, embed the same library in each participating application and mount its peer handler on the application's HTTP server. All runtime dependencies come from the Go standard library. MIT licensed.

## Install

```sh
go get github.com/rodriveracom/dostoevsky@latest
```

Requires Go 1.22 or later. Import it as `github.com/rodriveracom/dostoevsky`. The module has no dependencies outside the Go standard library.

## Run a persistent word count

Use Go 1.22 or later on Linux and clone the repository. From the module directory:

```sh
make dev-wordcount WORDS="blue red blue"
# {"blue":2,"red":1}
make dev-wordcount WORDS=red
# {"blue":2,"red":2}
make dev-wordcount
# {"blue":2,"red":2}
```

Each argument creates one event. The example commits the counts and the input offset together. Reopening the directory resumes at the next offset. To replay historical messages, read from offset zero or choose a fresh consumer group.

## Streaming concepts

A **producer** publishes a record to a named **topic**. Its key selects a **partition**, where each record receives an ordered **offset**. A **consumer group** maintains its own **checkpoint** for each partition. The checkpoint stores the cursor for the next read and optional processor state. Two groups can independently replay the same records.

A **subscriber** waits for a change associated with a notification key. Applications choose the key type. For example, use a partition to wake readers after an append, or an account ID to wake readers after a database transaction commits. Multiple subscribers can wait on the same key.

## Use it in an application

```go
node, err := dostoevsky.Open(dir, dostoevsky.Config{
    Node: "app", Nodes: []string{"app"},
    Topics: map[string]int{"orders": 4},
})
if err != nil { return err }
defer node.Close()

record, err := node.Publish(ctx, "orders", dostoevsky.Message{
    Key: "account-42", ID: "order-918", Value: []byte("paid"),
})
if err != nil { return err }
// record.Partition and record.Offset identify this event.
return dostoevsky.Fold(ctx, node, record.Partition, "paid-count", 0,
    func(_ context.Context, count int, event dostoevsky.Record) (int, error) {
        if string(event.Value) == "paid" { count++ }
        return count, nil
    })
```

Fold stays up and waits for more events until its context is canceled. This snippet handles one partition. To consume a whole topic, start one Fold per partition and assign the workers explicitly. Use a fresh initial value per worker. Reducers can return an error to stop processing with the current input still pending.

| Operation | Contract |
| --- | --- |
| Publish | FNV-1a hashes the UTF-8 key into a fixed partition. The empty key always selects the same partition. Each partition assigns contiguous zero-based offsets. |
| Message.ID | Optional retry identity scoped to a topic partition. An identical retry returns the original offset and timestamp. Conflicting content under an existing ID returns ErrConflict. |
| Read | Fetch any offset directly, tail with a context, or request an immediate result. An unavailable offset yields ErrEmpty in immediate mode. |
| Position / Commit | Each group has an independent cursor and state per partition. Commit compares the expected offset, advances it by one and stores the state in one journal entry. |
| Fold | Applies an ordinary Go reducer. State uses JSON and must fit within 1 MiB. A competing commit returns ErrConflict. |
| Pipe | Transforms one input to one output, then commits the input. It derives a stable output ID from the input coordinates and group, enabling retry across different partition owners. Reserve the ID prefix `pipe/` for this operation. |

A Pipe output can become visible before its input checkpoint advances. Reuse the same destination, group and deterministic transform when restarting. Use separate groups for separate projections. External side effects, including calls made inside reducers, can repeat after failures.

## Run two application processes

In two terminals, from the module directory, run one command in each:

```sh
make dev-mesh-a
make dev-mesh-b
```

After both print “ready”, type `b` on node a and `a` on node b. Each reports the other node as the owner at offset 0 on a fresh directory. Stop node b and node a will report a delivery error for key `b`. Restart b with the same directory to resume. The example binds loopback ports 8711 and 8712.

All nodes share an ordered membership list and topic partition counts. Partition number modulo node count selects the owner. Peer requests carry a fingerprint of this configuration and the intended node ID. Add topics by updating the manifest and reopening the nodes together. Existing node order, node IDs and partition counts are persistent. Applications control addresses, lifecycle, deadlines and TLS via net/http. Use HTTPS across machines and a unique random token per deployment. The shared token gives peers access to all topics.

## Durability and operating limits

* Each owner holds one checksummed append-only journal on a local filesystem with exclusive advisory locking. Acknowledgments follow file sync. Recovery verifies complete frames and truncates a partial final frame. A checksum failure stops recovery.
* Each partition retains an in-memory index of record positions and optional producer IDs. Payloads stay on disk. Startup scans the journal. Each commit stores the full group state. This first version retains history indefinitely and serializes storage operations per node. Plan for disk growth, replay time and index memory.
* Values and processor state are limited to 1 MiB each, keys to 4096 bytes and producer IDs to 256 bytes. Topic and group names use letters, digits, hyphens and underscores, up to 96 characters.
* Each partition has **one durable copy**. An offline owner makes its partitions unavailable. Disk loss requires restoring that owner's backup. For a consistent backup, close the node and copy its directory. Never start two live owners with the same identity and different directories.
* A lost publish response leaves the outcome uncertain. Retry with the same key, ID and payload. After an uncertain commit, read Position before deciding whether to retry. Cancellation interrupts waiting readers. A disk write already in progress may complete.
* Membership is static. This version has no quorum replication, automatic failover, retention policy, dynamic group balancing, or event-time window engine. Placement is per partition. There is no global event order.

## Design lineage and verification

Faust's supplied word-count example motivated keyed streams and recoverable processor state. Kafka's partition logs, replay offsets and independent consumer groups inspired the delivery model. This repository implements those ideas in original Go and includes two executable examples.

```sh
CGO_ENABLED=0 GOPROXY=off go test ./...
go test -race ./...
go vet ./...
```

Tests exercise recovery after process exit, incomplete frames, checksum failures, idempotent publish, concurrent commits, independent groups, cancellation, pipeline retry and peer routing. The race detector is a development tool and requires a C toolchain on Linux. Applications build with CGO_ENABLED=0.

## Implement a database-backed reader

Use one Notifications instance per process. Subscribe to the notification key before querying the database. If a page exists, release immediately. Otherwise wait on the captured channel or context, release on either path, then subscribe and query again. Call Signal only after the SQLite transaction succeeds. The subscription cap counts readers across all keys. Handle ErrLimit explicitly. Signals are process-local hints, so a writer in a different process requires a separate wake mechanism or bounded rechecks.

Implement BatchReader on your storage adapter and pass it to ConsumePages. Use an indexed keyset query ordered by the stream cursor, with a record limit and a byte budget for the returned page. Page.Next is the adapter's next resume position and may advance across gaps. An HTTP adapter can encode this cursor for its clients. When the first available item exceeds the byte limit, return ErrLimit. The Node implementation counts key, ID and value bytes before wire encoding. Node.ReadBatch waits only for the first record. Mesh retains its existing single-record wire protocol.

For a SQLite projection, load its durable cursor, then supply an apply callback. Begin a SQLite transaction, write the projection, and update the cursor with a predicate matching its previous value. Require exactly one affected cursor row (initialize it when registering the worker). Roll back on conflict or any write error. Commit before returning success. Keep external side effects outside this transaction or use an outbox. Node.CommitBatch offers the equivalent cursor comparison for journal-backed state. Multi-record checkpoints require the 0.2 reader on reopen.

### Benchmarking batch reads

Compare single-record and batch readers using the same dataset, storage volume and arrival trace. Run one consumer with a backlog and another waiting at the end of its partition. Measure backlog drain time and p95/p99 commit-to-visible latency for the waiting consumer separately. Record queries per thousand deliveries, CPU time and peak memory. After a forced restart, reconcile acknowledged records and committed effects against the producer ledger.

Use `make dev-bench-notify` to measure repeated signals on a key with eight already-notified subscribers.

## Development and deployment

Run `make help` for the command list. The Makefile targets Linux with GNU Make and coreutils. Go 1.22+ and gofmt must be on PATH. The race check also requires a C compiler.

| Workflow | Command | Result |
| --- | --- | --- |
| Development | `make dev` | Checks formatting, vets, tests, runs the race detector and builds examples in `.dev/bin/` |
| Local demos | `make dev-wordcount WORDS='blue red blue'` or `make dev-mesh-a` and `make dev-mesh-b` | Keeps demo journals in `.dev/data/` |
| Release the library | `make release` | Writes a versioned source tarball and SHA-256 file to `dist/` after testing an extracted copy |
| Verify a release | `make release-check` | Checks the checksum and reruns offline tests from the archive |
| Build for deployment | `make deploy-build APP_DIR=../orders APP_PACKAGE=./cmd/orders APP_NAME=orders ARCH=arm64` | Builds a pure-Go Linux executable in `build/deploy/linux-arm64/` |
| Install | `make deploy APP_DIR=../orders APP_PACKAGE=./cmd/orders APP_NAME=orders ARCH=arm64 DESTDIR=/srv/orders/bin` | Builds and atomically replaces the named executable in the selected directory |

**Deployment unit.** Ship the Go application that imports Dostoevsky. Set `APP_DIR` to that application's module and `APP_PACKAGE` to its main package. The build uses the dependency version selected by the host's go.mod. To use an unpublished source release, point the host's go.mod replace directive for `github.com/rodriveracom/dostoevsky` at the extracted module. Choose the target architecture explicitly for cross-compilation. The host application supplies its production journal directory, topology, credentials and process supervision. Installing a binary leaves the running process and its journal alone until your deployment system restarts it.

## Stream OpenAI events into a topic

These two examples call OpenAI over HTTP using Go's standard library. Set OPENAI_API_KEY in your shell and OPENAI_MODEL to a model available to your project for the chosen endpoint. The hosted agent needs an application key with api.agents.read, api.agents.write and api.responses.write. Live runs use your project's API quota.

~~~sh
make dev-openai-text RUN_DIR=.dev/data/explanation
make dev-openai-agent RUN_DIR=.dev/data/latencies
make dev-openai-replay RUN_DIR=.dev/data/latencies
~~~

The first command asks a model to explain checkpoints and records its Responses API events. The second creates an Agents API session and asks the agent to calculate a median in its hosted sandbox. Inspect the output events for the tool execution and the reported median (8 ms). Each received JSON event is appended to the local events topic before printing. Its offset identifies the event during local replay. Replay reads from offset zero and requires neither credentials nor network access.

Use a fresh journal directory for each live run. If the connection drops, replay shows the recorded prefix. Inspect the remote agent session using its session ID before deciding to start another run. Reissuing a create request can start another billable task. Hosted sessions are retained for inspection and can be deleted using the API.

[Agents API quickstart](https://developers.openai.com/api/docs/guides/agents-api/quickstart) · [Responses streaming guide](https://developers.openai.com/api/docs/guides/streaming-responses)

## Use a topic as a background task queue

A producer submits a named task and its input, then exits. Run the workers later. These examples share one local journal and can be run in sequence:

~~~sh
make dev-task-enqueue JOB_ID=hash-1 TASK=sha256 INPUT='hello world'
make dev-task-enqueue JOB_ID=retry-1 TASK=retry-demo INPUT='hello world'
make dev-task-enqueue JOB_ID=fail-1 TASK=exhaust-demo
make dev-task-drain
make dev-task-inspect
~~~

Look for a SHA-256 digest on the results topic, HELLO WORLD after two attempts, and a dead letter after three attempts. The latter two handlers deliberately simulate failures. An unknown task goes directly to dead letters. Each dead letter includes the original input and failure reason.

The jobs topic has four partitions and one consumer group. The drain command runs one goroutine per partition and exits when all available work is processed. The job ID routes every retry to the same partition. A retry appends immediately to its tail, allowing other queued work to run first. The worker publishes the result, retry or dead letter before acknowledging the original delivery by committing its offset. Inspection reads the outcome topics from the beginning on every invocation.

Run drain again after interruption. An unacknowledged handler may execute again. These example handlers are deterministic, and stable IDs deduplicate repeated publications. For real handlers that send emails, charge accounts or call external APIs, use a durable idempotency key at the destination and persist the outcome before acknowledging. A different outcome on replay requires application-level reconciliation. This example assigns partitions explicitly and runs producer and drain commands sequentially because a local directory has a single owner. A running application can share one Node between its producers and workers. The example implements immediate retries and has no worker leases or scheduled delivery.

## Contribute

Open an issue describing the problem, with a failing test when you can. For a change, fork the repository, branch, and run `make verify` (format, vet, tests, race detector, example build) before opening a pull request. Keep changes small and focused, and add or update tests and the CHANGELOG. Contributions are accepted under the MIT license.
