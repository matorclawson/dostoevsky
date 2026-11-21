# Dostoevsky

Dostoevsky is an embeddable Go library for durable topics and stateful stream processing. Start with a directory and a topic manifest. To distribute partitions, embed the same library in each participating application and mount its peer handler on the application's HTTP server. All runtime dependencies come from the Go standard library. MIT licensed.

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

**Deployment unit.** Ship the Go application that imports Dostoevsky. Set `APP_DIR` to that application's module and `APP_PACKAGE` to its main package. The build uses the dependency version selected by the host's go.mod. To use an unpublished source release, point the host's go.mod replace directive for `github.com/zeroemployeeorg/dostoevsky` at the extracted module. Choose the target architecture explicitly for cross-compilation. The host application supplies its production journal directory, topology, credentials and process supervision. Installing a binary leaves the running process and its journal alone until your deployment system restarts it.

## Run a persistent word count

Use Go 1.22 or later on Linux and extract the source archive. From the module directory:

```sh
make dev-wordcount WORDS="blue red blue"
# {"blue":2,"red":1}
make dev-wordcount WORDS=red
# {"blue":2,"red":2}
make dev-wordcount
# {"blue":2,"red":2}
```

Each argument creates one event. The example commits the counts and the input offset together. Reopening the directory resumes at the next offset. To replay historical messages, read from offset zero or choose a fresh consumer group.

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
