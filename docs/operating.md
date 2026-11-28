# Operating Dostoevsky

What is guaranteed and what is not, retention, backups and deployment. The mechanisms are explained in [how it works](how-it-works.md).

## Durability and operating limits

* Each owner holds one checksummed append-only journal on a local filesystem with exclusive advisory locking. Acknowledgments follow file sync. Recovery verifies complete frames and truncates a partial final frame. A checksum failure stops recovery.
* Each partition retains an in-memory index of record positions and optional producer IDs. Payloads stay on disk. Startup scans the journal. Each commit stores the full group state. History is retained until an application calls Retain, and storage operations are serialized per node. Plan for disk growth, replay time and index memory.
* Values and processor state are limited to 1 MiB each, keys to 4096 bytes and producer IDs to 256 bytes. Topic and group names use letters, digits, hyphens and underscores, up to 96 characters.
* Each partition has **one durable copy**. An offline owner makes its partitions unavailable. Disk loss requires restoring that owner's backup. For a consistent backup, close the node and copy its directory. Never start two live owners with the same identity and different directories.
* A lost publish response leaves the outcome uncertain. Retry with the same key, ID and payload. After an uncertain commit, read Position before deciding whether to retry. Cancellation interrupts waiting readers. A disk write already in progress may complete.
* Membership is static. This version has no quorum replication, automatic failover, automatic retention scheduling, dynamic group balancing, or event-time window engine. Placement is per partition. There is no global event order.

## Retention and restart recovery

Call `node.Retain(ctx, map[Partition]int64{partition: floor})` to remove records strictly below an absolute offset. The operation refuses a floor above any existing consumer group's committed position. A reader requesting an expired offset receives `ErrExpired`. Keep the floor at or below the oldest offset needed by all consumers, including consumers that have yet to register. New consumer groups start at offset zero and must handle an expired offset themselves.

Retention rewrites the journal while holding the node lock, syncs a replacement, then atomically renames it and syncs the directory. Retained offsets, checkpoints and state survive restart. A cancellation before replacement leaves the old journal in place. A crash may leave an unused temporary file. A successful replacement requires enough free space to hold the new journal alongside the old one. If directory synchronization reports an error after rename, reopen and inspect the resulting state before retrying.

Producer IDs attached to deleted records expire at the retention floor. Retrying a deleted ID can publish a new record. Set retention no shorter than the application's producer retry horizon. Records and IDs at or above the floor remain exact. Backups taken before retention preserve the historical journal.

Restart scans the compacted journal. Retention reduces the amount of history to scan. Recovery time still grows with the retained journal, and this version has no independent startup snapshot or on-disk index. Call retention regularly according to the application's storage and replay requirements.

## Development and deployment

Run `make help` for the command list. The Makefile targets Linux with GNU Make and coreutils. Go 1.22+ and gofmt must be on PATH. The race check also requires a C compiler.

Inside this repository `go.mod` names `toolchain go1.27.0`. A Go 1.22 to 1.26 install fetches it on first use under automatic toolchain selection; commands that run offline (`make test`, `make verify`, `make release`) need it already installed or cached. Applications that import the library need only Go 1.22 or later.

| Workflow | Command | Result |
| --- | --- | --- |
| Development | `make dev` | Checks formatting, vets, tests, runs the race detector and builds examples in `.dev/bin/` |
| Local demos | `make dev-wordcount WORDS='blue red blue'` or `make dev-mesh-a` and `make dev-mesh-b` | Keeps demo journals in `.dev/data/` |
| Release the library | `make release` | Writes a versioned source tarball and SHA-256 file to `dist/` after testing an extracted copy |
| Verify a release | `make release-check` | Checks the checksum and reruns offline tests from the archive |
| Build for deployment | `make deploy-build APP_DIR=../orders APP_PACKAGE=./cmd/orders APP_NAME=orders ARCH=arm64` | Builds a pure-Go Linux executable in `build/deploy/linux-arm64/` |
| Install | `make deploy APP_DIR=../orders APP_PACKAGE=./cmd/orders APP_NAME=orders ARCH=arm64 DESTDIR=/srv/orders/bin` | Builds and atomically replaces the named executable in the selected directory |

**Deployment unit.** Ship the Go application that imports Dostoevsky. Set `APP_DIR` to that application's module and `APP_PACKAGE` to its main package. The build uses the dependency version selected by the host's go.mod. To use an unpublished source release, point the host's go.mod replace directive for `github.com/rodriveracom/dostoevsky` at the extracted module. Choose the target architecture explicitly for cross-compilation. The host application supplies its production journal directory, topology, credentials and process supervision. Installing a binary leaves the running process and its journal alone until your deployment system restarts it.

## Performance comparisons

Run `make dev-bench-embedded` on Linux to compare embedded single-node publishers on your disk. Set `TMPDIR` to a directory on the intended volume. The benchmark module requires Go 1.26 or later. See [workloads and measurements](embedded-performance.md).

Concurrent calls to `Node.Publish` can share a journal sync. A lone producer writes immediately. Acknowledgments follow the sync, and messages from different calls keep their own receipts. Run `make dev-bench-publish` on Linux for the [concurrent publishing comparison](concurrent-publishing.md).

Run `make dev-bench-publish-gate` on Linux to compare synchronous publishers at one, eight and 32 producers. The command checks median throughput and peak process memory against embedded JetStream. See [measurements and reproduction](publisher-gate.md).

## Design lineage and verification

Faust's supplied word-count example motivated keyed streams and recoverable processor state. Kafka's partition logs, replay offsets and independent consumer groups inspired the delivery model. This repository implements those ideas in original Go and includes two executable examples.

```sh
CGO_ENABLED=0 GOPROXY=off go test ./...
go test -race ./...
go vet ./...
```

Tests exercise recovery after process exit, incomplete frames, checksum failures, idempotent publish, concurrent commits, independent groups, cancellation, pipeline retry and peer routing. The race detector is a development tool and requires a C toolchain on Linux. Applications build with CGO_ENABLED=0.
