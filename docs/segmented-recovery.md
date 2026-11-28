# Segmented storage and recovery

Use `OpenSegmented(dir, cfg)` for a new store, or pass `-segmented` when first starting the native server. Subsequent calls to `Open` recognize the format. A new directory opened through ordinary `Open` still uses the legacy journal. Clients use the same publish, read and checkpoint operations with both formats.

## Startup and integrity

Each segment begins with a checksummed catalog of partition names, next offsets, retention floors and group state. The writer rotates after approximately 8 MiB of new frames. The starting checkpoint and the final accepted batch can add to the segment size. The writer syncs the new segment and its directory and publishes a checksummed file naming the required range before acknowledging writes into it. Retention records new floors durably before unlinking an expired prefix.

Startup enumerates segment names, checks the required range, loads the active checkpoint, and replays its tail. Cost grows with segment count, active-tail bytes, partitions and groups. Sealed payloads are read on demand. An incomplete final append can be truncated in the active file. A damaged complete frame yields an error when checked. An application can record `Node.Truncated()` at startup.

Call `VerifySegments(ctx)` to read and checksum the complete physical history and check retained offset continuity. It holds a read lock for the duration. Schedule it when the writer can pause. A full backup performs this scan while exporting the retained records. Use an independent backup to recover data when verification reports corruption.

Each sealed segment can have a rebuildable index. Random reads locate a segment and scan forward from a sparse offset. Pages reuse a read window. ID-bearing publishes compare candidate matches with full records. Sealed segments can carry a checksummed Bloom filter. A definite miss skips the segment's exact ID index. A possible match still checks the index and the complete stored ID and payload. The first identified publish lazily loads the filters, and older directories build missing filters from verified indexes. To prepare a stopped store before upgrading, run `dost-storage -mode prepare-ids -config topology.json -data segmented-store`. An embedded application can call `PrepareIDFilters(ctx)` before accepting traffic. The filter cache has a 64 MiB bit-array budget. A workload exceeding it can reload filters from disk. Benchmark the actual retained ID count and churn. Large numbers of distinct partitions or consumer groups also increase checkpoint size. A lagging partition can keep a mixed segment on disk after other partitions advance.

A workload whose retained IDs exceed the filter budget, for example one scanning from newest to oldest, can reload the same filters repeatedly; how much that costs has not been measured.

**Keep consumer group state small.** Every segment starts with a checkpoint holding all groups' state, and records must fit in the first 4 GiB of a segment. A store whose checkpoint leaves no room refuses publishes with the limit error. Commits are still accepted, but this version cannot delete a group, and a group that has reached the end of its partition cannot commit again while publishes are refused. **There is no tested way out of this state in place.** Stay far below the limit.

**Do not run `dost-repair` or the upgrade tool of an earlier release on a segmented directory.** An old repair can remove the format marker and leave the segment data behind.

**An interrupted first initialization** (stopped before the format marker was completely written) acknowledged nothing. `OpenSegmented` and `workflow.Open` clear it and start again when `segments/` is empty, holds only staging files, or holds the first segment with nothing after its start checkpoint, with or without a range file naming exactly that segment. Plain `Open` refuses it. A directory refused for holding anything else is left untouched: keep it for diagnosis and initialize a new one elsewhere. Only if you know the directory is new and never served a write may you delete it yourself.

## Migrate, verify and roll back

Stop the source application. Set aside enough space for both stores and your backup. Supply the application's current topology JSON with its exact node, node list and topic counts. Opening a store performs ordinary recovery and can truncate an incomplete active tail. The CLI accepts a new destination with an existing parent. Keep the destination under exclusive operator control while the copy runs.

```sh
make dev-storage
.dev/bin/dost-storage -mode migrate -config topology.json -data legacy-store -dest segmented-store
.dev/bin/dost-storage -mode verify -config topology.json -data segmented-store
```

Point the application at the verified directory. The source retains the state it held at cutover. To carry forward writes made after cutover into a legacy application, stop the writer and export a fresh snapshot.

```sh
.dev/bin/dost-storage -mode backup -config topology.json -data segmented-store -dest rollback-store
```

Open the export with the previous legacy reader. Migration and export preserve retained offsets, timestamps, payload bytes, keys, IDs, floors and consumer state. Both operations publish their destination by renaming a synced staging directory. Back up the source before running older storage utilities. The current legacy repair tool refuses segmented directories.

## Reproduce the measurements

On Linux, select a volume with sufficient free space. Each scale run uses a unique disposable directory. Set GOMAXPROCS to match your comparison host. The driver records fresh-process startup RSS separately from the peak RSS of a full scan or readback. Filesystem caches remain warm.

```sh
GOMAXPROCS=2 make dev-storage-scale RECOVERY_PARENT=/path/to/test-volume RECOVERY_RECORDS=10000000 RECOVERY_BYTES=512
GOMAXPROCS=2 make dev-storage-scale RECOVERY_PARENT=/path/to/test-volume RECOVERY_RECORDS=100000000 RECOVERY_BYTES=8
GOMAXPROCS=2 make dev-storage-catalog RECOVERY_PARENT=/path/to/test-volume RECOVERY_TOPICS=100000
make dev-storage-recovery
make dev-storage-crash
```

The scale driver verifies deterministic payloads through the public batch reader and checks the end cursor. The catalog benchmark verifies saved group state, samples payloads, and checks a retained floor. The TCP process-kill test compares durable records and idempotent retries against receipts recorded by a surviving client.
