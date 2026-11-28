# Port notes

Phase 1 of the Rust engine is a faithful port of the Go engine: the same on-disk bytes, the same wire bytes, the same
error codes, the same results on the conformance suite. This file records how the Rust code maps to the Go code, each
Go behavior the port keeps on purpose, each it changes, and what is not yet known.

## State

| Row | What | State |
| ---: | --- | --- |
| 0 | Workspace, layout, JSON as Go writes and reads it, the test seam, the conformance driver, the differential driver | Done |
| 1 | Wire codec | Not started |
| 2 | Segment frames, checkpoints, the record scanner | Not started |
| 3 to 14 | Everything that opens a store | Not started |

No engine request works yet. The Rust driver answers each one with the error `other` and names the row that brings it.

## Map from Go to Rust

One Rust module per Go file, with the same name, so the two can be read side by side. A module that is still a stub
says so in its first line and names its row.

| Row | Go | Crate | Module | State |
| ---: | --- | --- | --- | --- |
| 0 | `cmd/dost-conform/main.go`, `workflow.go` | `dostoevsky-cli` | `conform`, binary `dost-conform` | ported; engine calls answer "not ported" |
| 0 | `internal/seam/seam.go` | `dostoevsky-core` | `seam` | ported |
| 0 | `internal/seam/points.go` | `dostoevsky-core` | `seam` (`arm`, `reach`, `disarm`) | mechanism only; the Go file has not been seen |
| 0 | the sentinel errors in `node.go` and its neighbours | `dostoevsky-core` | `errors` | ported |
| 0 | `encoding/json` as the engine uses it | `dostoevsky-format` | `gojson`, `base64` | ported for strings, integers and bytes |
| 1 | `wire/wire.go` | `dostoevsky-wire` | `wire` | stub |
| 2 | `checkpoint3.go` | `dostoevsky-format` | `checkpoint3` | stub |
| 2 | `frame3.go` | `dostoevsky-format` | `frame3` | stub |
| 2 | `recordscan.go` | `dostoevsky-format` | `recordscan` | stub |
| 3 | `admission.go` | `dostoevsky-core` | `admission` | stub |
| 3 | `id_index.go` | `dostoevsky-core` | `id_index` | stub |
| 3 | `idfilter.go` | `dostoevsky-format` | `idfilter` | stub |
| 3 | `location_index.go` | `dostoevsky-core` | `location_index` | stub |
| 3 | `segindex.go` | `dostoevsky-format` | `segindex` | stub |
| 4 | `journal.go` | `dostoevsky-core` | `journal` | stub |
| 4 | `logstate.go` | `dostoevsky-core` | `logstate` | stub |
| 4 | `sync_linux.go, sync_other.go` | `dostoevsky-core` | `sync` | stub |
| 5 | `seglayer.go` | `dostoevsky-core` | `seglayer` | stub |
| 5 | `segmented_node.go` | `dostoevsky-core` | `segmented_node` | stub |
| 6 | `batch.go` | `dostoevsky-core` | `batch` | stub |
| 6 | `cap.go` | `dostoevsky-core` | `cap` | stub |
| 6 | `node.go` | `dostoevsky-core` | `node` | stub |
| 6 | `notify.go` | `dostoevsky-core` | `notify` | stub |
| 6 | `publish_batch.go` | `dostoevsky-core` | `publish_batch` | stub |
| 6 | `publish_queue.go` | `dostoevsky-core` | `publish_queue` | stub |
| 6 | `stream.go` | `dostoevsky-core` | `stream` | stub |
| 7 | `retention.go` | `dostoevsky-core` | `retention` | stub |
| 8 | `backup.go` | `dostoevsky-core` | `backup` | stub |
| 8 | `migration.go` | `dostoevsky-core` | `migration` | stub |
| 8 | `repair.go` | `dostoevsky-core` | `repair` | stub |
| 8 | `upgrade.go` | `dostoevsky-core` | `upgrade` | stub |
| 9 | `workflow_log.go` | `dostoevsky-core` | `workflow_log` | stub |
| 10 | `workflow/store.go` | `dostoevsky-workflow` | `store` | stub |
| 11 | `wireserver/conn.go` | `dostoevsky-server` | `conn` | stub |
| 11 | `wireserver/pool.go` | `dostoevsky-server` | `pool` | stub |
| 11 | `wireserver/sequencer.go` | `dostoevsky-server` | `sequencer` | stub |
| 11 | `wireserver/server.go` | `dostoevsky-server` | `server` | stub |
| 12 | `clienthttp/handler.go` | `dostoevsky-server` | `clienthttp` | stub |
| 12 | `peers.go` | `dostoevsky-core` | `peers` | stub |
| 13 | `sketch/hll.go` | `dostoevsky-core` | `sketch::hll` | stub |
| 13 | `sketch/hot_topics.go` | `dostoevsky-core` | `sketch::hot_topics` | stub |
| 13 | `sketch/latency.go` | `dostoevsky-core` | `sketch::latency` | stub |
| 8 | `cmd/dost-repair`, `cmd/dost-storage` | `dostoevsky-cli` | binaries of the same names | not started |
| 11 | `cmd/dostoevsky-server` | `dostoevsky-server` | binary of the same name | not started |
| 12 | `cmd/dostoevsky-gateway` | `dostoevsky-server` | binary of the same name | not started |
| 14 | (none) | `dostoevsky-py`, `dostoevsky-node` | | empty |

`peers` and the `sketch` package are in `dostoevsky-core`. `clienthttp` and the gateway binary are in
`dostoevsky-server`. Not ported in phase 1: `cmd/plots`, `cmd/sketchreport`, `examples/`, `benchmarks/`, the existing
clients. `benchmarks/rustcore` is a frozen record of an earlier prototype: it lies outside `rust/`, is not a workspace
member, and the gates do not touch it.

Where a Go file mixes encoding with file access, the encoding goes to `dostoevsky-format` and the file access to
`dostoevsky-core`. The placement of the row 3 and row 4 files in the table is provisional until they are ported.

## What Go does, and what Rust does

### The JSON encoding

1. **The format is the output of Go 1.27.0's `encoding/json`.** Go writes `<`, `>` and `&` as `\u003c`, `\u003e`,
   `\u0026`; U+2028 and U+2029 as escapes; control bytes as `\b`, `\f`, `\n`, `\r`, `\t` or `\u00XX`; struct fields in
   declaration order and map keys sorted by bytes. Rust (`gojson`) writes the same bytes. Checked against 2,075 strings
   written by Go 1.27.0.
2. **Invalid UTF-8.** Go 1.27.0 writes each invalid byte as the three bytes EF BF BD; earlier releases wrote the six
   characters `\ufffd`. Rust writes the 1.27.0 form and reads both. Go replaces one invalid byte at a time; Rust's own
   lossy conversion replaces a whole invalid sequence at once, so `gojson` does not use it.
3. **What Go's decoder accepts.** Invalid UTF-8 inside strings, unpaired surrogate escapes (replaced by U+FFFD, and what
   follows read again on its own), duplicate keys (the last wins), and nesting up to 10,000. Rust accepts the same and
   refuses the same. Checked against 1,595 inputs through Go 1.27.0's decoder.
4. **Nesting to 10,000 must not overflow the stack.** Go's stacks grow; a Rust thread's does not. The reader, the
   writer and the destructor of `gojson::Value` keep their own stacks.
5. **A nil byte slice and an empty one differ on the wire.** Go writes a nil `[]byte` as `null` and an empty one as
   `""`. Rust carries such fields as `Option<Vec<u8>>`, and each ported function returns the same one of the two as Go.
6. **Times and floating-point numbers** in Go's JSON format are not read or written yet. Each is added in the row that
   first needs it.

### The driver

7. **Requests are read as Go's decoder reads the request struct.** A key matches a field by exact name, else without
   regard to case. `null` leaves a field at its zero value. An unknown field, a value of the wrong type, or an integer
   written with a fraction or exponent ends the driver with status 1 and no reply. A request may span lines.
8. **A Go error can wrap several sentinels,** and the Go driver reports the first match in a fixed order. Rust's
   `errors::Error` carries every kind it wraps and `Error::code` applies that order. The three workflow codes
   (`start_conflict`, `stale`, `missing`) join with the workflow layer.
9. **`fix_inputs` must come before anything is opened.** Go's `seam.Fix` is not safe while a node or store is in use.
   The Rust driver refuses `fix_inputs` once a node or store is open.

### The seam

10. **Clock and random source follow Go's `internal/seam`.** The k-th reading after `fix` (k from 0) is
    `start + k * step`; `advance` moves the clock without counting as a reading; random bytes are
    SHA-256(seed || counter) blocks with seed and counter as 8 bytes little-endian, and what is left of a request's last
    block is not used again. Checked against 20 readings and 32 requests made of the Go package.
11. **Write order.** Where Go writes partitions and consumer groups it holds in maps (legacy retention, the legacy
    export of a segmented store, migration), it writes partitions ordered by topic and then number, and groups by name.
    Checkpoints order partitions by their stored identifier. Rust must write in these orders; a Go map written in map
    order anywhere else is a defect to report, not a behavior to copy. Nothing ported so far writes from a map.
12. **Named points.** An armed point fires exactly once, on pass `skip + 1`; passes before and after do nothing. Rust's
    `seam::arm`, `reach` and `disarm` do this. The list of points, and where each is reached, come from Go's
    `internal/seam/points.go`, which the ported modules must follow as they are written.
13. **The system's random source.** Unset, Go reads `crypto/rand`; Rust reads `/dev/urandom`. Platforms without that
    file need another source before the workflow layer can run there.

### The build

14. **The source release copies `rust/` as it stands** (`scripts/release.sh`; a test requires every top-level directory
    to be listed). Cargo's build output therefore goes to `.dev/rust-target`, outside `rust/`.
## Observed in the Go engine

15. **Without the seam, two runs of the Go engine differ in more than record times.** The differential driver, run
    with the Go engine against itself, shows differences in the legacy journal and the segment logs, and also in the
    `.idx` and `.bloom` sidecars and in a backup's `journal.v1`. With the seam fixed these must all vanish; that run
    needs `fix_inputs` in the Go driver.

16. **Two conformance pairings pass for an engine that refuses everything.** `incomplete-backup-directory`, with Go as
    the writer and Rust as the reader, passes although the Rust driver has no engine: its last step only requires that
    opening fails. The case would be stricter if it required the code `incomplete_backup`.

## Not yet known

- **`internal/seam/points.go`** has not been seen. The point names, the meaning of `kill` and `skip` beyond note 12, and
  the type of `reached` in the `disarm` reply (written here as a number) are to be checked against it.
- **Go 1.27.0** was not on the machine that wrote the vector files other than the two JSON files. `seam-go.txt` comes
  from Go 1.24.7. Nothing in them depends on the JSON encoder's change between those releases, but they should be
  written again with 1.27.0 (the programs in `rust/testdata`).
- **The lint gate on releases newer than the pinned one.** `sha256.rs` avoids `chunks_exact`, which
  newer lints object to; that could not be confirmed with a newer release.
- **Wire bytes of a server.** The differential driver compares stores and driver replies. Comparing wire bytes needs
  the server (row 11).
- **Other platforms.** Everything ran on one x86-64 Linux machine.
