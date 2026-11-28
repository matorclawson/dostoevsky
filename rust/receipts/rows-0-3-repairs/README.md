# Receipts: binding under Go 1.27.0, quoting, the runner, and the index rows

Every path is relative to the repository root.

| File | What it shows |
| --- | --- |
| `environment.txt` | machine, toolchains, which Go driver was used |
| `rust-check.txt` | `make rust-check` with the pinned Rust 1.97.0: format, lints as errors, 53 tests |
| `crc32c-backend.txt` | the CRC-32C path the rows went through on this machine |
| `go-verify.txt` | `make verify`: every Go package passes |
| `conformance.txt` | `make conformance-rust`: 28 pairings pass and 76 fail, as before; the 76 need the Rust store |
| `differential-selftest.txt` | the runner against stand-in drivers: 38 cases, each ending with the status it must |
| `regenerate-on-this-tree.txt` | `rust/testdata/regenerate.sh` on this tree: it writes nothing and exits 1, because the Go driver here has no seam requests and `driver-requests-go.txt` cannot be written |
| `../seam/` | the differential runs, made again |

## What each change is checked by

| Change | Check |
| --- | --- |
| Base64 text over a byte field, as Go 1.27.0 | `requests_bind_as_the_go_driver_binds_them` replays the file written by Go 1.27.0: 7,621 rows, 5,683 bound, 1,935 refused, 3 empty |
| Names quoted as `%q` | `quoting_matches_go`: 664 rows of `strconv.Quote`; the unit test in `seam.rs` for a name with a control character, a line end and quotes |
| The order and texts of the two `arm` refusals | the unit test in `seam.rs`; the `seam-rearm` trace, for drivers that have the rule |
| A write deadline | self-test: a driver that stops reading, for requests and for a stream |
| A refusal needs `"ok": false` and an error code | self-test: accepted, no `ok`, `ok` null, no code, empty code, not an object: status 2 each |
| The vector script fails on a missing output | `regenerate-on-this-tree.txt` |
| The index types' private fields, the footer on decode | `the_segment_index_matches_go`: each of 779 decoded files carries its footer and writes back from it |
| The index rows' own seed and the rows at the edges | `the_id_table_and_filter_match_go`: 84 tables (19 with the largest offset), 304 positions, 53 filters asked directly (48 of odd length), 4 tables around the size cap; 62 sketches, 32 around the counter's limit |

## Not done

- **The base.** The series is still on `main` at `65020a52`. The current `main`, with the Go side of the point rules,
  was not at hand.
- **The `subtle` crate.** The commit that adds it and its lock file was not at hand, and no package registry can be
  reached from here. `digests_equal` is unchanged.
- **Thirteen vector files and `goquote_table.rs` still name Go 1.24.7** in their first line. Go 1.27.0 is not on this
  machine. Writing a different release's name into them by hand would make the line untrue.
- **The kill line between two drivers.** No ported module reaches a point through the driver, so no trace has a
  request that dies.
