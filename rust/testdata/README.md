# Vectors written by the Go engine

Each file here is the Go engine's own answer to a list of inputs, and a Rust test replays it line for line. None is
edited by hand. `regenerate.sh` writes them all again.

| File | Written by | Rows | Checks |
| --- | --- | ---: | --- |
| `gojson-strings.txt` | `gojson_vectors.go`, **Go 1.27.0** | 2,075 | `gojson::write_string`, with and without HTML escaping |
| `gojson-reader.txt` | `gojson_reader_vectors.go`, **Go 1.27.0** | 1,595 | `gojson::parse`: what Go refuses, accepts and decodes |
| `seam-go.txt` | `seam_vectors.go` | 64 | the fixed clock and random source against `internal/seam` |
| `wire-go.txt` | `wire_vectors.go` | 8,513 | every verdict and decoded field of package `wire` on damaged frames, the long one included |
| `wire-more-go.txt` | `wire_more_vectors.go` | 8,545 | package `wire` on 2,968 large inputs, fields at and beyond every limit up to a body one byte too long; and its three encoders on 5,577 calls, with what they refuse |
| `driver-requests-go.txt` | `driver_vectors_test.go.txt`, **Go 1.27.0** | 7,621 | how the Go conformance driver binds a request: bytes as numbers, folded names, `null`, a key given two and three times |
| `crc32c-go.txt` | `internal_vectors_test.go.txt` | 300 | CRC-32C, whole and continued |
| `frame3-go.txt` | the same | 4,018 | frames, records, commits and topic definitions: bytes written, verdicts, fields, error texts |
| `checkpoint3-go.txt` | the same | 535 | `readCheckpoint3` on whole, cut, damaged and reordered checkpoints |
| `gotime-go.txt` | the same | 5,318 | what `Time.UnmarshalJSON` accepts, and the instant it reads |
| `base64-go.txt` | the same | 2,162 | what `encoding/base64` accepts and decodes; 30 rows are named cases of loose final bits and line ends |
| `frametime-go.txt` | the same | 948 | a time to the frame's time field and back, at and around both ends of the range |
| `recordscan-go.txt` | the same, **Go 1.27.0** | 2,018 | the legacy journal's record scanner |
| `idtable-go.txt` | the same | 645 | the ID hash; the ID table's order of visits, with offsets up to the largest u64; the ID filter's bytes and answers, for filters of odd lengths and for tables around the filter's size cap |
| `segindex-go.txt` | the same | 801 | the segment index: 22 built record by record and asked up to the largest offset, 779 idx files read whole, cut or damaged |
| `index-misc-go.txt` | the same | 92 | the admission sketch, 32 of them one ID driven up to and past the counter's limit and across each halving; the location index |
| `goquote-go.txt` | `quote_vectors.go` | 664 | `strconv.Quote`, which is `%q` in Go's error texts |
| `sha256-lengths.txt` | Python's `hashlib` | 200 | SHA-256 at lengths around the block boundaries |

The `.go` programs carry `//go:build ignore`, so the Go module does not build them. The two `.go.txt` files call
unexported code (the engine's functions, the driver's request type). `regenerate.sh` gives each to the Go tool as an
overlay on its package, so nothing is written into the source tree; the script works in a temporary directory and
copies its results only when every expected file is there. `quote_table.go` writes a Rust source file,
`rust/crates/dostoevsky-format/src/goquote_table.rs`, and the script copies that too.

Four files come from Go 1.27.0. Three of them hold what its JSON encoder writes (the two JSON files, which define a
format, and `recordscan-go.txt`, whose inputs are built with the encoder), and `regenerate.sh` leaves those alone under
any other release. `driver-requests-go.txt` holds how its decoder binds a request; it needs a Go driver that has the
seam requests, and without one the script stops. The other files were written with Go 1.24.7, as their first line
says; none of them holds JSON, and they are to be written again with 1.27.0.

The index rows (`idtable-go.txt`, `segindex-go.txt`, `index-misc-go.txt`) are drawn from their own seed, so a change to
the rows before them leaves these files as they were.

Each Rust test also counts the rows of each kind it replayed and fails below a floor, so a file that loses a kind of
row fails its test.
