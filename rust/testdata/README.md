# Vectors written by the Go engine

Each file here is the Go engine's own answer to a list of inputs, and a Rust test replays it line for line. None is
edited by hand.

| File | Written by | Rows | Checks |
| --- | --- | ---: | --- |
| `gojson-strings.txt` | `gojson_vectors.go`, **Go 1.27.0** | 2,075 | `gojson::write_string`, with and without HTML escaping |
| `gojson-reader.txt` | `gojson_reader_vectors.go`, **Go 1.27.0** | 1,595 | `gojson::parse`: what Go refuses, accepts and decodes |
| `seam-go.txt` | `seam_vectors.go` | 64 | the fixed clock and random source against `internal/seam` |
| `wire-go.txt` | `wire_vectors.go` | 7,998 | every verdict and decoded field of package `wire` on damaged frames |
| `sha256-lengths.txt` | Python's `hashlib` | 200 | SHA-256 at lengths around the block boundaries |

The `.go` programs here carry `//go:build ignore`, so the Go module does not build them. Each says in its first lines how
to run it.

The two JSON files define a format as Go 1.27.0 writes it and were written with that release. The other Go-written
files were written with Go 1.24.7; they do not depend on the JSON encoder.
