#!/bin/sh
# Writes every vector file in this directory again with the Go toolchain on the PATH. Run from anywhere.
set -eu
here=$(cd "$(dirname "$0")" && pwd -P)
root=$(cd "$here/../.." && pwd -P)
cd "$root"
# The two JSON files define a format as Go 1.27.0 writes it, so only that release may write them.
if [ "$(go env GOVERSION)" = go1.27.0 ]; then
 go run rust/testdata/gojson_vectors.go > "$here/gojson-strings.txt"
 go run rust/testdata/gojson_reader_vectors.go > "$here/gojson-reader.txt"
else
 echo "left gojson-strings.txt and gojson-reader.txt alone: this is $(go env GOVERSION), they need go1.27.0"
fi
go run rust/testdata/seam_vectors.go > "$here/seam-go.txt"
go run rust/testdata/wire_vectors.go > "$here/wire-go.txt"
# The format vectors come from unexported functions, so their writer is compiled inside the package for one run.
trap 'rm -f "$root/zz_rust_vectors_test.go"' EXIT
cp "$here/internal_vectors_test.go.txt" "$root/zz_rust_vectors_test.go"
DOST_RUST_VECTORS="$here" go test -count=1 -run '^TestWriteRustVectors$' -v . | grep -E 'rows|ok|FAIL'
