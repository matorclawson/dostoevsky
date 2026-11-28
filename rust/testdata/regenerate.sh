#!/usr/bin/env bash
# Writes every vector file in this directory again with the Go toolchain on the PATH. Run from anywhere.
#
# Three files are written only by Go 1.27.0, because they hold what that release's JSON encoder writes: the two JSON
# files, which define a format, and the record scanner's rows, whose inputs are built with the encoder. Any other
# release leaves them alone and says so.
#
# Nothing is written into the checkout until every writer has succeeded and every expected file is there. The two
# writers that must be compiled inside a package are given to the Go tool as an overlay, so no file is added to or
# removed from the source tree.
set -eu
set -o pipefail
here=$(cd "$(dirname "$0")" && pwd -P)
root=$(cd "$here/../.." && pwd -P)
cd "$root"
new=$(mktemp -d)
trap 'rm -rf "$new"' EXIT
mkdir "$new/out" "$new/src"
version=$(go env GOVERSION)

go run rust/testdata/seam_vectors.go > "$new/out/seam-go.txt"
go run rust/testdata/wire_vectors.go > "$new/out/wire-go.txt"
go run rust/testdata/wire_more_vectors.go > "$new/out/wire-more-go.txt"
go run rust/testdata/gojson_vectors.go > "$new/out/gojson-strings.txt"
go run rust/testdata/gojson_reader_vectors.go > "$new/out/gojson-reader.txt"
go run rust/testdata/quote_vectors.go > "$new/out/goquote-go.txt"
go run rust/testdata/quote_table.go > "$new/src/goquote_table.rs"

# The format vectors come from unexported functions, and the driver's from its unexported request type.
cp "$here/internal_vectors_test.go.txt" "$new/src/engine_test.go"
cp "$here/driver_vectors_test.go.txt" "$new/src/driver_test.go"
cat > "$new/overlay.json" <<OVERLAY
{"Replace": {
 "$root/zz_rust_vectors_test.go": "$new/src/engine_test.go",
 "$root/cmd/dost-conform/zz_rust_vectors_test.go": "$new/src/driver_test.go"
}}
OVERLAY
DOST_RUST_VECTORS="$new/out" go test -overlay "$new/overlay.json" -count=1 -run '^TestWriteRustVectors$' -v . | grep -E 'rows|^ok|FAIL|SKIP'
DOST_RUST_VECTORS="$new/out" go test -overlay "$new/overlay.json" -count=1 -run '^TestWriteRustDriverVectors$' -v ./cmd/dost-conform | grep -E 'rows|^ok|FAIL|SKIP'

expected="base64-go.txt checkpoint3-go.txt crc32c-go.txt driver-requests-go.txt frame3-go.txt frametime-go.txt gojson-reader.txt
 gojson-strings.txt goquote-go.txt gotime-go.txt recordscan-go.txt seam-go.txt wire-go.txt wire-more-go.txt"
missing=
for name in $expected; do
 [ -s "$new/out/$name" ] || missing="$missing $name"
done
for file in "$new"/out/*; do
 case " $(echo $expected) " in
 *" $(basename "$file") "*) ;;
 *) missing="$missing (unexpected: $(basename "$file"))" ;;
 esac
done
[ -s "$new/src/goquote_table.rs" ] || missing="$missing goquote_table.rs"
if [ -n "$missing" ]; then
 echo "nothing was written: these outputs are missing or empty:$missing" >&2
 echo "(driver-requests-go.txt needs a Go driver that has the test seam requests)" >&2
 exit 1
fi

for name in $expected; do
 case "$name" in
 gojson-strings.txt | gojson-reader.txt | recordscan-go.txt)
  if [ "$version" != go1.27.0 ]; then
   echo "left $name alone: this is $version, it needs go1.27.0"
   continue
  fi
  ;;
 esac
 cp "$new/out/$name" "$here/$name"
done
cp "$new/src/goquote_table.rs" "$root/rust/crates/dostoevsky-format/src/goquote_table.rs"
