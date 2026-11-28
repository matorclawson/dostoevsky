#!/bin/sh
set -eu
# Supply a module with the Ephemeral SQLite release and its dependencies vendored.
: "${EPHEMERAL_REFERENCE_MODULE:?set to an existing Go module vendoring ephemeral}"
base=$(cd "$EPHEMERAL_REFERENCE_MODULE" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$base/go.mod" "$base/go.sum" "$tmp/"
ln -s "$base/vendor" "$tmp/vendor"
sed 's#"github.com/rodriveracom/dostoevsky/workflow"#"ephemeral"#' workflow/conformance_test.go > "$tmp/conformance_test.go"
(cd "$tmp" && GOPROXY=off "${GO:-go}" test -mod=vendor -count=1 -v .)
