#!/bin/sh
# Proves at the real boundary that an unmodified 0.2.3 binary refuses directories this
# build has opened. Run from the repository root on a build host with git and go.
#   sh scripts/fence-check.sh [OLD_REV]       (default OLD_REV: d9d4cbc5, release 0.2.3)
set -eu
old=${1:-d9d4cbc5}
root=$(pwd -P)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
mkdir "$tmp/src" "$tmp/fixture"
git archive "$old" | tar -x -C "$tmp/src"
(cd "$tmp/src" && CGO_ENABLED=0 GOFLAGS=-mod=mod go build -o "$tmp/wordcount-old" ./examples/wordcount)
# Control: the old binary works on a directory it created itself.
"$tmp/wordcount-old" "$tmp/legacy" blue red blue >/dev/null
# This build: a fresh directory swapped twice by Retain, and the legacy directory opened once.
(cd "$root" && DOST_FENCE_DIR="$tmp/fixture" DOST_FENCE_LEGACY="$tmp/legacy" mkdir -p "$tmp/fixture/retained" && \
  DOST_FENCE_DIR="$tmp/fixture" DOST_FENCE_LEGACY="$tmp/legacy" CGO_ENABLED=0 go test -count=1 -run '^TestFenceFixture$' .)
sum() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }
fail=0
for dir in "$tmp/fixture/retained" "$tmp/legacy"; do
  before=$(sum "$dir/journal.v1")
  if out=$("$tmp/wordcount-old" "$dir" blue 2>&1); then
    echo "FAIL: 0.2.3 opened $dir: $out"; fail=1; continue
  fi
  after=$(sum "$dir/journal.v1")
  [ "$before" = "$after" ] || { echo "FAIL: 0.2.3 modified $dir"; fail=1; continue; }
  case "$out" in *"invalid journal header"*) echo "PASS $(basename "$dir"): $out";; *) echo "FAIL: unexpected error for $dir: $out"; fail=1;; esac
done
exit $fail
