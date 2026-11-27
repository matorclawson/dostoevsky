#!/bin/sh
set -eu
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/old" "$tmp/base"
if [ -n "${BASELINE_DIR:-}" ]; then cp -R "$BASELINE_DIR/." "$tmp/old/"
else git archive 8e410b7 | tar -x -C "$tmp/old"; fi
cp fresh_bench_test.go "$tmp/old/"
export GOMAXPROCS=${GOMAXPROCS:-4}
(cd "$tmp/old" && go test -c -o "$tmp/baseline" .)
go test -c -o "$tmp/candidate" .
DOST_ADMISSION_DIR="$tmp/base" DOST_MODE=seed "$tmp/baseline" -test.run '^TestAdmissionWorkload$' >/dev/null
for rep in $(seq 1 "${REPEATS:-5}"); do
 order="baseline candidate"
 if [ $((rep%2)) -eq 0 ]; then order="candidate baseline"; fi
 for workers in 1 32; do
 for size in 0 1024; do
 for policy in $order; do
 cp -R "$tmp/base" "$tmp/run"
 DOST_FRESH_DIR="$tmp/run" DOST_WORKERS="$workers" DOST_PAYLOAD="$size" "$tmp/$policy" -test.run '^TestFreshPublishCell$' > "$tmp/result"
 awk -v rep="$rep" -v version="$policy" '$1=="CELL" {print rep,version,$0}' "$tmp/result"
 rm -rf "$tmp/run"
 done
 done
 done
done
