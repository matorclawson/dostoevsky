#!/bin/sh
set -eu
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
go test -c -o "$tmp/workload" .
mkdir "$tmp/base" "$tmp/original"
if [ -n "${BASELINE_DIR:-}" ]; then
 cp -R "$BASELINE_DIR/." "$tmp/original/"
else
 git archive 1745c8a | tar -x -C "$tmp/original"
fi
cp admission_bench_test.go "$tmp/original/"
(cd "$tmp/original" && go test -c -o "$tmp/baseline" .)
export GOMAXPROCS=${GOMAXPROCS:-4}
DOST_ADMISSION_DIR="$tmp/base" DOST_MODE=seed "$tmp/workload" -test.run '^TestAdmissionWorkload$' >/dev/null
for rep in $(seq 1 "${REPEATS:-3}"); do
 policies="original fifo frequency"
 if [ $((rep % 2)) -eq 0 ]; then policies="frequency fifo original"; fi
 for mode in mixed shift uniform append; do
 for workers in 1 32; do
 for policy in $policies; do
 cp -R "$tmp/base" "$tmp/run"
 binary="$tmp/workload"
 if [ "$policy" = original ]; then binary="$tmp/baseline"; fi
 DOST_ADMISSION_DIR="$tmp/run" DOST_MODE="$mode" DOST_WORKERS="$workers" DOST_POLICY="$policy" "$binary" -test.run '^TestAdmissionWorkload$'
 rm -rf "$tmp/run"
 done
 done
 done
done
