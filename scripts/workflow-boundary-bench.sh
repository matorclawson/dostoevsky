#!/bin/sh
set -eu
: "${DOST_WORKFLOW_BENCH_DIR:?supply a disposable directory on the test volume}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/base"
git archive bcc0238 | tar -x -C "$tmp/base"
export GOMAXPROCS=${GOMAXPROCS:-4}
(cd "$tmp/base" && "${GO:-go}" test -c -o "$tmp/baseline" ./workflow)
"${GO:-go}" test -c -o "$tmp/candidate" ./workflow
for trial in $(seq 1 "${REPEATS:-3}"); do
 order='baseline candidate'
 if [ $((trial%2)) -eq 0 ]; then order='candidate baseline'; fi
 for build in $order; do
 "$tmp/$build" -test.v -test.run '^TestWorkflowCost$' > "$tmp/result"
 awk -v r="$trial" -v v="$build" '/workers=/ {print r,v,$0}' "$tmp/result"
 done
done
