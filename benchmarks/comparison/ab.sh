#!/bin/sh
# Alternate two builds of the Dostoevsky benchmark binary on the publisher gate's
# workload, so a change is measured against its baseline in the same window.
#   sh benchmarks/comparison/ab.sh BASELINE_BIN CANDIDATE_BIN
# Build each binary from its own checkout, for example:
#   (cd benchmarks/embedded && CGO_ENABLED=0 go build -mod=readonly -o /path/dost-baseline ./cmd/dost)
# Run from the project root with TMPDIR on the data disk. Prints one receipt line per
# trial, labeled engine=baseline or engine=candidate, five rounds at 1, 8 and 32
# producers with the order alternated each round; a failed trial is printed as a
# FAILED line and never retried. Summarize with
#   go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -trials 5
set -u
base=${1:?usage: ab.sh BASELINE_BIN CANDIDATE_BIN}
cand=${2:?usage: ab.sh BASELINE_BIN CANDIDATE_BIN}
count=${BENCH_COUNT:-16000}
export GOMAXPROCS="${GOMAXPROCS:-2}"
failed=0
trial() { # label binary workers round
 if line=$("$2" -workers "$3" -count "$count" 2>/dev/null); then
  printf 'trial=%s %s\n' "$4" "$(printf '%s' "$line" | sed "s/^engine=[a-z]*/engine=$1/")"
 else
  failed=$((failed + 1))
  printf 'trial=%s engine=%s workers=%s FAILED\n' "$4" "$1" "$3"
 fi
}
rep=1
while [ "$rep" -le 5 ]; do
 for w in 1 8 32; do
  if [ $((rep % 2)) -eq 1 ]; then
   trial baseline "$base" "$w" "$rep"; trial candidate "$cand" "$w" "$rep"
  else
   trial candidate "$cand" "$w" "$rep"; trial baseline "$base" "$w" "$rep"
  fi
 done
 rep=$((rep + 1))
done
[ "$failed" -eq 0 ]
