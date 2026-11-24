#!/bin/sh
# Compare engines on the publisher gate's workload, one trial at a time.
# Run from the project root on the benchmark host with TMPDIR on the data disk.
#   BENCH_ENGINES="dostoevsky jetstream sqlite redis valkey" \
#   REDIS_BIN=/opt/bench/redis-8.12.0/src/redis-server \
#   VALKEY_BIN=/opt/bench/valkey-9.2.0/bin/valkey-server \
#   sh benchmarks/comparison/run.sh
# Every trial prints one receipt line. A trial that fails is printed as a
# FAILED line and counted, never retried, and the script exits 1 at the end.
set -u
engines=${BENCH_ENGINES:?set BENCH_ENGINES to a space separated list}
count=${BENCH_COUNT:-16000}
rounds=${BENCH_ROUNDS:-5}
# -mod=readonly: the build may not change go.mod or go.sum. It does write to the Go
# module and build caches (GOMODCACHE, GOCACHE) and the binaries to .dev/embedded.
(cd benchmarks/embedded && CGO_ENABLED=0 "${GO:-go}" build -mod=readonly -buildvcs=false -o ../../.dev/embedded/ ./cmd/...) || exit 2
export GOMAXPROCS="${GOMAXPROCS:-2}"
failed=0

run_engine() { # engine workers
 case "$1" in
  dostoevsky) .dev/embedded/dost -workers "$2" -count "$count" ;;
  jetstream) .dev/embedded/jet -workers "$2" -count "$count" ;;
  sqlite) .dev/embedded/sqlite -workers "$2" -count "$count" ;;
  redis) BENCH_ENGINE_LABEL=redis BENCH_SERVER_BIN="${REDIS_BIN:?set REDIS_BIN}" .dev/embedded/redis -workers "$2" -count "$count" ;;
  valkey) BENCH_ENGINE_LABEL=valkey BENCH_SERVER_BIN="${VALKEY_BIN:?set VALKEY_BIN}" .dev/embedded/redis -workers "$2" -count "$count" ;;
  *) echo "unknown engine $1" >&2; return 2 ;;
 esac
}

rep=1
while [ "$rep" -le "$rounds" ]; do
 # Rotate the starting engine each round so no engine always runs first or last.
 set -- $engines
 skip=$(( (rep - 1) % $# ))
 order=""
 i=0
 for e in "$@"; do
  if [ "$i" -ge "$skip" ]; then order="$order $e"; fi
  i=$((i + 1))
 done
 i=0
 for e in "$@"; do
  if [ "$i" -lt "$skip" ]; then order="$order $e"; fi
  i=$((i + 1))
 done
 for w in 1 8 32; do
  for e in $order; do
   if line=$(run_engine "$e" "$w" 2>.dev/embedded/last-error.log); then
    printf 'trial=%s %s\n' "$rep" "$line"
   else
    failed=$((failed + 1))
    printf 'trial=%s engine=%s workers=%s FAILED %s\n' "$rep" "$e" "$w" "$(tr '\n' ' ' < .dev/embedded/last-error.log | cut -c1-200)"
   fi
  done
 done
 rep=$((rep + 1))
done
[ "$failed" -eq 0 ]
