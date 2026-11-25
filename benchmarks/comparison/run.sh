#!/bin/sh
# Compare engines on the publisher gate's workload, one trial at a time.
# Run from the project root on the benchmark host with TMPDIR on the data disk.
#   BENCH_ENGINES="dostoevsky jetstream sqlite redis valkey" \
#   REDIS_BIN=/opt/bench/redis-8.12.0/src/redis-server \
#   VALKEY_BIN=/opt/bench/valkey-9.2.0/bin/valkey-server \
#   KAFKA_HOME=/opt/bench/kafka_2.13-4.4.0 JAVA_HOME=/opt/bench/jdk-21 \
#   RABBIT_HOME=/opt/bench/rabbitmq_server-4.4.1 \
#   REDPANDA_HOME=/opt/bench/redpanda-26.3.1/opt/redpanda [REDPANDA_LOADER=ld] [REDPANDA_MEMORY=1G] \
#   sh benchmarks/comparison/run.sh
# A smoke pass (one 200-record trial per engine and producer count) runs first and stops
# the run with exit 3 if anything fails. Every trial prints one receipt line. A trial that fails is printed as a
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

# Evidence lives in ONE directory per run, BENCH_EVIDENCE_DIR (default: a new directory named
# for this run). It must be new or empty, so keys from an earlier run can never mix with this
# one. Every invocation's stderr is appended, under its trial key, to stderr-all.log there, and
# the engines that write raw readbacks (redpanda) keep them there, keyed by the same
# BENCH_TRIAL_KEY. A write that fails is fatal (exit 4): a run that cannot keep its evidence
# does not continue. Nothing is overwritten: these files are part of the receipt.
mkdir -p .dev/embedded
BENCH_EVIDENCE_DIR="${BENCH_EVIDENCE_DIR:-.dev/embedded/evidence-$(date -u +%Y%m%dT%H%M%SZ)-$$}"; export BENCH_EVIDENCE_DIR
mkdir -p "$BENCH_EVIDENCE_DIR" || exit 4
if [ -n "$(ls -A "$BENCH_EVIDENCE_DIR" 2>/dev/null)" ]; then
 echo "BENCH_EVIDENCE_DIR $BENCH_EVIDENCE_DIR is not empty: a run needs a fresh evidence directory" >&2
 exit 4
fi
keep_stderr() { # key file
 { printf '== %s\n' "$1"; cat "$2"; } >> "$BENCH_EVIDENCE_DIR/stderr-all.log" || { echo "cannot write stderr evidence for $1 to $BENCH_EVIDENCE_DIR" >&2; exit 4; }
}
run_engine() { # engine workers
 case "$1" in
  dostoevsky) .dev/embedded/dost -workers "$2" -count "$count" ;;
  jetstream) .dev/embedded/jet -workers "$2" -count "$count" ;;
  sqlite) .dev/embedded/sqlite -workers "$2" -count "$count" ;;
  redis) BENCH_ENGINE_LABEL=redis BENCH_SERVER_BIN="${REDIS_BIN:?set REDIS_BIN}" .dev/embedded/redis -workers "$2" -count "$count" ;;
  valkey) BENCH_ENGINE_LABEL=valkey BENCH_SERVER_BIN="${VALKEY_BIN:?set VALKEY_BIN}" .dev/embedded/redis -workers "$2" -count "$count" ;;
  kafka) BENCH_ENGINE_LABEL=kafka BENCH_KAFKA_HOME="${KAFKA_HOME:?set KAFKA_HOME}" BENCH_JAVA_HOME="${JAVA_HOME:?set JAVA_HOME}" .dev/embedded/kafka -workers "$2" -count "$count" ;;
  redpanda) BENCH_ENGINE_LABEL=redpanda BENCH_REDPANDA_HOME="${REDPANDA_HOME:?set REDPANDA_HOME}" BENCH_REDPANDA_LOADER="${REDPANDA_LOADER:-}" BENCH_REDPANDA_MEMORY="${REDPANDA_MEMORY:-1G}" BENCH_REDPANDA_OVERPROVISIONED="${REDPANDA_OVERPROVISIONED:-0}" .dev/embedded/redpanda -workers "$2" -count "$count" ;;
  rabbitmq) BENCH_ENGINE_LABEL=rabbitmq BENCH_RABBIT_HOME="${RABBIT_HOME:?set RABBIT_HOME}" .dev/embedded/rabbit -workers "$2" -count "$count" ;;
  *) echo "unknown engine $1" >&2; return 2 ;;
 esac
}

# Smoke first: one 200-record trial per engine and producer count, before any timed
# trial. A failed smoke stops the run (exit 3) with nothing on stdout, so a harness or
# installation problem is found in minutes, not after a full run. Smoke lines go to
# stderr and are not part of the receipt. BENCH_SKIP_SMOKE=1 skips it.
if [ "${BENCH_SKIP_SMOKE:-0}" != 1 ]; then
 saved_count=$count
 count=${BENCH_SMOKE_COUNT:-200}
 smoke_failed=0
 for e in $engines; do
  for w in 1 8 32; do
   BENCH_TRIAL_KEY="smoke-$e-w$w"; export BENCH_TRIAL_KEY
   line=$(run_engine "$e" "$w" 2>.dev/embedded/smoke-error.log); rc=$?
   keep_stderr "$BENCH_TRIAL_KEY" .dev/embedded/smoke-error.log
   if [ "$rc" -eq 0 ]; then
    printf 'smoke %s\n' "$line" >&2
   else
    smoke_failed=$((smoke_failed + 1))
    printf 'smoke FAILED engine=%s workers=%s %s\n' "$e" "$w" "$(tr '\n' ' ' < .dev/embedded/smoke-error.log | cut -c1-200)" >&2
   fi
  done
 done
 count=$saved_count
 if [ "$smoke_failed" -ne 0 ]; then
  echo "smoke failed ($smoke_failed trials): no timed trial was run" >&2
  exit 3
 fi
fi

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
   BENCH_TRIAL_KEY="trial$rep-$e-w$w"; export BENCH_TRIAL_KEY
   line=$(run_engine "$e" "$w" 2>.dev/embedded/last-error.log); rc=$?
   keep_stderr "$BENCH_TRIAL_KEY" .dev/embedded/last-error.log
   if [ "$rc" -eq 0 ]; then
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
