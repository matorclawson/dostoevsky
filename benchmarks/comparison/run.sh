#!/bin/sh
# Compare engines on the publisher gate's workload, one trial at a time.
# Run from the project root on the benchmark host with TMPDIR on the data disk.
#   BENCH_ENGINES="dostoevsky jetstream sqlite redis valkey" \
# (gateway: the control for the language client benchmarks. Chart label to use in the plots meta:
#  label.gateway=gateway end to end (HTTP, process boundary); the gateway as the language cells get it, unpinned; the equal-budget variant, harness and gateway each GOMAXPROCS=1, is the separate row gateway-pinned1)
#   REDIS_BIN=/opt/bench/redis-8.12.0/src/redis-server \
#   VALKEY_BIN=/opt/bench/valkey-9.2.0/bin/valkey-server \
#   KAFKA_HOME=/opt/bench/kafka_2.13-4.4.0 JAVA_HOME=/opt/bench/jdk-21 \
#   RABBIT_HOME=/opt/bench/rabbitmq_server-4.4.1 \
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
# The gateway control engine needs the gateway binary, built from the same checkout.
case " $engines " in
 *" gateway "*|*" gateway-pinned1 "*)
  if [ -z "${GATEWAY_BIN:-}" ]; then
   (CGO_ENABLED=0 "${GO:-go}" build -mod=readonly -buildvcs=false -o .dev/embedded/dostoevsky-gateway ./cmd/dostoevsky-gateway) || exit 2
  fi
  # The gateway binary's identity goes on record whether it was built here or overridden.
  gw="${GATEWAY_BIN:-.dev/embedded/dostoevsky-gateway}"
  if command -v sha256sum >/dev/null 2>&1; then gwsum=$(sha256sum "$gw" | cut -d' ' -f1); else gwsum=$(shasum -a 256 "$gw" | cut -d' ' -f1); fi
  # A digest that could not be computed must stop the run, never print an empty one.
  case "$gwsum" in
   [0-9a-f]????????????????????????????????????????????????????????????????) ;;
   *) echo "cannot compute the sha256 of $gw" >&2; exit 2 ;;
  esac
  printf 'gateway_bin=%s sha256=%s\n' "$gw" "$gwsum" | tee .dev/embedded/gateway.sha256 >&2 ;;
esac
export GOMAXPROCS="${GOMAXPROCS:-2}"
failed=0

run_engine() { # engine workers
 case "$1" in
  dostoevsky) .dev/embedded/dost -workers "$2" -count "$count" ;;
  jetstream) .dev/embedded/jet -workers "$2" -count "$count" ;;
  gateway) BENCH_SERVER_BIN="${GATEWAY_BIN:-.dev/embedded/dostoevsky-gateway}" .dev/embedded/gateway -workers "$2" -count "$count" ;;
  gateway-pinned1) GOMAXPROCS=1 BENCH_GATEWAY_GOMAXPROCS=1 BENCH_SERVER_BIN="${GATEWAY_BIN:-.dev/embedded/dostoevsky-gateway}" .dev/embedded/gateway -workers "$2" -count "$count" ;;
  sqlite) .dev/embedded/sqlite -workers "$2" -count "$count" ;;
  redis) BENCH_ENGINE_LABEL=redis BENCH_SERVER_BIN="${REDIS_BIN:?set REDIS_BIN}" .dev/embedded/redis -workers "$2" -count "$count" ;;
  valkey) BENCH_ENGINE_LABEL=valkey BENCH_SERVER_BIN="${VALKEY_BIN:?set VALKEY_BIN}" .dev/embedded/redis -workers "$2" -count "$count" ;;
  kafka) BENCH_ENGINE_LABEL=kafka BENCH_KAFKA_HOME="${KAFKA_HOME:?set KAFKA_HOME}" BENCH_JAVA_HOME="${JAVA_HOME:?set JAVA_HOME}" .dev/embedded/kafka -workers "$2" -count "$count" ;;
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
   if line=$(run_engine "$e" "$w" 2>.dev/embedded/smoke-error.log); then
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
