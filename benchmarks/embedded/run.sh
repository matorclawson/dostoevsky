#!/bin/sh
set -eu
# Run from the project root. Keep experimental dependencies in this module.
(cd benchmarks/embedded && CGO_ENABLED=0 "${GO:-go}" build -buildvcs=false -o ../../.dev/embedded/ ./cmd/...)
export GOMAXPROCS="${GOMAXPROCS:-2}"
if [ "${BENCH_SUITE:-}" = gate ]; then
 exec sh benchmarks/embedded/gate.sh
fi
if [ "${BENCH_SUITE:-}" = concurrent ]; then
 exec sh benchmarks/embedded/concurrent.sh
fi
for order in 'dost jet' 'jet dost' 'dost jet'; do
 for workers in 1 8; do
  for engine in $order; do
   ".dev/embedded/$engine" -workers "$workers" -count "${BENCH_COUNT:-2000}"
  done
 done
done
