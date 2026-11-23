#!/bin/sh
# run.sh builds both measured engines and sets GOMAXPROCS.
set -eu

run_trial() {
 engine=$1
 case "$engine" in
 baseline) [ -n "${BENCH_BASELINE:-}" ] || return 0; binary=$BENCH_BASELINE ;;
 *) binary=".dev/embedded/$engine" ;;
 esac
 line=$("$binary" -workers "$2" -count "$3")
 printf 'binary=%s %s\n' "$engine" "$line"
}

for order in 'baseline dost jet' 'dost jet baseline' 'jet baseline dost' 'jet dost baseline' 'baseline jet dost'; do
 for workers in 1 8 32; do
  for engine in $order; do run_trial "$engine" "$workers" 4000; done
 done
done
for order in 'dost jet' 'jet dost' 'dost jet'; do
 for workers in 8 32; do
  for engine in $order; do run_trial "$engine" "$workers" 32000; done
 done
done
if [ -n "${BENCH_BASELINE:-}" ]; then
 for order in 'baseline dost' 'dost baseline' 'baseline dost'; do
  for engine in $order; do run_trial "$engine" 1 16000; done
 done
fi
