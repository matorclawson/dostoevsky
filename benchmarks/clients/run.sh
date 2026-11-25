#!/bin/sh
# The language client benchmarks, one trial at a time, in the same shape as benchmarks/comparison/run.sh:
# a smoke pass first (one short trial per cell and producer count, stopping the run with exit 3 if anything
# fails), then five rounds with the starting cell rotated each round, at 1, 8 and 32 producers. A cell is one
# language against one engine. Every trial prints one receipt line; a trial that fails is printed as a FAILED
# line, counted and NEVER retried, and the script exits 1 at the end.
#
# Run from the repository root, on the benchmark host, after building (see benchmarks/clients/README.md):
#   CLIENT_LANGS="python javascript java" CLIENT_ENGINES="dostoevsky kafka nats redis" \
#   BENCH_NATS_SERVER=... BENCH_REDIS_SERVER=... BENCH_GATEWAY_SERVER=... BENCH_KAFKA_HOME=... BENCH_JAVA_HOME=... \
#   BENCH_TARGET=16000 BENCH_WINDOW=0 sh benchmarks/clients/run.sh > receipt.log
#
# The server for each trial is started by benchmarks/embedded/cmd/serve (the same code the Go comparison uses),
# which prints "READY addr= pid= token=". This script reads addr and token into variables, hands the token to the
# worker through the environment (BENCH_TOKEN), and never prints, logs or passes it on a command line; no
# `set -x`, and the serve output file lives in a private directory and is removed after the trial.
set -u
langs=${CLIENT_LANGS:-python javascript java}
engines=${CLIENT_ENGINES:-dostoevsky kafka nats redis}
target=${BENCH_TARGET:-16000}
window=${BENCH_WINDOW:-0}
rounds=${BENCH_ROUNDS:-5}
serve=${SERVE_BIN:-.dev/clients/serve}
failed=0
umask 077

serve_engine() { # receipt engine -> the name serve knows it by
  case "$1" in dostoevsky) echo gateway ;; *) echo "$1" ;; esac
}

worker_cmd() { # lang engine addr workers count window -> the command line (token comes from BENCH_TOKEN)
  l=$1; e=$2; a=$3; w=$4; c=$5; win=$6
  override=$(eval "echo \"\${WORKER_CMD_$l:-}\"")
  if [ -n "$override" ]; then echo "$override --engine $e --addr $a --workers $w --target $c --window $win"; return; fi
  case "$l" in
    python) echo "${PYTHON_BIN:-.dev/bench-venv/bin/python} benchmarks/clients/python/worker.py --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    javascript) echo "node benchmarks/clients/javascript/worker.mjs --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    java) echo "java -Xmx${JAVA_HEAP:-256m} -Dbench.deps=benchmarks/clients/java/deps.lock -Dbench.repo=. -cp '.dev/bench-java/classes:.dev/bench-java/lib/*' io.github.rodriveracom.dostoevsky.bench.Main --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    *) echo "" ;;
  esac
}

field() { # key line -> the value of key=value (first match), empty if absent
  printf '%s\n' "$2" | tr ' ' '\n' | sed -n "s/^$1=//p" | head -1
}

# run_cell lang engine workers count window trial  -> prints one line on stdout; returns 0 ok, 1 failed
run_cell() {
  l=$1; e=$2; w=$3; c=$4; win=$5; n=$6
  se=$(serve_engine "$e")
  d=$(mktemp -d "${TMPDIR:-/tmp}/clients-trial.XXXXXX") || { echo "trial=$n engine=$l-$e workers=$w FAILED no_temp_dir"; return 1; }
  mkfifo "$d/in"
  "$serve" -engine "$se" < "$d/in" > "$d/out" 2> "$d/err" &
  spid=$!
  exec 3> "$d/in" # holding this open keeps the server running; closing it stops it
  i=0; ready=""
  while [ $i -lt "${READY_WAIT:-3600}" ]; do # tenths of a second (6 minutes): Kafka needs minutes
    ready=$(grep -m1 '^READY ' "$d/out" 2>/dev/null) && [ -n "$ready" ] && break
    kill -0 "$spid" 2>/dev/null || break
    sleep 0.1; i=$((i + 1))
  done
  if [ -z "$ready" ]; then
    exec 3>&-; wait "$spid" 2>/dev/null
    echo "trial=$n engine=$l-$e workers=$w FAILED server_did_not_become_ready"
    rm -f "$d/in" "$d/out" "$d/err"; rmdir "$d" 2>/dev/null
    return 1
  fi
  addr=$(field addr "$ready"); token=$(field token "$ready")
  cmd=$(worker_cmd "$l" "$e" "$addr" "$w" "$c" "$win")
  line=""; code=0
  if [ -z "$cmd" ]; then code=2; else
    line=$(BENCH_TOKEN="$token" sh -c "$cmd" 2> "$d/werr") || code=$?
  fi
  token=""; ready=""
  exec 3>&- # the server's stdin closes: it stops, prints STOPPED
  wait "$spid"; scode=$?
  stopped=$(grep -m1 '^STOPPED ' "$d/out" 2>/dev/null)
  rc=0
  if [ $code -ne 0 ] || [ -z "$line" ]; then
    echo "trial=$n engine=$l-$e workers=$w FAILED worker_exit_$code"
    rc=1
  elif [ $scode -ne 0 ] || [ -z "$stopped" ]; then
    echo "trial=$n engine=$l-$e workers=$w FAILED server_did_not_stop_cleanly"
    rc=1
  elif [ "$(field result "$line")" = "FAILED" ]; then
    # Converted to the marker cmd/plots counts as a failed trial: everything after FAILED is a diagnostic.
    echo "trial=$n engine=$(field engine "$line") workers=$w FAILED stop=$(field stop "$line") $(field detail "$line")"
    rc=1
  else
    srss=$(field server_rss_KiB "$stopped"); crss=$(field client_rss_KiB "$line")
    case "$srss$crss" in *[!0-9]*|"") echo "trial=$n engine=$l-$e workers=$w FAILED memory_not_reported"; rc=1 ;; *)
      echo "trial=$n $line server_rss_KiB=$srss server_rss_source=$(field rss_source "$stopped") total_rss_KiB=$((srss + crss))" ;;
    esac
  fi
  rm -f "$d/in" "$d/out" "$d/err" "$d/werr"; rmdir "$d" 2>/dev/null
  return $rc
}

cells=""
for l in $langs; do for e in $engines; do cells="$cells $l:$e"; done; done
[ -x "$serve" ] || { echo "no server driver at $serve: build it first (benchmarks/clients/README.md)" >&2; exit 2; }

# Smoke first, before any timed trial.
if [ "${BENCH_SKIP_SMOKE:-0}" != 1 ]; then
  smoke_failed=0
  for cell in $cells; do
    for w in 1 8 32; do
      out=$(run_cell "${cell%%:*}" "${cell#*:}" "$w" "${BENCH_SMOKE_COUNT:-200}" 0 0) || { smoke_failed=$((smoke_failed + 1)); printf 'smoke FAILED %s\n' "$out" >&2; continue; }
      printf 'smoke ok %s %s\n' "$cell" "$w" >&2
    done
  done
  if [ $smoke_failed -ne 0 ]; then echo "smoke failed ($smoke_failed trials): no timed trial was run" >&2; exit 3; fi
fi

rep=1
set -- $cells
ncells=$#
while [ $rep -le "$rounds" ]; do
  skip=$(( (rep - 1) % ncells )); order=""; i=0
  for cell in "$@"; do [ $i -ge $skip ] && order="$order $cell"; i=$((i + 1)); done
  i=0
  for cell in "$@"; do [ $i -lt $skip ] && order="$order $cell"; i=$((i + 1)); done
  for w in 1 8 32; do
    for cell in $order; do
      run_cell "${cell%%:*}" "${cell#*:}" "$w" "$target" "$window" "$rep" || failed=$((failed + 1))
    done
  done
  rep=$((rep + 1))
done
[ $failed -eq 0 ]
