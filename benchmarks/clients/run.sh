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
# Every number that bounds a wait must be a CANONICAL whole number of at most seven digits (no sign, no leading zero,
# no decimal point, no letters, nothing past the shell's integer range), or the script refuses to start: a value like
# "ten" would otherwise turn a deadline into no deadline, "08" is read as an invalid octal number inside $(( )), and a
# 25-digit number overflows the arithmetic the deadline test uses. Only BENCH_WINDOW may be 0 (no window).
canonical_seconds() { # name value allow_zero
  case "$2" in
    0) [ "$3" = yes ] && return 0 ;;
    [1-9]) return 0 ;;
    [1-9][0-9]|[1-9][0-9][0-9]|[1-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9][0-9][0-9]) return 0 ;;
  esac
  echo "$1 must be a whole number from $([ "$3" = yes ] && echo 0 || echo 1) to 9999999 without a sign or leading zeros, not '$2'" >&2
  exit 2
}
canonical_seconds BENCH_WINDOW "$window" yes
[ -z "${WORKER_MAX+x}" ] || canonical_seconds WORKER_MAX "$WORKER_MAX" no
[ -z "${STOP_WAIT+x}" ] || canonical_seconds STOP_WAIT "$STOP_WAIT" no
[ -z "${READY_WAIT+x}" ] || canonical_seconds READY_WAIT "$READY_WAIT" no

serve_engine() { # receipt engine -> the name serve knows it by
  case "$1" in dostoevsky) echo gateway ;; *) echo "$1" ;; esac
}

worker_cmd() { # lang engine addr workers count window -> the command line (token comes from BENCH_TOKEN)
  l=$1; e=$2; a=$3; w=$4; c=$5; win=$6
  override=$(eval "echo \"\${WORKER_CMD_$l:-}\"")
  if [ -n "$override" ]; then echo "$override --engine $e --addr $a --workers $w --target $c --window $win"; return; fi
  case "$l" in
    python) echo "env PYTHONPATH=clients/python ${PYTHON_BIN:-.dev/bench-venv/bin/python} benchmarks/clients/python/worker.py --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    javascript) echo "node benchmarks/clients/javascript/worker.mjs --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    java) echo "java -Xmx${JAVA_HEAP:-256m} -Dbench.deps=benchmarks/clients/java/deps.lock -Dbench.repo=. -cp '.dev/bench-java/classes:.dev/bench-java/lib/*' io.github.rodriveracom.dostoevsky.bench.Main --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    rust) echo "env BENCH_REPO=. benchmarks/clients/rust/worker/target/release/worker --engine $e --addr $a --workers $w --target $c --window $win --require-known-build" ;;
    *) echo "" ;;
  esac
}

field() { # key line -> the value of key=value (first match), empty if absent
  printf '%s\n' "$2" | tr ' ' '\n' | sed -n "s|^$1=||p" | head -1
}

# ---- cleanup: whatever ends the script (a TERM, an INT, a normal exit), the trial's server is stopped and its directory
# ---- (which holds the token) is removed. Every wait is bounded: a hung server is killed, never waited for forever.
CUR_DIR=""; CUR_SPID=""; CUR_WPID=""; CUR_EPID=""

stop_pid() { # pid seconds: TERM, wait up to `seconds`, then KILL; returns at once if it is already gone
  p=$1; secs=${2:-10}
  [ -n "$p" ] || return 0
  kill -TERM "$p" 2>/dev/null || { wait "$p" 2>/dev/null; return 0; }
  i=0
  while kill -0 "$p" 2>/dev/null && [ $i -lt $((secs * 10)) ]; do sleep 0.1; i=$((i + 1)); done
  if kill -0 "$p" 2>/dev/null; then kill -KILL "$p" 2>/dev/null; fi
  wait "$p" 2>/dev/null
  return 0
}

# kill_engine_group: serve starts its engine in a process group of its own whose id is the engine's pid (READY's pid=).
# Killing a hung serve alone would orphan the engine, so the whole group goes too.
kill_engine_group() {
  case "$CUR_EPID" in ""|*[!0-9]*|0|1) return 0 ;; esac   # never a group id that could mean "everything"
  kill -KILL -- "-$CUR_EPID" 2>/dev/null
  return 0
}

cleanup_trial() {
  exec 3>&-
  stop_pid "$CUR_WPID" 5
  stop_pid "$CUR_SPID" "${STOP_WAIT:-120}"
  kill_engine_group
  if [ -n "$CUR_DIR" ]; then rm -f "$CUR_DIR/in" "$CUR_DIR/out" "$CUR_DIR/err" "$CUR_DIR/wout" "$CUR_DIR/werr"; rmdir "$CUR_DIR" 2>/dev/null; fi
  CUR_DIR=""; CUR_SPID=""; CUR_WPID=""; CUR_EPID=""
}
trap 'cleanup_trial; trap - EXIT; exit 143' TERM
trap 'cleanup_trial; trap - EXIT; exit 130' INT
trap cleanup_trial EXIT

# is_decimal x: a non-negative decimal such as 12 or 2002.301 (not empty, not NaN, not 1e3, not -1, not .5, not 5.)
is_decimal() {
  case "$1" in ""|*[!0-9.]*|.*|*.|*.*.*) return 1 ;; esac
  return 0
}

# mask_token token text: text with every occurrence of the token replaced by _token_, done by the shell itself, so the
# token is never an argument of any other program (ps could read it there).
mask_token() {
  mt_t=$1; mt_s=$2; mt_out=""
  if [ -z "$mt_t" ]; then printf '%s' "$mt_s"; return; fi
  while :; do
    case $mt_s in
      *"$mt_t"*) mt_out="$mt_out${mt_s%%"$mt_t"*}_token_"; mt_s=${mt_s#*"$mt_t"} ;;
      *) mt_out="$mt_out$mt_s"; break ;;
    esac
  done
  printf '%s' "$mt_out"
}

# valid_receipt line lang engine workers -> prints the name of the first thing wrong, nothing when the line is whole
valid_receipt() {
  v_line=$1; v_want_engine=$2-$3; v_want_workers=$4
  for k in engine workers result acked verified records/s elapsed_s p50_us p95_us p99_us p99_9_us p99_99_us quantiles client_rss_KiB client_rss_source lang client client_build stop; do
    [ -n "$(field "$k" "$v_line")" ] || { echo "missing_$(echo "$k" | tr '/' '_')"; return; }
  done
  for k in workers acked verified client_rss_KiB; do
    case "$(field "$k" "$v_line")" in *[!0-9]*) echo "not_a_number_$k"; return ;; esac
  done
  for k in records/s elapsed_s p50_us p95_us p99_us p99_9_us p99_99_us; do
    is_decimal "$(field "$k" "$v_line")" || { echo "not_a_decimal_$(echo "$k" | tr '/' '_')"; return; }
  done
  [ "$(field workers "$v_line")" = "$v_want_workers" ] || { echo "wrong_workers"; return; }
  [ "$(field engine "$v_line")" = "$v_want_engine" ] || { echo "wrong_engine"; return; }
  case "$(field result "$v_line")" in ok|FAILED) ;; *) echo "bad_result"; return ;; esac
  [ "$(field quantiles "$v_line")" = "bin-upper-1pct" ] || { echo "bad_quantiles"; return; }
}

# run_cell lang engine workers count window trial: sets RESULT to one line, returns 0 when the trial succeeded.
run_cell() {
  l=$1; e=$2; w=$3; c=$4; win=$5; n=$6
  se=$(serve_engine "$e")
  d=$(mktemp -d "${TMPDIR:-/tmp}/clients-trial.XXXXXX") || { RESULT="trial=$n engine=$l-$e workers=$w FAILED no_temp_dir"; return 1; }
  CUR_DIR=$d
  mkfifo "$d/in"
  "$serve" -engine "$se" < "$d/in" > "$d/out" 2> "$d/err" &
  spid=$!; CUR_SPID=$spid
  exec 3> "$d/in" # holding this open keeps the server running; closing it stops it
  i=0; ready=""
  while [ $i -lt "${READY_WAIT:-3600}" ]; do # tenths of a second (6 minutes): Kafka needs minutes
    ready=$(grep -m1 '^READY ' "$d/out" 2>/dev/null) && [ -n "$ready" ] && break
    kill -0 "$spid" 2>/dev/null || break
    sleep 0.1; i=$((i + 1))
  done
  if [ -z "$ready" ]; then
    RESULT="trial=$n engine=$l-$e workers=$w FAILED server_did_not_become_ready"
    cleanup_trial
    return 1
  fi
  addr=$(field addr "$ready"); token=$(field token "$ready"); CUR_EPID=$(field pid "$ready")
  cmd=$(worker_cmd "$l" "$e" "$addr" "$w" "$c" "$win")
  line=""; code=0; diag=""
  if [ -z "$cmd" ]; then code=2; else
    # In the background so a TERM reaches the trap at once; exec so the pid is the worker itself and a kill reaches it;
    # 3>&- so the worker does not inherit the FIFO's write end (it would keep the server's stdin open).
    BENCH_TOKEN="$token" sh -c "exec $cmd" 3>&- > "$d/wout" 2> "$d/werr" &
    CUR_WPID=$!
    # A deadline: the window plus time to read everything back, or WORKER_MAX seconds when given; 10 minutes without a window.
    limit=${WORKER_MAX:-$(( win > 0 ? win * 3 + 1800 : 600 ))}
    started=$(date +%s)
    while kill -0 "$CUR_WPID" 2>/dev/null; do
      if [ $(( $(date +%s) - started )) -ge "$limit" ]; then stop_pid "$CUR_WPID" 5; code=124; break; fi
      sleep 0.2
    done
    if [ $code -ne 124 ]; then wait "$CUR_WPID"; code=$?; else wait "$CUR_WPID" 2>/dev/null; fi
    CUR_WPID=""
    line=$(head -n 1 "$d/wout" 2>/dev/null)
    # The worker's first error line, read by the shell (never an argument of another program), masked, THEN cut to 120
    # characters and reduced to a safe alphabet; the token is only ever handled by shell builtins.
    # The LAST line, not the first: a Python traceback begins with "Traceback (most recent call last):" and ends with the error.
    raw=$(tail -n 1 "$d/werr" 2>/dev/null | head -c 4096)
    raw=$(mask_token "$token" "$raw")
    diag=$(printf '%s' "$raw" | head -c 120 | sed 's/[^A-Za-z0-9_.:,()\/-]/_/g')
  fi
  token=""; ready=""
  # The server's stdin closes: it stops, prints STOPPED. Bounded: a server that does not stop is killed.
  exec 3>&-
  i=0
  while kill -0 "$spid" 2>/dev/null && [ $i -lt $((${STOP_WAIT:-120} * 10)) ]; do sleep 0.1; i=$((i + 1)); done
  if kill -0 "$spid" 2>/dev/null; then stop_pid "$spid" 5; kill_engine_group; scode=137; else wait "$spid"; scode=$?; fi
  CUR_SPID=""; CUR_EPID=""
  stopped=$(grep -m1 '^STOPPED ' "$d/out" 2>/dev/null)
  rc=0; bad=""
  [ -n "$line" ] && bad=$(valid_receipt "$line" "$l" "$e" "$w")
  if [ $code -ne 0 ] || [ -z "$line" ]; then
    if [ $code -eq 124 ]; then RESULT="trial=$n engine=$l-$e workers=$w FAILED worker_timeout_after_${limit}s"
    else RESULT="trial=$n engine=$l-$e workers=$w FAILED worker_exit_$code${diag:+:$diag}"; fi
    rc=1
  elif [ -n "$bad" ]; then
    RESULT="trial=$n engine=$l-$e workers=$w FAILED incomplete_worker_receipt:$bad"; rc=1
  elif [ $scode -ne 0 ] || [ -z "$stopped" ] || [ "$(field verdict "$stopped")" != "clean" ]; then
    RESULT="trial=$n engine=$l-$e workers=$w FAILED server_did_not_stop_cleanly"; rc=1
  elif [ "$(field result "$line")" = "FAILED" ]; then
    # Converted to the marker cmd/plots counts as a failed trial: everything after FAILED is a diagnostic.
    RESULT="trial=$n engine=$(field engine "$line") workers=$w FAILED stop=$(field stop "$line") $(field detail "$line")"; rc=1
  else
    srss=$(field server_rss_KiB "$stopped"); crss=$(field client_rss_KiB "$line")
    case "$srss" in *[!0-9]*|"") RESULT="trial=$n engine=$l-$e workers=$w FAILED server_memory_not_reported"; rc=1 ;; *)
      RESULT="trial=$n $line server_rss_KiB=$srss server_rss_source=$(field rss_source "$stopped") total_rss_KiB=$((srss + crss))" ;;
    esac
  fi
  cleanup_trial
  return $rc
}

# RUN_SH_PRINT_COMMANDS=1: print the command each language's worker would run (no token), then exit. For the tests.
if [ -n "${RUN_SH_PRINT_COMMANDS:-}" ]; then
  for l in $langs; do echo "$l: $(worker_cmd "$l" kafka 127.0.0.1:1 8 10 0)"; done
  exit 0
fi

cells=""
for l in $langs; do for e in $engines; do cells="$cells $l:$e"; done; done
[ -x "$serve" ] || { echo "no server driver at $serve: build it first (benchmarks/clients/README.md)" >&2; exit 2; }

# Smoke first, before any timed trial.
if [ "${BENCH_SKIP_SMOKE:-0}" != 1 ]; then
  smoke_failed=0
  for cell in $cells; do
    for w in 1 8 32; do
      if run_cell "${cell%%:*}" "${cell#*:}" "$w" "${BENCH_SMOKE_COUNT:-200}" 0 0; then
        printf 'smoke ok %s %s\n' "$cell" "$w" >&2
      else
        smoke_failed=$((smoke_failed + 1)); printf 'smoke FAILED %s\n' "$RESULT" >&2
      fi
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
      printf '%s\n' "$RESULT"
    done
  done
  rep=$((rep + 1))
done
[ $failed -eq 0 ]
