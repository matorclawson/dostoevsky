#!/bin/sh
# Self-test of run.sh with a stub server driver and stub workers: no engine, no client library, no network.
# From the repository root:  sh benchmarks/clients/run_test.sh
set -u
here=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/tmp"
SECRET=TESTSECRET0123456789abcdef
fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }

# A stub server driver: READY with a secret token, then waits for stdin to close, then STOPPED.
#   STUB_SERVE_DIES=<engine>   exits before READY for that engine
#   STUB_SERVE_HANG=1          never prints READY and never exits by itself
#   STUB_SERVE_STUCK=1         prints READY, then ignores TERM and the closing of stdin
cat > "$t/serve" <<STUB
#!/bin/sh
echo \$\$ > "$t/serve.pid"
if [ "\${STUB_SERVE_DIES:-}" = "\$2" ]; then echo "FAILED: stub" >&2; exit 1; fi
if [ -n "\${STUB_SERVE_HANG:-}" ]; then trap '' TERM; while :; do sleep 1; done; fi
if [ -n "\${STUB_SERVE_STUCK:-}" ]; then
  # an "engine" in a process group of its own, as serve's engine is; READY names its pid
  set -m; sleep 600 & epid=\$!; set +m
  echo \$epid > "$t/engine.pid"
  echo "READY addr=127.0.0.1:12345 pid=\$epid token=$SECRET"
  trap '' TERM; while :; do sleep 1; done
fi
echo "READY addr=127.0.0.1:12345 pid=\$\$ token=$SECRET"
cat > /dev/null
echo "STOPPED server_rss_KiB=1000 rss_source=VmHWM verdict=\${STUB_VERDICT:-clean}"
STUB
# A stub worker: checks the token arrived in the environment, never in argv, and prints a canned, complete line.
cat > "$t/worker" <<STUB
#!/bin/sh
[ "\${BENCH_TOKEN:-}" = "$SECRET" ] || { echo "the token did not arrive in the environment" >&2; exit 9; }
case "\$*" in *$SECRET*) echo "the token is in argv" >&2; exit 8 ;; esac
echo "\$*" >> "$t/argv.log"
eng=\$2; workers=\$6
if [ "\${STUB_MODE:-ok}" = hang ]; then while :; do sleep 1; done; fi
if [ "\${STUB_MODE:-ok}" = leakfd ]; then sleep 20 & fi   # a child that would keep the FIFO's write end open if it inherited it
if [ "\${STUB_MODE:-ok}" = crash ]; then echo "boom with the token $SECRET and a bad char \\\$(x) in it, then more and more text that goes on long enough to need cutting off somewhere around the hundred and twentieth character of it" >&2; exit 3; fi
common="p50_us=2.000 p95_us=2.000 p99_us=2.000 p99_9_us=2.000 p99_99_us=2.000 quantiles=bin-upper-1pct client_rss_source=ru_maxrss lang=python client=stub-1 client_build=n/a"
if [ "\${STUB_MODE:-ok}" = failed ] && [ "\$eng" = "\${STUB_FAIL_ENGINE:-kafka}" ]; then
  echo "engine=python-\$eng workers=\$workers result=FAILED acked=4 verified=4 records/s=2 elapsed_s=2.00 \$common client_rss_KiB=2000 stop=failed detail=publish_4:_Error:engine-error_(after_4_acknowledged)"
elif [ "\${STUB_MODE:-ok}" = incomplete ]; then
  echo "engine=python-\$eng workers=\$workers result=ok acked=10 verified=10 records/s=100 elapsed_s=1.00 \$common stop=reached"   # no client_rss_KiB
elif [ "\${STUB_MODE:-ok}" = badnum ]; then
  echo "engine=python-\$eng workers=\$workers result=ok acked=10 verified=10 records/s=NaN elapsed_s=1.00 \$common client_rss_KiB=2000 stop=reached"
elif [ "\${STUB_MODE:-ok}" = badp99 ]; then
  echo "engine=python-\$eng workers=\$workers result=ok acked=10 verified=10 records/s=100 elapsed_s=1.00 p50_us=2.000 p95_us=2.000 p99_us=-5 p99_9_us=2.000 p99_99_us=2.000 quantiles=bin-upper-1pct client_rss_source=ru_maxrss lang=python client=stub-1 client_build=n/a client_rss_KiB=2000 stop=reached"
elif [ "\${STUB_MODE:-ok}" = wrongcount ]; then
  echo "engine=python-\$eng workers=7 result=ok acked=10 verified=10 records/s=100 elapsed_s=1.00 \$common client_rss_KiB=2000 stop=reached"
else
  echo "engine=python-\$eng workers=\$workers result=ok acked=10 verified=10 records/s=100 elapsed_s=1.00 \$common client_rss_KiB=2000 stop=reached"
fi
STUB
chmod +x "$t/serve" "$t/worker"
run() { # extra env assignments as arguments; stdout -> $t/out, stderr -> $t/err, exit code -> $code
  : > "$t/argv.log"; rm -f "$t/serve.pid"
  env TMPDIR="$t/tmp" SERVE_BIN="$t/serve" WORKER_CMD_python="sh $t/worker" CLIENT_LANGS=python CLIENT_ENGINES="dostoevsky kafka" "$@" \
    sh "$here/run.sh" > "$t/out" 2> "$t/err"
  code=$?
}
leftovers() { ls "$t/tmp" | grep -c 'clients-trial' ; }

run BENCH_ROUNDS=2 BENCH_SKIP_SMOKE=1
check "a clean run exits 0" "[ $code -eq 0 ]"
check "2 rounds x 3 producer counts x 2 cells = 12 trial lines" "[ \$(grep -c '^trial=' $t/out) -eq 12 ]"
check "every line carries the server memory and the sum" "[ \$(grep -c 'server_rss_KiB=1000 server_rss_source=VmHWM total_rss_KiB=3000' $t/out) -eq 12 ]"
check "the starting cell is rotated: round 1 starts with dostoevsky, round 2 with kafka" "[ \"\$(grep '^trial=1 ' $t/out | head -1 | cut -d' ' -f2)\" = engine=python-dostoevsky ] && [ \"\$(grep '^trial=2 ' $t/out | head -1 | cut -d' ' -f2)\" = engine=python-kafka ]"
check "the token appears in no output" "! grep -q $SECRET $t/out $t/err"
check "the token was never on a command line" "! grep -q $SECRET $t/argv.log"
check "the serve engine for dostoevsky is the gateway" "grep -q -- '--engine dostoevsky' $t/argv.log"
check "a finished run leaves no trial directory" "[ \$(leftovers) -eq 0 ]"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=failed
check "a failed trial is a FAILED line and the run exits 1" "[ $code -eq 1 ] && grep -q '^trial=1 engine=python-kafka workers=1 FAILED stop=failed' $t/out"
check "failed trials are counted, never retried: 6 trials, 6 worker calls" "[ \$(grep -c '^trial=' $t/out) -eq 6 ] && [ \$(wc -l < $t/argv.log) -eq 6 ]"
check "the detail carries no token" "! grep -q $SECRET $t/out"

run BENCH_ROUNDS=1 STUB_MODE=failed
check "a failed smoke stops the run with exit 3 and no timed line" "[ $code -eq 3 ] && [ \$(grep -c '^trial=' $t/out) -eq 0 ] && grep -q 'no timed trial was run' $t/err"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=crash
check "a crashing worker is a FAILED line with a bounded, sanitized, token-free diagnostic" "[ $code -eq 1 ] && grep -q 'FAILED worker_exit_3:boom_with_the_token__token_' $t/out && ! grep -q $SECRET $t/out && [ \$(head -1 $t/out | wc -c) -lt 260 ] && ! grep -q '[$]' $t/out"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=incomplete
check "a receipt missing a required field is a FAILED line naming the field" "[ $code -eq 1 ] && grep -q 'FAILED incomplete_worker_receipt:missing_client_rss_KiB' $t/out"
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=wrongcount
check "a receipt for another worker count is refused" "[ $code -eq 1 ] && grep -q 'FAILED incomplete_worker_receipt:wrong_workers' $t/out"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_SERVE_DIES=gateway READY_WAIT=50
check "a server that dies before READY is a FAILED line, not a hang" "[ $code -eq 1 ] && grep -q 'FAILED server_did_not_become_ready' $t/out"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_SERVE_STUCK=1 STOP_WAIT=2 CLIENT_ENGINES=dostoevsky
check "a server that will not stop is killed after the bound and reported, not waited for" "[ $code -eq 1 ] && grep -q 'FAILED server_did_not_stop_cleanly' $t/out && [ \$(leftovers) -eq 0 ]"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=badnum CLIENT_ENGINES=dostoevsky
check "a non-numeric throughput is refused, not accepted with exit 0" "[ $code -eq 1 ] && grep -q 'FAILED incomplete_worker_receipt:not_a_decimal_records_s' $t/out"
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=badp99 CLIENT_ENGINES=dostoevsky
check "a negative percentile is refused" "[ $code -eq 1 ] && grep -q 'FAILED incomplete_worker_receipt:not_a_decimal_p99_us' $t/out"
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_VERDICT=dirty CLIENT_ENGINES=dostoevsky
check "a STOPPED line without verdict=clean is not a clean stop" "[ $code -eq 1 ] && grep -q 'FAILED server_did_not_stop_cleanly' $t/out"

begin=$(date +%s)
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=hang WORKER_MAX=2 CLIENT_ENGINES=dostoevsky
check "a worker that never ends hits its deadline and is reported, not waited for" "[ $code -eq 1 ] && grep -q 'FAILED worker_timeout_after_2s' $t/out && [ \$(( \$(date +%s) - $begin )) -lt 60 ]"

begin=$(date +%s)
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=leakfd STOP_WAIT=5 CLIENT_ENGINES=dostoevsky
check "a worker's child does not inherit the server's stdin: the server stops at once and the trial succeeds" "[ $code -eq 0 ] && [ \$(grep -c '^trial=' $t/out) -eq 3 ] && [ \$(( \$(date +%s) - $begin )) -lt 15 ]"

rm -f "$t/engine.pid"
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_SERVE_STUCK=1 STOP_WAIT=2 CLIENT_ENGINES=dostoevsky
epid=$(cat "$t/engine.pid" 2>/dev/null)
check "when a hung serve is killed, its engine's process group goes with it" "[ -n \"$epid\" ] && ! kill -0 $epid 2>/dev/null"

# No program the script runs may see the token in its arguments (ps can read them): every child's argv is logged by a wrapper.
mkdir -p "$t/bin"; : > "$t/children.log"
for c in sed head tr grep sleep date mktemp mkfifo rm rmdir cut wc; do
  real=$(command -v $c) || continue
  printf '#!/bin/sh\nprintf "%%s %%s\\n" "%s" "$*" >> "%s"\nexec %s "$@"\n' "$c" "$t/children.log" "$real" > "$t/bin/$c"
  chmod +x "$t/bin/$c"
done
run PATH="$t/bin:$PATH" BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=crash CLIENT_ENGINES=dostoevsky
check "the wrappers saw the children run" "[ \$(wc -l < $t/children.log) -gt 20 ]"
check "the token is in no child's argv (sed, head, grep, ... all logged)" "! grep -q $SECRET $t/children.log"
check "the diagnostic is still masked after moving the masking into the shell" "grep -q 'FAILED worker_exit_3:boom_with_the_token__token_' $t/out"

for bad in WORKER_MAX=ten WORKER_MAX=0 STOP_WAIT=-5 READY_WAIT=1.5 BENCH_WINDOW=abc WORKER_MAX=08 STOP_WAIT=007 READY_WAIT=99999999999999999999999999 BENCH_WINDOW=12345678 BENCH_WINDOW=+5 WORKER_MAX=; do
  run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 "$bad" CLIENT_ENGINES=dostoevsky
  check "an invalid '$bad' is refused before anything starts (exit 2, no trial line)" "[ $code -eq 2 ] && [ \$(grep -c '^trial=' $t/out) -eq 0 ] && grep -q 'must be' $t/err"
done

# The Python worker must be started with the client on its path (a rehearsal failed without it).
out=$(env RUN_SH_PRINT_COMMANDS=1 CLIENT_LANGS="python javascript java rust" SERVE_BIN="$t/serve" sh "$here/run.sh" 2>&1)
check "the python worker command sets PYTHONPATH=clients/python and runs the venv's interpreter" "printf '%s\n' \"$out\" | grep -q '^python: env PYTHONPATH=clients/python .dev/bench-venv/bin/python benchmarks/clients/python/worker.py'"
check "every language has a worker command, none with a token" "[ \$(printf '%s\n' \"$out\" | grep -c ': ') -eq 4 ] && ! printf '%s' \"$out\" | grep -q $SECRET"

# A Python traceback's FIRST line says nothing; the diagnostic is the LAST line of stderr, masked and reduced.
cat > "$t/worker_tb" <<STUB
#!/bin/sh
[ "\${BENCH_TOKEN:-}" = "$SECRET" ] || exit 9
echo "Traceback (most recent call last):" >&2
echo "  File \"worker.py\", line 1" >&2
echo "ModuleNotFoundError: No module named dostoevsky_client token $SECRET" >&2
exit 1
STUB
chmod +x "$t/worker_tb"
run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 CLIENT_ENGINES=dostoevsky WORKER_CMD_python="sh $t/worker_tb"
check "the failure detail is the last stderr line (the error), not 'Traceback', with the token masked" "grep -q 'FAILED worker_exit_1:ModuleNotFoundError:_No_module_named_dostoevsky_client_token__token_' $t/out && ! grep -q Traceback $t/out && ! grep -q $SECRET $t/out"

# TERM during readiness: the server is stopped and the directory with the token is removed.
: > "$t/argv.log"; rm -f "$t/serve.pid"
env TMPDIR="$t/tmp" SERVE_BIN="$t/serve" WORKER_CMD_python="sh $t/worker" CLIENT_LANGS=python CLIENT_ENGINES=dostoevsky BENCH_SKIP_SMOKE=1 BENCH_ROUNDS=1 STUB_SERVE_HANG=1 STOP_WAIT=3 \
  sh "$here/run.sh" > "$t/out" 2> "$t/err" &
rpid=$!
i=0; while [ ! -s "$t/serve.pid" ] && [ $i -lt 100 ]; do sleep 0.1; i=$((i + 1)); done
spid=$(cat "$t/serve.pid" 2>/dev/null)
kill -TERM "$rpid"
i=0; while kill -0 "$rpid" 2>/dev/null && [ $i -lt 200 ]; do sleep 0.1; i=$((i + 1)); done
check "a TERM during readiness ends run.sh promptly" "! kill -0 $rpid 2>/dev/null"
wait "$rpid" 2>/dev/null; code=$?
check "run.sh exits 143 on TERM" "[ $code -eq 143 ]"
check "the stubbed server is gone after the TERM" "[ -n \"$spid\" ] && ! kill -0 $spid 2>/dev/null"
check "no trial directory (and so no token) is left after the TERM" "[ \$(leftovers) -eq 0 ]"

if [ $fails -ne 0 ]; then echo "$fails check(s) failed"; exit 1; fi
echo "all checks passed"
