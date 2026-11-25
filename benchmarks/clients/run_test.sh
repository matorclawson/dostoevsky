#!/bin/sh
# Self-test of run.sh with a stub server driver and stub workers: no engine, no client library, no network.
# From the repository root:  sh benchmarks/clients/run_test.sh
set -u
here=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
SECRET=TESTSECRET-0123456789abcdef
fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }

# A stub server driver: READY with a secret token, then waits for stdin to close, then STOPPED.
cat > "$t/serve" <<STUB
#!/bin/sh
if [ "\${STUB_SERVE_DIES:-}" = "\$2" ]; then echo "FAILED: stub" >&2; exit 1; fi
echo "READY addr=127.0.0.1:12345 pid=\$\$ token=$SECRET"
cat > /dev/null
echo "STOPPED server_rss_KiB=1000 rss_source=VmHWM verdict=clean"
STUB
# A stub worker: checks the token arrived in the environment, never in argv, and prints a canned line.
cat > "$t/worker" <<STUB
#!/bin/sh
[ "\${BENCH_TOKEN:-}" = "$SECRET" ] || { echo "the token did not arrive in the environment" >&2; exit 9; }
case "\$*" in *$SECRET*) echo "the token is in argv" >&2; exit 8 ;; esac
echo "\$*" >> "$t/argv.log"
eng=\$2; workers=\$6
if [ "\${STUB_MODE:-ok}" = crash ]; then exit 3; fi
if [ "\${STUB_MODE:-ok}" = failed ] && [ "\$eng" = "\${STUB_FAIL_ENGINE:-kafka}" ]; then
  echo "engine=python-\$eng workers=\$workers result=FAILED acked=4 verified=4 stop=failed detail=publish_4:_Error:engine-error_(after_4_acknowledged)"
else
  echo "engine=python-\$eng workers=\$workers result=ok acked=10 verified=10 records/s=100 client_rss_KiB=2000 client_rss_source=ru_maxrss lang=python stop=reached"
fi
STUB
chmod +x "$t/serve" "$t/worker"
run() { # extra env assignments as arguments; stdout -> $t/out, stderr -> $t/err, exit code -> $code
  : > "$t/argv.log"
  env SERVE_BIN="$t/serve" WORKER_CMD_python="sh $t/worker" CLIENT_LANGS=python CLIENT_ENGINES="dostoevsky kafka" "$@" \
    sh "$here/run.sh" > "$t/out" 2> "$t/err"
  code=$?
}

run BENCH_ROUNDS=2 BENCH_SKIP_SMOKE=1
check "a clean run exits 0" "[ $code -eq 0 ]"
check "2 rounds x 3 producer counts x 2 cells = 12 trial lines" "[ \$(grep -c '^trial=' $t/out) -eq 12 ]"
check "every line carries the server memory and the sum" "[ \$(grep -c 'server_rss_KiB=1000 server_rss_source=VmHWM total_rss_KiB=3000' $t/out) -eq 12 ]"
check "the starting cell is rotated: round 1 starts with dostoevsky, round 2 with kafka" "[ \"\$(grep '^trial=1 ' $t/out | head -1 | cut -d' ' -f2)\" = engine=python-dostoevsky ] && [ \"\$(grep '^trial=2 ' $t/out | head -1 | cut -d' ' -f2)\" = engine=python-kafka ]"
check "the token appears in no output" "! grep -q $SECRET $t/out $t/err"
check "the token was never on a command line" "! grep -q $SECRET $t/argv.log"
check "the serve engine for dostoevsky is the gateway" "grep -q -- '--engine dostoevsky' $t/argv.log"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=failed
check "a failed trial is a FAILED line and the run exits 1" "[ $code -eq 1 ] && grep -q '^trial=1 engine=python-kafka workers=1 FAILED stop=failed' $t/out"
check "failed trials are counted, never retried: 6 trials, 6 worker calls" "[ \$(grep -c '^trial=' $t/out) -eq 6 ] && [ \$(wc -l < $t/argv.log) -eq 6 ]"
check "the detail carries no token" "! grep -q $SECRET $t/out"

run BENCH_ROUNDS=1 STUB_MODE=failed
check "a failed smoke stops the run with exit 3 and no timed line" "[ $code -eq 3 ] && [ \$(grep -c '^trial=' $t/out) -eq 0 ] && grep -q 'no timed trial was run' $t/err"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_MODE=crash
check "a crashing worker is a FAILED line" "[ $code -eq 1 ] && grep -q 'FAILED worker_exit_3' $t/out"

run BENCH_ROUNDS=1 BENCH_SKIP_SMOKE=1 STUB_SERVE_DIES=gateway READY_WAIT=50
check "a server that dies before READY is a FAILED line, not a hang" "[ $code -eq 1 ] && grep -q 'FAILED server_did_not_become_ready' $t/out"

if [ $fails -ne 0 ]; then echo "$fails check(s) failed"; exit 1; fi
echo "all checks passed"
