#!/bin/bash
set -euo pipefail
umask 077
export PATH=/usr/bin:/bin:/usr/sbin:/sbin LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ -O "$stage" && "$stage" =~ ^/srv/zeo-gates/home/bench-final-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" ]]
cd "$stage"
export TMPDIR="$stage/tmp" GOTMPDIR="$stage/tmp" GOCACHE="$stage/go/cache" GOMODCACHE="$stage/go/mod"
export GOPATH="$stage/go/path" XDG_CONFIG_HOME="$stage/go/config" GOENV=off GOTOOLCHAIN=local GOMAXPROCS=2
unset GOFLAGS BENCH_ROUNDS BENCH_COUNT BENCH_SKIP_SMOKE BENCH_SMOKE_COUNT
[[ "$(findmnt -T "$TMPDIR" -n -o FSTYPE)" == ext4 ]]
grep -q ' prep_exit=0$' "$stage/PREP-EXIT.log"
grep -qx 'provenance=hex-prebuilt' "$stage/ERLANG-PROVENANCE.txt"
awk '{exit !($1 < 0.3)}' /proc/loadavg
awk -F= '$1=="quiet_elapsed_seconds" && $2>=300{ok=1} END{exit !ok}' "$stage/QUIET-WAIT.log"
grep -qx 'private_socket_checks=pass harness_preflight=pass' "$stage/PRECONDITIONS.txt"
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server'; then exit 4; fi
awk '/^MemAvailable:/{seen=1; ok=$2>=2097152} END{exit !(seen && ok)}' /proc/meminfo
(( $(df -B1 --output=avail "$stage" | tail -1) >= 8 * 1024 * 1024 * 1024 ))
epoch=$(date +%s)
printf '%s\n' "$epoch" > "$stage/WINDOW-EPOCH.txt"
phase=preconditions
watch=''
capture=''
engine_pid=''
gate_paths=(/srv/zeo-gates/inputs /srv/zeo-gates/work /srv/zeo-gates/receipts /srv/zeo-gates/tmp)
watch_gate() {
    printf 'dispatch_scan_utc=%s since_epoch=%s\n' "$(date -u +%FT%TZ)" "$epoch"
    local changes
    changes=$(find "${gate_paths[@]}" -mindepth 1 -maxdepth 2 -newermt "@$epoch" -printf '%TY-%Tm-%TdT%TH:%TM:%TSZ %p\n' | sort)
    if [[ -n "$changes" ]]; then
        printf '%s\n' "$changes"
        : > "$stage/GATE-OVERLAP"
    else
        printf 'dispatch_activity=none_observed\n'
    fi
}
finish() {
    code=$?
    trap - EXIT
    if [[ -n "$engine_pid" ]]; then kill "$engine_pid" 2>/dev/null || true; wait "$engine_pid" 2>/dev/null || true; fi
    if [[ -n "$capture" ]]; then kill "$capture" 2>/dev/null || true; wait "$capture" 2>/dev/null || true; fi
    if [[ -n "$watch" ]]; then kill "$watch" 2>/dev/null || true; wait "$watch" 2>/dev/null || true; fi
    {
        printf 'host_end=%s stage=%s pipeline_exit=%s\n' "$(date -u +%FT%TZ)" "$phase" "$code"
        uptime
        ps -eo user,comm,pcpu,pmem,state --sort=-pcpu | head -15 || true
        watch_gate
    } >> "$stage/HOST.log"
    printf 'pipeline_phase=%s pipeline_exit=%s\n' "$phase" "$code" >> "$stage/EXIT.log"
    exit "$code"
}
trap finish EXIT
{
    date -u +%FT%TZ
    printf 'reservation=5052 start_epoch=%s reserved_minutes=90 end_epoch=%s\n' "$epoch" "$((epoch+5400))"
    cat "$stage/QUIET-WAIT.log"
    uname -a
    lscpu
    free -h
    cat /proc/loadavg
    df -hT "$TMPDIR"
    findmnt -T "$TMPDIR" -o SOURCE,FSTYPE,TARGET,OPTIONS
    lsblk -d -o NAME,SIZE,ROTA,MODEL
    uptime
    git -C checkout rev-parse HEAD 'HEAD^{tree}'
    grep 'go1.26.0.linux-arm64.tar.gz' ENGINE-VERSIONS.log
    ps -eo user,comm,pcpu,pmem,state --sort=-pcpu | head -15 || true
    watch_gate
} > HOST.log
[[ ! -e GATE-OVERLAP ]]
( while sleep 30; do watch_gate; done ) >> "$stage/DISPATCH.log" 2>&1 &
watch=$!
export PATH="$stage/go/toolchain/go/bin:$stage/erlang-27.3.4.20/bin:$PATH" GO="$stage/go/toolchain/go/bin/go"
export JAVA_HOME="$stage/jdk-21" KAFKA_HOME="$stage/kafka_2.13-4.4.0" RABBIT_HOME="$stage/rabbitmq_server-4.4.1"
export BENCH_JAVA_HOME="$JAVA_HOME" BENCH_KAFKA_HOME="$KAFKA_HOME" BENCH_RABBIT_HOME="$RABBIT_HOME"
export BENCH_EPMD="$stage/erlang-27.3.4.20/bin/epmd"
export ERL_EPMD_ADDRESS=127.0.0.1 GOPROXY=off
export REDIS_BIN=$(cat "$stage/REDIS-BIN.txt") VALKEY_BIN=$(cat "$stage/VALKEY-BIN.txt")
test -x "$REDIS_BIN" && test -x "$VALKEY_BIN"
test -x "$BENCH_EPMD"
cat "$stage/KAFKA-CONFIG-CITATIONS.txt" >> "$stage/ENGINE-VERSIONS.log"
cat "$stage/ERLANG-PROVENANCE.txt" >> "$stage/ENGINE-VERSIONS.log"
go version >> "$stage/HOST.log"
printf 'preflight_exit=0 preflight_completed_before_window=yes\n' > "$stage/EXIT.log"
[[ ! -e "$stage/GATE-OVERLAP" ]]
cd "$stage/checkout"
phase=comparison
export BENCH_ENGINES='dostoevsky jetstream sqlite redis valkey kafka rabbitmq'
printf 'start=%s command=sh benchmarks/comparison/run.sh\n' "$(date -u +%FT%TZ)" >> "$stage/EXIT.log"
python3 "$stage/preserve-kafka-smoke.py" "$stage" > "$stage/SMOKE-CAPTURE.log" 2>&1 &
capture=$!
run_status=0
sh benchmarks/comparison/run.sh > "$stage/comparison.log" 2> "$stage/comparison.err" || run_status=$?
printf 'finish=%s comparison_exit=%s\n' "$(date -u +%FT%TZ)" "$run_status" >> "$stage/EXIT.log"
kill "$capture" 2>/dev/null || true
wait "$capture" 2>/dev/null || true
capture=''
grep '^smoke ' "$stage/comparison.err" > "$stage/SMOKE.log" || true
printf 'smoke_lines=%s smoke_failures=%s\n' "$(awk '/^smoke engine=|^smoke FAILED/{n++} END{print n+0}' "$stage/SMOKE.log")" "$(awk '/^smoke FAILED/{n++} END{print n+0}' "$stage/SMOKE.log")" >> "$stage/EXIT.log"
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server' > "$stage/PROCESS-AFTER.log"; then
    printf 'engine_processes=present\n' >> "$stage/EXIT.log"
else
    printf 'engine_processes=none\n' >> "$stage/EXIT.log"
fi
if (( run_status == 3 )); then
    phase=smoke-failed
    test ! -s "$stage/comparison.log"
    if [[ -d "$stage/smoke-kafka" ]]; then
        find "$stage/smoke-kafka" -type f -name '.kafka_cleanshutdown' -print > "$stage/KAFKA-MARKERS.txt"
        tar -C "$stage" -czf "$stage/SMOKE-KAFKA-LOGS.tar.gz" smoke-kafka
    fi
    exit 3
fi
(( run_status == 0 || run_status == 1 ))
phase=medians
cd "$stage/checkout"
cp "$stage/comparison.log" comparison.log
summary=(go run ./cmd/plots -summary -receipt comparison.log -expect-engines dostoevsky,jetstream,sqlite,redis,valkey,kafka,rabbitmq -trials 5)
printf 'summary_command=%s\n' "${summary[*]}" >> "$stage/EXIT.log"
result_status=0
"${summary[@]}" > "$stage/MEDIANS.md" 2> "$stage/RESULT.err" || result_status=$?
printf 'result_exit=%s trial_lines=%s failed_lines=%s\n' "$result_status" "$(awk '/^trial=/{n++} END {print n+0}' comparison.log)" "$(awk '/^trial=.* FAILED /{n++} END {print n+0}' comparison.log)" >> "$stage/EXIT.log"
if [[ "$result_status" == 1 ]] && grep -Fq 'plots: receipt rejected:' "$stage/RESULT.err"; then
    incomplete=0
    "${summary[@]}" -allow-incomplete > "$stage/MEDIANS-INCOMPLETE.md" 2> "$stage/RESULT-INCOMPLETE.err" || incomplete=$?
    printf 'allow_incomplete_exit=%s\n' "$incomplete" >> "$stage/EXIT.log"
    [[ "$incomplete" == 0 ]]
else
    [[ "$result_status" == 0 ]]
fi
cmp comparison.log "$stage/comparison.log"
git status --porcelain --untracked-files=no > "$stage/SOURCE-STATUS.log"
phase=complete
