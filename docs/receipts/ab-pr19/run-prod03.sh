#!/bin/bash
set -euo pipefail
umask 077
export PATH=/usr/bin:/bin:/usr/sbin:/sbin LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ -O "$stage" && "$stage" =~ ^/srv/zeo-gates/home/bench-ab19-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" ]]
cd "$stage"
mkdir -p dl bin tmp go/toolchain go/mod go/cache go/path go/config
export TMPDIR="$stage/tmp" GOTMPDIR="$stage/tmp" GOCACHE="$stage/go/cache" GOMODCACHE="$stage/go/mod"
export GOPATH="$stage/go/path" XDG_CONFIG_HOME="$stage/go/config" GOENV=off GOTOOLCHAIN=local GOMAXPROCS=2
unset GOFLAGS BENCH_ROUNDS BENCH_COUNT
[[ "$(findmnt -T "$TMPDIR" -n -o FSTYPE)" == ext4 ]]
epoch=$(stat -c %W "$stage")
(( epoch > 0 ))
phase=preconditions
watch=''
smoke=''
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
    printf 'reservation=4907 start_epoch=%s\n' "$epoch"
    uname -a
    lscpu
    free -h
    df -hT "$TMPDIR"
    findmnt -T "$TMPDIR" -o SOURCE,FSTYPE,TARGET,OPTIONS
    lsblk -d -o NAME,SIZE,ROTA,MODEL
    uptime
    ps -eo user,comm,pcpu,pmem,state --sort=-pcpu | head -15 || true
    watch_gate
} > HOST.log
[[ ! -e GATE-OVERLAP ]]
( while sleep 30; do watch_gate; done ) >> "$stage/DISPATCH.log" 2>&1 &
watch=$!
dpkg-query -W -f='${Package} ${Version}\n' | sort > PACKAGES-BEFORE.txt
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server|dost'; then exit 4; fi
printf 'engine_preflight_processes=none\n' >> HOST.log
awk '/^MemAvailable:/ {exit !($2 >= 1572864)}' /proc/meminfo
(( $(df -B1 --output=avail "$TMPDIR" | tail -1) >= 5 * 1024 * 1024 * 1024 ))
{
    printf 'apt_package_changes=none\n'
} > ENGINE-VERSIONS.log
sha256sum -c SOURCE-SHA256SUMS
phase=toolchain
curl -qfsSL --proto '=https' --proto-redir '=https' --retry 2 --max-time 180 https://go.dev/dl/go1.26.0.linux-arm64.tar.gz -o dl/go1.26.0.linux-arm64.tar.gz
printf 'bd03b743eb6eb4193ea3c3fd3956546bf0e3ca5b7076c8226334afe6b75704cd  dl/go1.26.0.linux-arm64.tar.gz\n' | sha256sum -c | tee -a ENGINE-VERSIONS.log
sha256sum dl/go1.26.0.linux-arm64.tar.gz >> ENGINE-VERSIONS.log
tar -xzf dl/go1.26.0.linux-arm64.tar.gz -C go/toolchain
export PATH="$stage/go/toolchain/go/bin:$PATH"
go version | tee -a ENGINE-VERSIONS.log HOST.log
phase=builds
: > BUILT-PINS.txt
for ref in baseline candidate; do
    git clone --quiet --branch "$ref" --no-tags "$stage/source.bundle" "$ref"
    { printf '%s ' "$ref"; git -C "$ref" log -1 --format='%H %T'; } >> BUILT-PINS.txt
    (
        cd "$ref/benchmarks/embedded"
        go mod verify
        CGO_ENABLED=0 go build -mod=readonly -o "$stage/bin/dost-$ref" ./cmd/dost
        go version -m "$stage/bin/dost-$ref"
    ) > "$ref-BUILD.log" 2>&1
done
cmp EXPECTED-PINS.txt BUILT-PINS.txt
sha256sum bin/dost-baseline bin/dost-candidate > BINARY-SHA256SUMS
cat BUILT-PINS.txt BINARY-SHA256SUMS >> HOST.log
[[ ! -e GATE-OVERLAP ]]
phase=ab
cd "$stage/candidate"
printf 'start=%s command=sh benchmarks/comparison/ab.sh BASELINE_BIN CANDIDATE_BIN\n' "$(date -u +%FT%TZ)" >> "$stage/EXIT.log"
ab_status=0
sh benchmarks/comparison/ab.sh "$stage/bin/dost-baseline" "$stage/bin/dost-candidate" > "$stage/ab.log" 2> "$stage/ab.err" || ab_status=$?
printf 'finish=%s ab_exit=%s\n' "$(date -u +%FT%TZ)" "$ab_status" >> "$stage/EXIT.log"
phase=summary
cp "$stage/ab.log" ab.log
summary=(go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -trials 5)
printf 'summary_command=%s\n' "${summary[*]}" >> "$stage/EXIT.log"
summary_status=0
"${summary[@]}" > "$stage/MEDIANS.md" 2> "$stage/RESULT.err" || summary_status=$?
printf 'summary_exit=%s\n' "$summary_status" >> "$stage/EXIT.log"
if [[ "$summary_status" == 1 ]] && grep -Fq 'plots: receipt rejected:' "$stage/RESULT.err"; then
    incomplete=0
    "${summary[@]}" -allow-incomplete > "$stage/MEDIANS-INCOMPLETE.md" 2> "$stage/RESULT-INCOMPLETE.err" || incomplete=$?
    printf 'allow_incomplete_exit=%s\n' "$incomplete" >> "$stage/EXIT.log"
fi
cmp ab.log "$stage/ab.log"
phase=crash
crash_status=0
GOFLAGS=-v go test -count=3 -run TestConcurrentPublishKill . > "$stage/CRASH-TESTS.log" 2>&1 || crash_status=$?
printf 'crash_test_exit=%s\n' "$crash_status" >> "$stage/EXIT.log"
git status --porcelain --untracked-files=no > "$stage/SOURCE-STATUS.log"
phase=complete
