#!/bin/bash
set -euo pipefail
umask 077
export PATH=/usr/bin:/bin:/usr/sbin:/sbin LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ -O "$stage" && "$stage" =~ ^/srv/zeo-gates/home/bench-ab19-1p-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" ]]
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
    printf 'reservation=4926 start_epoch=%s\n' "$epoch"
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
phase=rebuild
original=$(cat REPRO-BUILD-PATH.txt)
[[ "$original" == /srv/zeo-gates/home/bench-ab19-20261125T004930Z.T5hmFnej ]]
[[ ! -e "$original" && ! -L "$original" ]]
mkdir -m 700 -- "$original"
# Relocate the owned build tree on every exit before the outer receipt is collected.
relocate() {
    local code=$?
    if [[ -d "$original" && -O "$original" && ! -L "$original" && ! -e "$stage/rebuild" ]]; then
        mv -T -- "$original" "$stage/rebuild" || return 9
    fi
    return "$code"
}
(
    trap relocate EXIT
    cd "$original"
    mkdir -p dl bin tmp go/toolchain go/mod go/cache go/path go/config
    export TMPDIR="$original/tmp" GOTMPDIR="$original/tmp" GOCACHE="$original/go/cache" GOMODCACHE="$original/go/mod"
    export GOPATH="$original/go/path" XDG_CONFIG_HOME="$original/go/config"
    curl -qfsSL --proto '=https' --proto-redir '=https' --retry 2 --max-time 180 https://go.dev/dl/go1.26.0.linux-arm64.tar.gz -o dl/go1.26.0.linux-arm64.tar.gz
    printf 'bd03b743eb6eb4193ea3c3fd3956546bf0e3ca5b7076c8226334afe6b75704cd  dl/go1.26.0.linux-arm64.tar.gz\n' | sha256sum -c | tee -a "$stage/ENGINE-VERSIONS.log"
    sha256sum dl/go1.26.0.linux-arm64.tar.gz >> "$stage/ENGINE-VERSIONS.log"
    tar -xzf dl/go1.26.0.linux-arm64.tar.gz -C go/toolchain
    export PATH="$original/go/toolchain/go/bin:$PATH"
    go version | tee -a "$stage/ENGINE-VERSIONS.log" "$stage/HOST.log"
    : > "$stage/BUILT-PINS.txt"
    for ref in baseline candidate; do
        git clone --quiet --branch "$ref" --no-tags "$stage/source.bundle" "$ref"
        { printf '%s ' "$ref"; git -C "$ref" log -1 --format='%H %T'; } >> "$stage/BUILT-PINS.txt"
        (
            cd "$ref/benchmarks/embedded"
            go mod verify
            CGO_ENABLED=0 go build -mod=readonly -o "$original/bin/dost-$ref" ./cmd/dost
            go version -m "$original/bin/dost-$ref"
        ) > "$stage/$ref-BUILD.log" 2>&1
    done
    cmp "$stage/EXPECTED-PINS.txt" "$stage/BUILT-PINS.txt"
    sha256sum bin/dost-baseline bin/dost-candidate > "$stage/BINARY-SHA256SUMS"
    sha256sum -c "$stage/EXPECTED-BINARY-SHA256SUMS" > "$stage/BINARY-MATCH.log"
)
[[ ! -e "$original" && ! -L "$original" && -d "$stage/rebuild" ]]
printf 'original_build_path_removed=%s\n' "$original" >> "$stage/BINARY-MATCH.log"
cat BUILT-PINS.txt BINARY-SHA256SUMS BINARY-MATCH.log >> HOST.log
[[ ! -e GATE-OVERLAP ]]
phase=ab
export PATH="$stage/rebuild/go/toolchain/go/bin:$PATH"
cd "$stage/rebuild/candidate"
printf 'start=%s command=bash one-producer.sh\n' "$(date -u +%FT%TZ)" >> "$stage/EXIT.log"
ab_status=0
bash "$stage/one-producer.sh" "$stage/rebuild/bin/dost-baseline" "$stage/rebuild/bin/dost-candidate" > "$stage/ab.log" 2> "$stage/ab.err" || ab_status=$?
printf 'finish=%s ab_exit=%s\n' "$(date -u +%FT%TZ)" "$ab_status" >> "$stage/EXIT.log"
phase=summary
cp "$stage/ab.log" ab.log
summary=(go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -expect-producers 1 -trials 15)
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
git status --porcelain --untracked-files=no > "$stage/SOURCE-STATUS.log"
phase=complete
