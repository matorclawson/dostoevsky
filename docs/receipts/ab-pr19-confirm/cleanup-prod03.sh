#!/bin/bash
set -euo pipefail
export LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-ab19-confirm-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
grep -q '^pipeline_phase=' "$stage/EXIT.log"
epoch=$(stat -c %W "$stage")
(( epoch > 0 ))
printf 'cleanup_start=%s\nstage=%s\n' "$(date -u +%FT%TZ)" "$stage"
df -hT "$stage"
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server|dost'; then
    printf 'cleanup aborted: engine still running\n' >&2
    exit 2
fi
printf 'engine_processes=none\n'
if cmp -s "$stage/PACKAGES-BEFORE.txt" "$stage/PACKAGES-AFTER.txt"; then
    printf 'package_inventory_diff=empty\n'
else
    diff -u "$stage/PACKAGES-BEFORE.txt" "$stage/PACKAGES-AFTER.txt" || true
    printf 'package_inventory_diff=detected\n'
fi
for cache in "$stage/go/mod" "$stage/rebuild/go/mod"; do
    if [[ -d "$cache" ]]; then find "$cache" -xdev -type d -uid "$(id -u)" -exec chmod u+w -- {} +; fi
done
original=$(cat "$stage/REPRO-BUILD-PATH.txt")
[[ ! -e "$original" && ! -L "$original" ]]
printf 'original_build_path_absent=%s\n' "$original"
find "$stage" -xdev -depth -delete
[[ ! -e "$stage" && ! -L "$stage" && ! -e /opt/bench && ! -L /opt/bench ]]
printf 'scratch=removed /opt/bench=absent\n'
df -hT "${stage%/*}"
printf 'dispatch_scan_utc=%s since_epoch=%s\n' "$(date -u +%FT%TZ)" "$epoch"
changes=$(find /srv/zeo-gates/{inputs,work,receipts,tmp} -mindepth 1 -maxdepth 2 -newermt "@$epoch" -printf '%TY-%Tm-%TdT%TH:%TM:%TSZ %p\n' | sort)
if [[ -n "$changes" ]]; then
    printf 'dispatch_activity=overlap\n%s\n' "$changes"
else
    printf 'dispatch_activity=none_observed\n'
fi
printf 'cleanup_end=%s\n' "$(date -u +%FT%TZ)"
