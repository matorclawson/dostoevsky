#!/bin/bash
set -euo pipefail
export LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-final-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
grep -Eq '^pipeline_phase=' "$stage/EXIT.log" 2>/dev/null || grep -Eq ' prep_exit=[1-9][0-9]*$' "$stage/PREP-EXIT.log"
epoch=$(cat "$stage/WINDOW-EPOCH.txt" 2>/dev/null || date +%s)
(( epoch > 0 ))
printf 'cleanup_start=%s\nstage=%s\n' "$(date -u +%FT%TZ)" "$stage"
df -hT "$stage"
free_before=$(awk 'NR==2{print $4}' "$stage/DISK-BEFORE.txt")
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server|kafka|rabbitmq-server'; then
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
if [[ -d "$stage/go/mod" ]]; then
    find "$stage/go/mod" -xdev -type d -uid "$(id -u)" -exec chmod u+w -- {} +
fi
find "$stage" -xdev -depth -delete
[[ ! -e "$stage" && ! -L "$stage" && ! -e /opt/bench && ! -L /opt/bench ]]
printf 'scratch=removed /opt/bench=absent\n'
df -hT "${stage%/*}"
free_after=$(df -B1 --output=avail "${stage%/*}" | tail -1)
printf 'free_bytes_before=%s free_bytes_after=%s difference_bytes=%s\n' "$free_before" "$free_after" "$((free_after-free_before))"
printf 'dispatch_scan_utc=%s since_epoch=%s\n' "$(date -u +%FT%TZ)" "$epoch"
changes=$(find /srv/zeo-gates/{inputs,work,receipts,tmp} -mindepth 1 -maxdepth 2 -newermt "@$epoch" -printf '%TY-%Tm-%TdT%TH:%TM:%TSZ %p\n' | sort)
if [[ -n "$changes" ]]; then
    printf 'dispatch_activity=overlap\n%s\n' "$changes"
else
    printf 'dispatch_activity=none_observed\n'
fi
printf 'cleanup_end=%s\n' "$(date -u +%FT%TZ)"
