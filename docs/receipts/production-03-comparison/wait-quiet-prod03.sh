#!/bin/bash
set -euo pipefail
umask 077
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-final-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
cd "$stage"
grep -q ' prep_exit=0$' PREP-EXIT.log
quiet_since=-1
for (( n=0; n<180; n++ )); do
    read -r load rest < /proc/loadavg
    printf 'sample_utc=%s monotonic_seconds=%s one_minute_load=%s\n' "$(date -u +%FT%TZ)" "$SECONDS" "$load" >> QUIET-WAIT.log
    if awk -v load="$load" 'BEGIN {exit !(load < 0.30)}'; then
        if (( quiet_since < 0 )); then quiet_since=$SECONDS; fi
        elapsed=$((SECONDS-quiet_since))
        if (( elapsed >= 300 )); then
            printf 'quiet_elapsed_seconds=%s\n' "$elapsed" >> QUIET-WAIT.log
            exec bash "$stage/run-prod03.sh" "$stage"
        fi
    else
        quiet_since=-1
    fi
    sleep 5
done
printf 'quiet_window_not_opened=%s\n' "$(date -u +%FT%TZ)" >> QUIET-WAIT.log
exit 5
