#!/bin/bash
set -euo pipefail
stage=$1
[[ "$(hostname -s):$(id -un)" == zeo-internal-production-03:zeobuild ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-final-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
cd "$stage"
test ! -e EXIT.log
test ! -e WINDOW-EPOCH.txt
old=
while read -r pid args; do
    if [[ "$args" == "bash $stage/wait-quiet-prod03.sh $stage" ]]; then
        [[ -z "$old" ]]
        old=$pid
    fi
done < <(ps -u "$(id -un)" -o pid=,args=)
if [[ -n "$old" ]]; then kill -- "$old"; fi
last_sample=$(awk -F '[ =]' '$1=="sample_utc"{stamp=$2} END{print stamp}' QUIET-WAIT.log)
last_epoch=$(date -u -d "$last_sample" +%s)
now=$(date -u +%s)
read -r current rest < /proc/loadavg
elapsed=$(awk -F '[ =]' '
$1=="sample_utc" {
    elapsed=$4
    if ($6+0 >= 0.30 || (seen && elapsed-last>6)) start=-1
    if ($6+0 < 0.30 && (!seen || start<0)) start=elapsed
    last=elapsed
    seen=1
}
END {if (seen && start>=0) print last-start; else print 0}' QUIET-WAIT.log)
if (( now-last_epoch <= 6 && elapsed >= 300 )) && awk -v sample="$current" 'BEGIN{exit !(sample+0<0.30)}'; then
    printf 'verified_sample_utc=%s one_minute_load=%s\nquiet_elapsed_seconds=%s\n' "$(date -u +%FT%TZ)" "$current" "$elapsed" >> QUIET-WAIT.log
    exec bash "$stage/run-prod03.sh" "$stage"
fi
printf 'corrected_sampler_restart_utc=%s prior_eligible_seconds=%s previous_sample_age=%s\n' "$(date -u +%FT%TZ)" "$elapsed" "$((now-last_epoch))" >> QUIET-WAIT.log
exec bash "$stage/wait-quiet-fixed.sh" "$stage"
