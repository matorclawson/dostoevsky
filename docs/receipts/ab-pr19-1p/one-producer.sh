#!/bin/bash
set -euo pipefail
base=$1
cand=$2
failed=0
trial() {
    local label=$1 binary=$2 round=$3 line status=0
    line=$("$binary" -workers 1 -count 16000) || status=$?
    if [[ "$status" == 0 ]]; then
        printf 'trial=%s engine=%s%s\n' "$round" "$label" "${line#engine=dostoevsky}"
    else
        failed=$((failed + 1))
        printf 'trial=%s engine=%s workers=1 FAILED exit=%s\n' "$round" "$label" "$status"
    fi
}
for round in {1..15}; do
    if (( round % 2 )); then
        trial baseline "$base" "$round"
        trial candidate "$cand" "$round"
    else
        trial candidate "$cand" "$round"
        trial baseline "$base" "$round"
    fi
done
(( failed == 0 ))
