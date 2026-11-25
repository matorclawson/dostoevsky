#!/bin/sh
set -eu
cmp raw/PACKAGES-BEFORE.txt raw/PACKAGES-AFTER.txt
test ! -f raw/GATE-OVERLAP
! grep -q '^dispatch_activity=overlap$' CLEANUP.log || exit 2
{
    cat raw/HOST.log
    printf '\n# Reservation dispatch scans\n'
    cat raw/DISPATCH.log
    printf '\n# Dispatch scan through cleanup\n'
    sed -n '/^dispatch_scan_utc=/,$p' CLEANUP.log
} > HOST.log
for file in ENGINE-VERSIONS.log PACKAGES-BEFORE.txt PACKAGES-AFTER.txt EXIT.log ab.log ab.err BINARY-MATCH.log BINARY-SHA256SUMS BUILT-PINS.txt; do
    if test -f "raw/$file"; then cp "raw/$file" "$file"; fi
done
validation=0
if test -f ab.log; then
    python3 validate-trials.py ab.log > TRIAL-VALIDATION.log || validation=$?
    python3 report-trials.py ab.log > TRIALS.tsv
fi
{
    printf '# Production-03 Dostoevsky PR 19 A/B at 32 publishers\n\n'
    printf 'Git heads and trees\n\n```text\n'
    cat BUILT-PINS.txt
    printf '```\n\nBinary SHA-256 values\n\n```text\n'
    cat BINARY-SHA256SUMS
    printf '```\n\n'
    cat <<'REPORT'
Summary command `go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -expect-producers 32 -trials 5` ran from the candidate checkout.

Each engine ran five trials with 32 publishers and 16,000 records per trial. The first engine alternated by round.

REPORT
    printf '```text\n'
    cat EXIT.log
    if test -f TRIAL-VALIDATION.log; then
        printf 'trial_validation_exit=%s\n' "$validation"
        sed -n '1p' TRIAL-VALIDATION.log
    fi
    if test -s raw/RESULT.err; then
        printf '\nStrict summary stderr\n'
        cat raw/RESULT.err
    fi
    if test -f raw/MEDIANS-INCOMPLETE.md; then
        printf '\nSummary with -allow-incomplete\n'
        cat raw/MEDIANS-INCOMPLETE.md
    elif test -f raw/MEDIANS.md; then
        cat raw/MEDIANS.md
    fi
    printf '```\n'
    if test -f TRIALS.tsv; then
        printf '\n[Per-trial throughput and p99](TRIALS.tsv)\n'
    fi
} > RESULT.md
{
    for file in HOST.log ENGINE-VERSIONS.log PACKAGES-BEFORE.txt PACKAGES-AFTER.txt EXIT.log ab.log ab.err BINARY-MATCH.log RESULT.md CLEANUP.log; do
        if test -f "$file"; then
            lines=$(wc -l < "$file")
            bytes=$(wc -c < "$file")
            printf '%s lines=%d bytes=%d\n' "$file" "$lines" "$bytes"
        fi
    done
} > FILE-COUNTS.txt
find . -type f ! -name SHA256SUMS ! -name MAIL-SENT.json ! -name '*.pending' -print | LC_ALL=C sort | while IFS= read -r file; do shasum -a 256 "$file"; done > SHA256SUMS
shasum -a 256 -c SHA256SUMS
