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
for file in ENGINE-VERSIONS.log PACKAGES-BEFORE.txt PACKAGES-AFTER.txt TESTS.log EXIT.log PREP-EXIT.log SMOKE.log SMOKE-CAPTURE.log PROCESS-AFTER.log ERLANG-PROVENANCE.txt comparison.log comparison.err; do
    if test -f "raw/$file"; then cp "raw/$file" "$file"; fi
done
! grep -q '^--- SKIP:' TESTS.log || exit 2
validation=0
if test -s comparison.log; then python3 validate-trials.py comparison.log > TRIAL-VALIDATION.log || validation=$?; fi
{
    cat <<'REPORT'
# Production-03 final seven-engine comparison

Harness and checker commit `6f35c74be497ed8e5a8ac0e411f5c3472adba42f` and tree `709e319b5768e2dbb58c055da0e33139a7e2b4f1`.

Memory for server engines sums client and server process peaks sampled every 50 ms. The peaks can occur at different times, and shared pages can appear in each process's peak. Kafka uses a fixed 1 GiB JVM heap. RabbitMQ uses a 1 GiB watermark.

Reopen measures in-process open and replay for Dostoevsky, JetStream and SQLite. For Redis and Valkey it includes server restart and AOF replay. For Kafka and RabbitMQ it measures server restart to readiness.

Kafka fsync per message is inferred from `log.flush.interval.messages=1`. RabbitMQ uses the vendor default durability, unverified, and the Hex.pm Erlang/OTP prebuilt. SQLite uses one writer, one connection and one transaction per publish. Redis, Valkey and Dostoevsky group syncs across parallel writes.

Summary command `go run ./cmd/plots -summary -receipt comparison.log -expect-engines dostoevsky,jetstream,sqlite,redis,valkey,kafka,rabbitmq -trials 5`.

REPORT
    if test -f raw/MEDIANS-INCOMPLETE.md; then
        printf 'Strict summary exited 1. The second summary command added `-allow-incomplete`.\n\n'
    fi
    printf '```text\n'
    cat EXIT.log
    if test -f TRIAL-VALIDATION.log; then
        printf 'trial_validation_exit=%s\n' "$validation"
        sed -n '1p' TRIAL-VALIDATION.log
    fi
    if test -s raw/RESULT.err; then
        printf 'Strict summary stderr\n'
        cat raw/RESULT.err
    fi
    if test -f raw/MEDIANS-INCOMPLETE.md; then
        printf 'Second summary with -allow-incomplete\n'
        cat raw/MEDIANS-INCOMPLETE.md
    elif test -f raw/MEDIANS.md; then
        cat raw/MEDIANS.md
    else
        printf 'Measured comparison did not start.\n'
        if test -f SMOKE.log; then cat SMOKE.log; fi
    fi
    printf '```\n'
} > RESULT.md
{
    for file in HOST.log ENGINE-VERSIONS.log PACKAGES-BEFORE.txt PACKAGES-AFTER.txt TESTS.log EXIT.log SMOKE.log SMOKE-CAPTURE.log PROCESS-AFTER.log comparison.log comparison.err RESULT.md CLEANUP.log; do
        if test -f "$file"; then
            lines=$(wc -l < "$file")
            bytes=$(wc -c < "$file")
            printf '%s lines=%d bytes=%d\n' "$file" "$lines" "$bytes"
        fi
    done
} > FILE-COUNTS.txt
find . -type f ! -name SHA256SUMS ! -name MAIL-SENT.json ! -name '*.pending' -print | LC_ALL=C sort | while IFS= read -r file; do shasum -a 256 "$file"; done > SHA256SUMS
shasum -a 256 -c SHA256SUMS
