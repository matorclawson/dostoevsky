# Production-03 Dostoevsky PR 19 A/B

Git heads and trees

```text
baseline 1ce4a6d1a514236f9b2a698046ca15fad338ccb3 d8172ab1564cbe2455f1b9d6c7e0801cb6abee68
candidate 6d2b9eaf7ec55e5d17f9f7c73cc65ef5b19120c3 981d86123e2fa75e47931fcb0746e70792ab365f
```

Binary SHA-256 values

```text
cdc12fb97b71ade59f3d4fda2e850e50fe20f017394f79a8cb5f66107bd76cb1  bin/dost-baseline
50256d3aa7c8aa699d566f56abb7cc9992f8944ad768a35ce8e2f5fbd8b6c870  bin/dost-candidate
```

Summary command `go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -trials 5` ran from the candidate checkout.

Crash command `go test -count=3 -run TestConcurrentPublishKill .` ran from the candidate checkout with verbose output enabled.

```text
start=2026-11-25T00:50:00Z command=sh benchmarks/comparison/ab.sh BASELINE_BIN CANDIDATE_BIN
finish=2026-11-25T00:54:32Z ab_exit=0
summary_command=go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -trials 5
summary_exit=0
crash_test_exit=0
pipeline_phase=complete pipeline_exit=0
trial_validation_exit=0
trial_lines=30 failed_trials=0 low_memory_aborts=0 group_issues=0
lines=30 failed=0
engine=baseline workers=1 trials=5 records/s=832 p99_us=4053 total_rss_KiB=9848 reopen_ms=211.11
engine=candidate workers=1 trials=5 records/s=812 p99_us=3677 total_rss_KiB=9932 reopen_ms=211.1
engine=baseline workers=8 trials=5 records/s=3059 p99_us=6375 total_rss_KiB=10200 reopen_ms=208.65
engine=candidate workers=8 trials=5 records/s=5226 p99_us=4058 total_rss_KiB=10316 reopen_ms=211.78
engine=baseline workers=32 trials=5 records/s=7409 p99_us=9118 total_rss_KiB=10412 reopen_ms=215.38
engine=candidate workers=32 trials=5 records/s=16653 p99_us=5141 total_rss_KiB=10380 reopen_ms=211.06
```
