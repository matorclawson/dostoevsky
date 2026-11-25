# Production-03 final seven-engine comparison

Harness and checker commit `6f35c74be497ed8e5a8ac0e411f5c3472adba42f` and tree `709e319b5768e2dbb58c055da0e33139a7e2b4f1`.

Memory for server engines sums client and server process peaks sampled every 50 ms. The peaks can occur at different times, and shared pages can appear in each process's peak. Kafka uses a fixed 1 GiB JVM heap. RabbitMQ uses a 1 GiB watermark.

Reopen measures in-process open and replay for Dostoevsky, JetStream and SQLite. For Redis and Valkey it includes server restart and AOF replay. For Kafka and RabbitMQ it measures server restart to readiness.

Kafka fsync per message is inferred from `log.flush.interval.messages=1`. RabbitMQ uses the vendor default durability, unverified, and the Hex.pm Erlang/OTP prebuilt. SQLite uses one writer, one connection and one transaction per publish. Redis, Valkey and Dostoevsky group syncs across parallel writes.

Summary command `go run ./cmd/plots -summary -receipt comparison.log -expect-engines dostoevsky,jetstream,sqlite,redis,valkey,kafka,rabbitmq -trials 5`.

```text
preflight_exit=0 preflight_completed_before_window=yes
start=2026-11-25T02:31:47Z command=sh benchmarks/comparison/run.sh
finish=2026-11-25T03:08:11Z comparison_exit=0
smoke_lines=21 smoke_failures=0
engine_processes=none
summary_command=go run ./cmd/plots -summary -receipt comparison.log -expect-engines dostoevsky,jetstream,sqlite,redis,valkey,kafka,rabbitmq -trials 5
result_exit=0 trial_lines=105 failed_lines=0
pipeline_phase=complete pipeline_exit=0
trial_validation_exit=0
trial_lines=105 failed_trials=0 low_memory_aborts=0 group_issues=0
lines=105 failed=0
engine=dostoevsky workers=1 trials=5 records/s=833 p99_us=3993 total_rss_KiB=9936 reopen_ms=213.48
engine=jetstream workers=1 trials=5 records/s=739 p99_us=4037 total_rss_KiB=44100 reopen_ms=18.01
engine=sqlite workers=1 trials=5 records/s=799 p99_us=3046 total_rss_KiB=16920 reopen_ms=0.61
engine=redis workers=1 trials=5 records/s=693 p99_us=4379 total_rss_KiB=32404 reopen_ms=84.48
engine=valkey workers=1 trials=5 records/s=721 p99_us=3922 total_rss_KiB=35572 reopen_ms=85.12
engine=kafka workers=1 trials=5 records/s=332 p99_us=7894 total_rss_KiB=515228 reopen_ms=4684.78
engine=rabbitmq workers=1 trials=5 records/s=439 p99_us=5504 total_rss_KiB=308204 reopen_ms=4678.07
engine=dostoevsky workers=8 trials=5 records/s=5315 p99_us=3997 total_rss_KiB=10044 reopen_ms=212.3
engine=jetstream workers=8 trials=5 records/s=803 p99_us=23124 total_rss_KiB=44180 reopen_ms=14.54
engine=sqlite workers=8 trials=5 records/s=792 p99_us=42698 total_rss_KiB=17048 reopen_ms=0.68
engine=redis workers=8 trials=5 records/s=2625 p99_us=7385 total_rss_KiB=31892 reopen_ms=83.65
engine=valkey workers=8 trials=5 records/s=2783 p99_us=6726 total_rss_KiB=35208 reopen_ms=85.39
engine=kafka workers=8 trials=5 records/s=1080 p99_us=16096 total_rss_KiB=498600 reopen_ms=4539.18
engine=rabbitmq workers=8 trials=5 records/s=2006 p99_us=8226 total_rss_KiB=318576 reopen_ms=4677.36
engine=dostoevsky workers=32 trials=5 records/s=18412 p99_us=4391 total_rss_KiB=10408 reopen_ms=210.12
engine=jetstream workers=32 trials=5 records/s=826 p99_us=70433 total_rss_KiB=44464 reopen_ms=18.83
engine=sqlite workers=32 trials=5 records/s=783 p99_us=189220 total_rss_KiB=17288 reopen_ms=0.63
engine=redis workers=32 trials=5 records/s=8384 p99_us=8866 total_rss_KiB=32380 reopen_ms=85.97
engine=valkey workers=32 trials=5 records/s=9419 p99_us=6772 total_rss_KiB=35492 reopen_ms=84.39
engine=kafka workers=32 trials=5 records/s=3168 p99_us=20427 total_rss_KiB=479656 reopen_ms=4393.4
engine=rabbitmq workers=32 trials=5 records/s=5551 p99_us=11288 total_rss_KiB=318132 reopen_ms=4500.77
```
