# Python worker for the language client benchmarks

One trial of the publisher workload from Python against one engine: 512-byte records, N synchronous producers, acknowledged only after the engine's durable acknowledgment, no retries, every acknowledged record read back, one receipt line. The server is started by `benchmarks/embedded/cmd/serve` (shared with the Go comparison); this worker never starts one.

```sh
python3 -m venv .dev/bench-venv && .dev/bench-venv/bin/pip install --require-hashes -r benchmarks/clients/python/requirements.txt
BENCH_TOKEN=<token from serve's READY line> PYTHONPATH=clients/python \
  .dev/bench-venv/bin/python benchmarks/clients/python/worker.py --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 --window 600
```

Engines: `dostoevsky` (our client, through the gateway), `kafka` (confluent-kafka, acks=all, no retries, no idempotence, one message per batch), `nats` (nats-py JetStream), `redis` (redis-py, XADD, no client retries). The token comes from `BENCH_TOKEN`, never the command line. A publish failure ends the run as a result (`stop=failed`), never an exit code. Stop reasons: `reached`, `window`, `failed`, `verification-failed`.

The receipt line says `result=ok` or `result=FAILED` (a publish failure, even one that lands after the target or window was already reached, or a read-back that does not verify); `records/s` counts only acknowledged-and-verified messages. Engine error text never reaches the line: only the exception class and one fixed word (`timeout`, `connection`, `protocol`, `engine-error`). `client=` is the package and version from the installed metadata, and `client_build=` is the git commit (with `-dirty` when changed) of our own client; the worker refuses to run with a dostoevsky_client that has no connection pool. It adds `p99_9_us`, `p99_99_us`, `client_rss_KiB` (the kernel's peak, `ru_maxrss`) and `client=` with the exact library version; the driver appends the server's `server_rss_KiB` and its `rss_source`. The histogram is the Go harness's (1% bins) and both are checked against `benchmarks/clients/hist_vector.json`.

Tests need only the standard library: `python3 -m unittest discover -s benchmarks/clients/python/tests`.
Not measured here: publish-to-consume latency (a separate open-loop mode, later) and reopen.

Receipt format: `benchmarks/clients/receipt_examples.txt` holds the lines these workers write for fixed inputs; the tests rebuild them byte for byte, and `cmd/plots` parses the same file in its own tests. Every line carries `quantiles=bin-upper-1pct`; client names are whitespace-free; a Kafka delivery timeout is reported as `ambiguous=1` (the broker may have stored the message) and the run stops as failed. `--require-known-build` (used by gated runs) refuses to run when our client's git build cannot be identified. The Python tests read `clients/python/pyproject.toml`, so run them from a full checkout.
