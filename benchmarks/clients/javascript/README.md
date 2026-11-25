# JavaScript worker for the language client benchmarks

One trial of the publisher workload from Node against one engine: 512-byte records, N concurrent synchronous producers, acknowledged only after the engine's durable acknowledgment, no retries, every acknowledged record read back, one receipt line. The server is started by `benchmarks/embedded/cmd/serve` (shared with the Go comparison); this worker never starts one. Same contract as the Python worker; offsets are BigInt.

```sh
cd benchmarks/clients/javascript && npm ci --ignore-scripts      # from the committed package-lock.json (generated on ENG-UK; all 15 integrity values checked against DEPS-js-worker-v1.md)
BENCH_TOKEN=<token from serve's READY line> node worker.mjs --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 --window 600
```

Engines: `dostoevsky` (our client, through the gateway), `kafka` (kafkajs, acks=-1, no retries, no idempotence), `nats` (@nats-io/transport-node and @nats-io/jetstream, v3), `redis` (ioredis, XADD, no retries or reconnects). The token comes from `BENCH_TOKEN`, never the command line. Stop reasons: `reached`, `window`, `failed`, `verification-failed`; a publish failure is a result, never an exit code.

Dependencies exist only here, for benchmarking: 15 packages, none with an install script, installed from a lockfile whose integrity values are checked against `DEPS-js-worker-v1.md` (in the evidence record). kafkajs 2.2.4 was last published on 2023-02-27; it is still the standard JS Kafka client, and the results say so.

Tests need only Node and no engine: `cd benchmarks/clients/javascript && npm test` (Node 24 and 26 on ENG-UK).

Receipt format (shared with the Python worker): `benchmarks/clients/receipt_examples.txt` holds the lines the workers write for fixed inputs; these tests rebuild the JavaScript ones byte for byte and `cmd/plots` parses the same file in its own tests. Every line says `result=ok` or `result=FAILED` (a publish failure, even one that lands after the target or window was reached, or a read-back that does not verify), carries `quantiles=bin-upper-1pct`, counts only acknowledged-and-verified messages in `records/s`, and never contains engine error text (only the error class and one fixed word). A timeout is reported as `ambiguous=1`: the engine may have stored the message. `client=` and `client_build=` come from the installed packages and git, never from typed names; `--require-known-build` (gated runs) refuses an unknown build. `--workers` and `--target` must be whole numbers, `--window` finite.

Run `npm ci --ignore-scripts` before `npm test`: the identity test reads the installed packages and fails, loudly, when they are missing.

`--debug-errors` prints the raw engine error of a failed read-back to stderr, for a person reproducing one cell by hand at a terminal. `run.sh` never passes it and no receipt ever contains it. Verification reads are not publishes: the Kafka consumer keeps kafkajs's default retries while it joins its group (a fresh broker creates the group coordinator on first use); publishes are still never retried.
