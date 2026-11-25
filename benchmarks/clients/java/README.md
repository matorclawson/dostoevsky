# Java worker for the language client benchmarks

One trial of the publisher workload from Java against one engine: 512-byte records, N synchronous producer threads, acknowledged only after the engine's durable acknowledgment, no retries, every acknowledged record read back, one receipt line. Same contract, stop reasons and line format as the Python and JavaScript workers; the server is started by `benchmarks/embedded/cmd/serve`.

```sh
sh benchmarks/clients/java/build.sh                    # verified jars (deps.lock) + javac; from the repository root
BENCH_TOKEN=<token from serve's READY line> java -Xmx256m -Dbench.deps=benchmarks/clients/java/deps.lock -Dbench.repo=. \
  -cp ".dev/bench-java/classes:.dev/bench-java/lib/*" io.github.rodriveracom.dostoevsky.bench.Main \
  --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 --window 600
```

Engines: `dostoevsky` (our client through the gateway, JDK HttpClient keep-alive), `kafka` (kafka-clients: acks=all, retries=0, no idempotence, one record per batch), `nats` (jnats JetStream, synchronous), `redis` (Jedis through a pool, XADD and XRANGE by `sendCommand` so bytes stay bytes). The token comes from `BENCH_TOKEN`, never the command line. A publish failure ends the run as a result (`stop=failed`, `result=FAILED`), never an exit code; a timeout is `ambiguous=1`; error text never reaches the line (the class and a fixed word only). `--require-known-build` refuses an unknown git build of our client. `client=` names come from `deps.lock` and our client's pom, never from typed names.

**No Maven.** `build.sh` runs `fetch-deps.sh`, which downloads each of the 14 jars by exact URL from repo1.maven.org and refuses any whose SHA-256 differs from `deps.lock` (table with licenses and dates: `DEPS-java-worker-v1.md` in the evidence record), then compiles with plain `javac` (JDK 17+). redis-authx-core is a beta release that Jedis 8 depends on.

Client memory is the JVM's kernel peak (`VmHWM`) on Linux and the largest `ps` sample (`ps-sampled`) elsewhere; a JVM's resident size depends on its heap settings, so the run script fixes `-Xmx`.

Tests need only a JDK, no jars and no engine: `sh benchmarks/clients/java/test.sh` (it reads `benchmarks/clients/hist_vector.json` and `receipt_examples_java.txt`; the Go test in `cmd/plots` parses the same lines).

Licenses of the three jars whose POMs omit one are in `licenses/`: slf4j-api (MIT, from the project's tag v_1.7.36), commons-pool2 (Apache-2.0 and its NOTICE, from the jar's META-INF) and bcprov-lts8on (the Bouncy Castle licence, read from `org/bouncycastle/LICENSE.class` in the jar). Results that include Jedis say that it pulls redis-authx-core 0.1.1-beta2, Redis's own beta package, the way kafkajs is marked unmaintained.
