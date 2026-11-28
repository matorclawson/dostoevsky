#!/bin/sh
# Builds the Java worker with plain javac (JDK 17+): fetches and verifies the pinned jars, then compiles the worker
# together with our own Java client source. Output: $1 (default .dev/bench-java), classes/ and lib/.
# Run from the repository root.  Then:
#   BENCH_TOKEN=... java -Dbench.deps=benchmarks/clients/java/deps.lock -Dbench.repo=. -Xmx256m \
#     -cp ".dev/bench-java/classes:.dev/bench-java/lib/*" io.github.rodriveracom.dostoevsky.bench.Main --engine kafka ...
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=${1:-.dev/bench-java}
sh "$here/fetch-deps.sh" "$out/lib"
mkdir -p "$out/classes"
"${JAVAC:-javac}" --release 17 -encoding UTF-8 -cp "$out/lib/*" -d "$out/classes" \
  clients/java/src/main/java/io/github/rodriveracom/dostoevsky/*.java \
  "$here"/src/io/github/rodriveracom/dostoevsky/bench/*.java
echo "built $out/classes"
