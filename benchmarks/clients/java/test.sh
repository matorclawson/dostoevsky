#!/bin/sh
# Compiles the worker's core and its tests with only a JDK (17+): no jars, no network, no engine.
# From the repository root:  sh benchmarks/clients/java/test.sh
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
"${JAVAC:-javac}" --release 17 -encoding UTF-8 -d "$out" \
  "$here/src/io/github/rodriveracom/dostoevsky/bench/Core.java" \
  "$here/src/io/github/rodriveracom/dostoevsky/bench/Worker.java" \
  "$here/src/io/github/rodriveracom/dostoevsky/bench/Build.java" \
  "$here/test/io/github/rodriveracom/dostoevsky/bench/CoreTest.java"
java -ea -cp "$out" io.github.rodriveracom.dostoevsky.bench.CoreTest "$here/.."
