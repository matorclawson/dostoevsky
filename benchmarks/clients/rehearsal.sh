#!/bin/sh
# The 60-second rehearsal of every cell on ENG-UK: builds what is needed, writes a meta record, runs run.sh with one
# round and a 60-second window, and leaves everything under one .noindex directory (no Spotlight indexing during a run).
# From the repository root:   sh benchmarks/clients/rehearsal.sh
# Nothing is pasted by hand: the locations below are the ones the runtime inventory lists; override with the variables.
set -eu
rt=${ZEO_RUNTIMES:-/opt/zeo/runtimes}
nats=${NATS_SERVER:-/Users/rodbot/zeo/runtimes-local/nats-server-v2.15.3}
py=${PYTHON3:-python3}
window=${REHEARSAL_WINDOW:-60}
find_one() { # directory, shell pattern (matched against the path) -> the first file that matches, or exit 2
  p=$(find "$1" -maxdepth 6 -type f -path "$2" 2>/dev/null | sort | head -1)
  [ -n "$p" ] || { echo "cannot find $2 under $1" >&2; exit 2; }
  echo "$p"
}
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

redis=$(find_one "$rt/redis-8.12.0" '*/redis-server')
kafka_start=$(find_one "$rt/kafka-4.4.0" '*/bin/kafka-server-start.sh')
kafka_home=$(dirname "$(dirname "$kafka_start")")
java=$(find_one "$rt/temurin-21.0.13.1+2" '*/bin/java')
java_home=$(dirname "$(dirname "$java")")
[ -x "$nats" ] || { echo "no nats-server at $nats" >&2; exit 2; }
"$nats" --version | grep -q 'v2.15.3' || { echo "$nats is not nats-server v2.15.3" >&2; exit 2; }

stamp=$(date -u +%Y%m%dT%H%M%SZ)
out=.dev/clients/rehearsal-$stamp.noindex
mkdir -p "$out/tmp" .dev/clients
export TMPDIR="$PWD/$out/tmp"

# Build, once per host: the server driver, the gateway, the Python venv (hash-pinned), the JavaScript packages (lockfile), the Java worker.
(cd benchmarks/embedded && CGO_ENABLED=0 go build -mod=readonly -buildvcs=false -o ../../.dev/clients/serve ./cmd/serve)
CGO_ENABLED=0 go build -mod=readonly -buildvcs=false -o .dev/clients/dostoevsky-gateway ./cmd/dostoevsky-gateway
[ -x .dev/bench-venv/bin/python ] || "$py" -m venv .dev/bench-venv
.dev/bench-venv/bin/pip install --quiet --require-hashes -r benchmarks/clients/python/requirements.txt
(cd benchmarks/clients/javascript && npm ci --ignore-scripts --silent)
JAVAC="$java_home/bin/javac" PATH="$java_home/bin:$PATH" sh benchmarks/clients/java/build.sh

{
  echo "rehearsal=$stamp window_s=$window rounds=1"
  echo "host=$(uname -srm)"
  echo "git_head=$(git rev-parse HEAD) dirty=$(git status --porcelain | wc -l | tr -d ' ')"
  echo "nats-server=$("$nats" --version) sha256=$(sha "$nats")"
  echo "redis-server=$("$redis" --version) sha256=$(sha "$redis")"
  echo "kafka_home=$kafka_home"
  echo "java=$("$java" -version 2>&1 | head -1) home=$java_home"
  echo "node=$(node --version) python=$(.dev/bench-venv/bin/python --version 2>&1)"
  echo "serve sha256=$(sha .dev/clients/serve)"
  echo "gateway sha256=$(sha .dev/clients/dostoevsky-gateway)"
  echo "memory_sources: servers and the Java and Python clients VmHWM on Linux, ps-sampled on macOS (never compared); JavaScript maxRSS"
} > "$out/meta.txt"

status=0
env CLIENT_LANGS="${CLIENT_LANGS:-python javascript java}" CLIENT_ENGINES="${CLIENT_ENGINES:-dostoevsky kafka nats redis}" \
    SERVE_BIN=.dev/clients/serve PYTHON_BIN=.dev/bench-venv/bin/python \
    BENCH_NATS_SERVER="$nats" BENCH_REDIS_SERVER="$redis" BENCH_GATEWAY_SERVER="$PWD/.dev/clients/dostoevsky-gateway" \
    BENCH_KAFKA_HOME="$kafka_home" BENCH_JAVA_HOME="$java_home" PATH="$java_home/bin:$PATH" \
    BENCH_ROUNDS=1 BENCH_WINDOW="$window" BENCH_TARGET="${BENCH_TARGET:-100000000}" \
    sh benchmarks/clients/run.sh > "$out/receipt.log" 2> "$out/stderr.log" || status=$?
echo "$status" > "$out/exit"
echo "rehearsal finished with exit $status: $out (receipt.log, stderr.log, meta.txt, exit)"
exit "$status"
