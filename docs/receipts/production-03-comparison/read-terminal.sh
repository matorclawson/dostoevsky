#!/bin/bash
set -euo pipefail
export LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-final-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
cd "$stage"
grep -Eq '^pipeline_phase=' EXIT.log 2>/dev/null || grep -Eq ' prep_exit=[1-9][0-9]*$' PREP-EXIT.log
dpkg-query -W -f='${Package} ${Version}\n' | sort > PACKAGES-AFTER.txt
for name in HOST.log DISPATCH.log ENGINE-VERSIONS.log PACKAGES-BEFORE.txt PACKAGES-AFTER.txt ERLANG-PROVENANCE.txt KAFKA-CONFIG-CITATIONS.txt KAFKA-GPG-IMPORT.log KAFKA-GPG.log RABBITMQ-GPG.log RABBITMQ-DIGEST.txt HEX-BUILDS.txt KAFKA-STORAGE-USAGE.log DISK-BEFORE.txt WINDOW-EPOCH.txt QUIET-WAIT.log PRECONDITIONS.txt REDIS-VENDOR.txt VALKEY-VENDOR.txt REDIS-BUILD.log HARNESS-BUILD.log redis-smoke.log valkey-smoke.log PREP-EXIT.log SMOKE.log SMOKE-CAPTURE.log PROCESS-AFTER.log KAFKA-MARKERS.txt SMOKE-KAFKA-LOGS.tar.gz TESTS.log comparison.log comparison.err EXIT.log MEDIANS.md RESULT.err MEDIANS-INCOMPLETE.md RESULT-INCOMPLETE.err EXPECTED-TREE.txt SOURCE-STATUS.log SOURCE-SHA256SUMS GATE-OVERLAP; do
    if [[ -f "$name" ]]; then sha256sum -- "$name"; fi
done
