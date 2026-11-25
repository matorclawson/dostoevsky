#!/bin/bash
set -euo pipefail
export LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-ab19-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
cd "$stage"
grep -q '^pipeline_phase=' EXIT.log
dpkg-query -W -f='${Package} ${Version}\n' | sort > PACKAGES-AFTER.txt
for name in HOST.log DISPATCH.log ENGINE-VERSIONS.log PACKAGES-BEFORE.txt PACKAGES-AFTER.txt ab.log ab.err CRASH-TESTS.log BINARY-SHA256SUMS BUILT-PINS.txt baseline-BUILD.log candidate-BUILD.log EXIT.log MEDIANS.md RESULT.err MEDIANS-INCOMPLETE.md RESULT-INCOMPLETE.err EXPECTED-PINS.txt SOURCE-STATUS.log SOURCE-SHA256SUMS GATE-OVERLAP; do
    if [[ -f "$name" ]]; then sha256sum -- "$name"; fi
done
