#!/bin/bash
set -euo pipefail
umask 077
export PATH=/usr/bin:/bin:/usr/sbin:/sbin LC_ALL=C
stage=$1
[[ "$(hostname -s):$(uname -sm):$(id -un)" == 'zeo-internal-production-03:Linux aarch64:zeobuild' ]]
[[ "$stage" =~ ^/srv/zeo-gates/home/bench-final-[0-9]{8}T[0-9]{6}Z\.[[:alnum:]]{8}$ && ! -L "$stage" && -O "$stage" ]]
cd "$stage"
[[ "$(findmnt -T . -n -o FSTYPE)" == ext4 ]]
printf 'prep_start=%s\n' "$(date -u +%FT%TZ)" > PREP-EXIT.log
prep_finish() {
    code=$?
    if [[ -n "${engine_pid:-}" ]]; then
        kill "$engine_pid" 2>/dev/null || true
        wait "$engine_pid" 2>/dev/null || true
    fi
    printf 'prep_end=%s prep_exit=%s\n' "$(date -u +%FT%TZ)" "$code" >> "$stage/PREP-EXIT.log"
}
trap prep_finish EXIT
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server'; then exit 4; fi
awk '/^MemAvailable:/{seen=1; ok=$2>=2097152} END{exit !(seen && ok)}' /proc/meminfo
(( $(df -B1 --output=avail . | tail -1) >= 8 * 1024 * 1024 * 1024 ))
df -B1 . > DISK-BEFORE.txt
dpkg-query -W -f='${Package} ${Version}\n' | sort > PACKAGES-BEFORE.txt
{
    date -u +%FT%TZ
    command -v gcc cc make perl
    gcc --version | sed -n '1p'
    make --version | sed -n '1p'
    perl -v | sed -n '2p'
    status=0
    printf '#include <openssl/ssl.h>\n' | gcc -E -x c - >/dev/null || status=$?
    printf 'openssl_header_check_exit=%s\n' "$status"
    ssl_status=$status
    status=0
    printf '#include <ncurses.h>\n' | gcc -E -x c - >/dev/null || status=$?
    printf 'ncurses_header_check_exit=%s\n' "$status"
    (( ssl_status != 0 || status != 0 ))
} > ERLANG-PROVENANCE.txt 2>&1
sha256sum -c SOURCE-SHA256SUMS
git clone --quiet --branch bench --no-tags "$stage/source.bundle" "$stage/checkout"
head=$(git -C checkout rev-parse HEAD)
tree=$(git -C checkout rev-parse 'HEAD^{tree}')
[[ "$head:$tree" == "6f35c74be497ed8e5a8ac0e411f5c3472adba42f:$(cat EXPECTED-TREE.txt)" ]]
mkdir -p dl tmp go/toolchain go/mod go/cache go/path go/config jdk-21 erlang-27.3.4.20 kafka_2.13-4.4.0 rabbitmq_server-4.4.1
export TMPDIR="$stage/tmp" GNUPGHOME="$stage/gnupg"
mkdir -m 700 "$GNUPGHOME"
dl() { curl -qfsSL --proto '=https' --proto-redir '=https' --retry 2 --max-time 300 "$1" -o "$2"; }
check256() {
    [[ "$1" =~ ^[A-Fa-f0-9]{64}$ ]]
    printf '%s  %s\n' "$1" "$2" | sha256sum -c
    sha256sum "$2"
}
cd dl
exec >> "$stage/ENGINE-VERSIONS.log" 2>&1
printf 'HEAD=%s\nTREE=%s\n' "$head" "$tree"
temurin=OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.13.1_2.tar.gz
tbase=https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13.1%2B2
dl "$tbase/$temurin.sha256.txt" "$temurin.sha256.txt"
dl "$tbase/$temurin" "$temurin"
cat "$temurin.sha256.txt"
check256 "$(awk 'NR==1 {print $1}' "$temurin.sha256.txt")" "$temurin"
kafka=kafka_2.13-4.4.0.tgz
kbase=https://downloads.apache.org/kafka/4.4.0
dl "$kbase/$kafka.sha512" "$kafka.sha512"
dl "$kbase/$kafka.asc" "$kafka.asc"
dl https://downloads.apache.org/kafka/KEYS KAFKA-KEYS
dl "$kbase/$kafka" "$kafka"
cat "$kafka.sha512"
khash=$(sed '1s/^[^:]*://' "$kafka.sha512" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
[[ "$khash" =~ ^[a-f0-9]{128}$ ]]
printf '%s  %s\n' "$khash" "$kafka" | sha512sum -c
sha256sum "$kafka" KAFKA-KEYS
gpg --batch --no-tty --import KAFKA-KEYS > "$stage/KAFKA-GPG-IMPORT.log" 2>&1
gpg --batch --no-tty --status-fd 1 --verify "$kafka.asc" "$kafka" > "$stage/KAFKA-GPG.log" 2>&1
cat "$stage/KAFKA-GPG.log"
grep -q '^\[GNUPG:\] VALIDSIG ' "$stage/KAFKA-GPG.log"
rabbit=rabbitmq-server-generic-unix-4.4.1.tar.xz
rbase=https://github.com/rabbitmq/rabbitmq-server/releases/download/v4.4.1
dl https://api.github.com/repos/rabbitmq/rabbitmq-server/releases/tags/v4.4.1 RABBITMQ-RELEASE.json
jq -er --arg name "$rabbit" '.assets[] | select(.name==$name) | .digest' RABBITMQ-RELEASE.json > "$stage/RABBITMQ-DIGEST.txt"
dl "$rbase/$rabbit" "$rabbit"
dl "$rbase/$rabbit.asc" "$rabbit.asc"
dl https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc RABBITMQ-KEY.asc
cat "$stage/RABBITMQ-DIGEST.txt"
rhash=$(sed 's/^sha256://' "$stage/RABBITMQ-DIGEST.txt")
check256 "$rhash" "$rabbit"
rfpr=0A9AF2115F4687BD29803A206B73A36E6026DFCA
[[ "$(gpg --batch --no-tty --with-colons --show-keys RABBITMQ-KEY.asc | awk -F: '$1=="fpr"{print $10; exit}')" == "$rfpr" ]]
gpg --batch --no-tty --import RABBITMQ-KEY.asc
gpg --batch --no-tty --status-fd 1 --verify "$rabbit.asc" "$rabbit" > "$stage/RABBITMQ-GPG.log" 2>&1
cat "$stage/RABBITMQ-GPG.log"
awk -v fpr="$rfpr" '$2=="VALIDSIG" && ($3==fpr || $NF==fpr){valid=1} END{exit !valid}' "$stage/RABBITMQ-GPG.log"
gpgconf --kill all
otp=OTP-27.3.4.20
obase=https://builds.hex.pm/builds/otp/arm64/ubuntu-26.04
dl "$obase/builds.txt" "$stage/HEX-BUILDS.txt"
dl "$obase/$otp.tar.gz" "$otp.tar.gz"
{
    printf 'source=%s/%s.tar.gz\nindex=%s/builds.txt\n' "$obase" "$otp" "$obase"
    row=$(awk -v tag="$otp" '$1==tag{print; n++} END{exit n!=1}' "$stage/HEX-BUILDS.txt")
    printf '%s\n' "$row"
    osha=$(awk '{print $4}' <<<"$row")
    if [[ -n "$osha" ]]; then
        check256 "$osha" "$otp.tar.gz"
        printf 'hex_index_sha256=matched\n'
    else
        sha256sum "$otp.tar.gz"
        printf 'hex_index_sha256=absent\n'
    fi
} >> "$stage/ERLANG-PROVENANCE.txt"
dl https://go.dev/dl/go1.26.0.linux-arm64.tar.gz go1.26.0.linux-arm64.tar.gz
check256 bd03b743eb6eb4193ea3c3fd3956546bf0e3ca5b7076c8226334afe6b75704cd go1.26.0.linux-arm64.tar.gz
tar -xzf "$temurin" --strip-components=1 -C "$stage/jdk-21"
tar -xzf "$kafka" --strip-components=1 -C "$stage/kafka_2.13-4.4.0"
tar -xJf "$rabbit" --strip-components=1 -C "$stage/rabbitmq_server-4.4.1"
tar -xzf "$otp.tar.gz" --strip-components=1 -C "$stage/erlang-27.3.4.20"
tar -xzf go1.26.0.linux-arm64.tar.gz -C "$stage/go/toolchain"
cd "$stage/erlang-27.3.4.20"
./Install -minimal "$PWD"
export PATH="$PWD/bin:$PATH"
ldd "$PWD/lib/erlang/erts-"*/bin/beam.smp 2>/dev/null || ldd "$PWD/erts-"*/bin/beam.smp
{
    erl -noshell -eval 'io:format("otp_release=~s~n",[erlang:system_info(otp_release)]), "27" = erlang:system_info(otp_release), halt().'
    erl -noshell -eval 'io:format("crypto_hashes=~p~nopenssl=~p~n",[crypto:supports(hashs),crypto:info_lib()]), halt().'
    for app in crypto ssl public_key inets xmerl mnesia os_mon sasl runtime_tools syntax_tools tools eldap; do
        match=$(find "$PWD" -maxdepth 5 -type f -name "$app.app" -print)
        [[ -n "$match" ]]
        printf 'application=%s path=%s\n' "$app" "$match"
    done
    printf 'provenance=hex-prebuilt\n'
} >> "$stage/ERLANG-PROVENANCE.txt"
"$stage/jdk-21/bin/java" -version
export JAVA_HOME="$stage/jdk-21"
"$stage/kafka_2.13-4.4.0/bin/kafka-storage.sh" --help > "$stage/KAFKA-STORAGE-USAGE.log" 2>&1
sed -n '1,3p' "$stage/KAFKA-STORAGE-USAGE.log"
test -x "$stage/rabbitmq_server-4.4.1/sbin/rabbitmq-server"
printf 'rabbit_server=present\n'

cd "$stage"
printf 'redis_toolchain\n'
dpkg-query -W -f='${Package} ${Version}\n' gcc make libc6-dev
gcc --version | sed -n '1p'
make --version | sed -n '1p'
printf 'apt_package_changes=none\n'
cd "$stage/dl"
dl https://raw.githubusercontent.com/redis/redis-hashes/master/README redis-hashes.README
awk '$1=="hash"&&$2=="redis-8.12.0.tar.gz"&&$3=="sha256"{print; n++} END {exit n!=1}' redis-hashes.README > "$stage/REDIS-VENDOR.txt"
dl https://download.redis.io/releases/redis-8.12.0.tar.gz redis-8.12.0.tar.gz
cat "$stage/REDIS-VENDOR.txt"
check256 "$(awk '{print $4}' "$stage/REDIS-VENDOR.txt")" redis-8.12.0.tar.gz
dl https://download.valkey.io/releases/valkey-9.2.0-noble-arm64.tar.gz.sha256 "$stage/VALKEY-VENDOR.txt"
dl https://download.valkey.io/releases/valkey-9.2.0-noble-arm64.tar.gz valkey-9.2.0-noble-arm64.tar.gz
cat "$stage/VALKEY-VENDOR.txt"
[[ "$(awk 'NR==1{print $2}' "$stage/VALKEY-VENDOR.txt")" == valkey-9.2.0-noble-arm64.tar.gz ]]
check256 "$(awk 'NR==1{print $1}' "$stage/VALKEY-VENDOR.txt")" valkey-9.2.0-noble-arm64.tar.gz
tar -xzf redis-8.12.0.tar.gz -C "$stage"
mkdir "$stage/valkey"
tar -xzf valkey-9.2.0-noble-arm64.tar.gz -C "$stage/valkey"
printf 'redis_build=make -C src -j2 BUILD_TLS=no\n' >> "$stage/ENGINE-VERSIONS.log"
(cd "$stage/redis-8.12.0" && make -C src -j2 BUILD_TLS=no) > "$stage/REDIS-BUILD.log" 2>&1
redis="$stage/redis-8.12.0/src/redis-server"
mapfile -t vbins < <(find "$stage/valkey" -type f -path '*/bin/valkey-server')
[[ "${#vbins[@]}" == 1 ]]
valkey=${vbins[0]}
export REDIS_BIN="$redis" VALKEY_BIN="$valkey"
phase=engine-smoke
smoke_engine() {
    local label=$1 server=$2 cli=$3 pong=''
    smoke="$TMPDIR/$label-smoke"
    mkdir "$smoke"
    "$server" --version >> "$stage/ENGINE-VERSIONS.log"
    "$server" --port 0 --unixsocket "$smoke.sock" --unixsocketperm 700 --save '' --appendonly no --dir "$smoke" > "$stage/$label-smoke.log" 2>&1 &
    engine_pid=$!
    for n in {1..50}; do
        if pong=$("$cli" -s "$smoke.sock" ping 2>/dev/null) && [[ "$pong" == PONG ]]; then break; fi
        kill -0 "$engine_pid"
        sleep 0.1
    done
    [[ "$pong" == PONG ]]
    "$cli" -s "$smoke.sock" shutdown nosave
    wait "$engine_pid"
    engine_pid=''
    printf '%s_smoke=PONG stopped=yes\n' "$label" >> "$stage/ENGINE-VERSIONS.log"
}
smoke_engine redis "$redis" "${redis%/*}/redis-cli"
smoke_engine valkey "$valkey" "${valkey%/*}/valkey-cli"

printf '%s\n' "$REDIS_BIN" > "$stage/REDIS-BIN.txt"
printf '%s\n' "$VALKEY_BIN" > "$stage/VALKEY-BIN.txt"
unset GOFLAGS GO111MODULE
export PATH="$stage/go/toolchain/go/bin:$PATH" GO="$stage/go/toolchain/go/bin/go"
export TMPDIR="$stage/tmp" GOTMPDIR="$stage/tmp" GOCACHE="$stage/go/cache" GOMODCACHE="$stage/go/mod"
export GOPATH="$stage/go/path" XDG_CONFIG_HOME="$stage/go/config" GOENV=off GOTOOLCHAIN=local GOMAXPROCS=2
{
    cd "$stage/checkout"
    go version
    go mod download
    go mod verify
    cd benchmarks/embedded
    export GOFLAGS=-mod=readonly
    go mod download
    go mod verify
    go vet ./...
    go test -count=1 -v ./...
} > "$stage/TESTS.log" 2>&1
if grep -Eq '^[[:blank:]]*--- SKIP:' "$stage/TESTS.log"; then exit 3; fi
(
    cd "$stage/checkout/benchmarks/embedded"
    CGO_ENABLED=0 go build -mod=readonly -buildvcs=false -o ../../.dev/embedded/ ./cmd/...
) > "$stage/HARNESS-BUILD.log" 2>&1
if pgrep -xa 'java|beam\.smp|epmd|redis-server|valkey-server'; then exit 4; fi
printf 'private_socket_checks=pass harness_preflight=pass\n' > "$stage/PRECONDITIONS.txt"
