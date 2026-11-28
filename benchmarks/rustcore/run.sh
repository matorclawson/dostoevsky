#!/bin/sh
# A/B of the Rust write-path prototype against the Go engine on one machine.
# Usage: benchmarks/rustcore/run.sh WORKDIR OUTDIR [stage ...]   stages: env conform write sustained reopen read
# Rules fixed before the run are in benchmarks/rustcore/README.md.
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$1"; OUT="$2"; shift 2
STAGES="${*:-env conform write sustained reopen read}"
GO="$WORK/bin/goab"; RS="$ROOT/benchmarks/rustcore/dostoevsky-core/target/release/rsab"
PAYLOAD="${PAYLOAD:-100}"; TRIALS="${TRIALS:-3}"; SECONDS_EACH="${SECONDS_EACH:-20}"; SUSTAINED="${SUSTAINED:-1800}"; KEEP_SEGMENTS="${KEEP_SEGMENTS:-64}"
mkdir -p "$WORK/bin" "$OUT"
(cd "$ROOT" && go build -trimpath -o "$GO" ./benchmarks/rustcore/goab)
(cd "$ROOT/benchmarks/rustcore/dostoevsky-core" && cargo build --release --offline 2>/dev/null)
engine() { if [ "$1" = go ]; then shift; "$GO" "$@"; else shift; "$RS" "$@"; fi; }
drop() { sync; echo 3 > /proc/sys/vm/drop_caches; }
field() { sed -n "s/.*\"$1\":\"\{0,1\}\([^,\"}]*\).*/\1/p"; }

for stage in $STAGES; do case $stage in
env)
  { echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "kernel: $(uname -srvm)"
    echo "os: $(. /etc/os-release; echo $PRETTY_NAME)"
    lscpu | grep -E "Architecture|Model name|^CPU\(s\)|Thread|Core|Hypervisor" | sed 's/^/cpu: /'
    echo "mem: $(grep -E 'MemTotal|SwapTotal' /proc/meminfo | tr -s ' ' | tr '\n' ';')"
    echo "filesystem: $(findmnt -no SOURCE,FSTYPE,OPTIONS -T "$WORK")"
    echo "device_write_cache: $(cat /sys/block/vda/queue/write_cache 2>/dev/null) rotational=$(cat /sys/block/vda/queue/rotational 2>/dev/null)"
    echo "go: $(go version) GOGC=${GOGC:-100 (default)} GOMEMLIMIT=${GOMEMLIMIT:-unset} GOMAXPROCS=default"
    echo "rustc: $(rustc -V); release, opt-level=3, lto, codegen-units=1, panic=abort; system allocator (glibc malloc)"
    echo "durability: both engines acknowledge a publish after pwrite + fdatasync of the active segment; segment creation: fsync file, rename, fsync directory"
    echo "payload_bytes: $PAYLOAD  key: empty  id: empty  topic: events (1 partition)"
    echo "go_commit: $(cd "$ROOT" && git rev-parse HEAD)"
    echo "binary_bytes: goab=$(stat -c %s "$GO") rsab=$(stat -c %s "$RS")"
  } > "$OUT/environment.txt" ;;
conform)
  : > "$OUT/conformance.txt"
  for writer in go rust; do
    other=rust; [ $writer = rust ] && other=go
    d="$WORK/conf-$writer"; rm -rf "$d"
    engine $writer fill --dir "$d" --records 300000 --payload $PAYLOAD --batch 500 >> "$OUT/conformance.txt"
    a=$(engine go digest --dir "$d"); b=$(engine rust digest --dir "$d")
    echo "written by $writer: go reads $(echo "$a" | field records) digest $(echo "$a" | field digest); rust reads $(echo "$b" | field records) digest $(echo "$b" | field digest)" >> "$OUT/conformance.txt"
    # the other engine appends under concurrency (group commit, rollovers) to the same store
    engine $other write --dir "$d" --producers 8 --seconds 3 --payload $PAYLOAD > /dev/null
    engine $writer write --dir "$d" --producers 8 --seconds 3 --payload $PAYLOAD > /dev/null
    a=$(engine go digest --dir "$d"); b=$(engine rust digest --dir "$d")
    echo "then appended by $other and $writer: go reads $(echo "$a" | field records) digest $(echo "$a" | field digest); rust reads $(echo "$b" | field records) digest $(echo "$b" | field digest)" >> "$OUT/conformance.txt"
    # a torn tail: cut 9 bytes off the active segment; both must truncate the same frame
    for reader in go rust; do
      rm -rf "$d-torn"; cp -r "$d" "$d-torn"
      last=$(ls "$d-torn/segments"/seg-*.log | tail -1)
      truncate -s -9 "$last"
      r=$(engine $reader digest --dir "$d-torn")
      echo "torn tail, recovered by $reader: reads $(echo "$r" | field records) digest $(echo "$r" | field digest) truncated_bytes $(echo "$r" | field truncated_bytes)" >> "$OUT/conformance.txt"
      other_r=go; [ $reader = go ] && other_r=rust
      r=$(engine $other_r digest --dir "$d-torn")
      echo "  then read by $other_r: reads $(echo "$r" | field records) digest $(echo "$r" | field digest) truncated_bytes $(echo "$r" | field truncated_bytes)" >> "$OUT/conformance.txt"
    done
    rm -rf "$d" "$d-torn"
  done
  cat "$OUT/conformance.txt" ;;
write)
  : > "$OUT/write.jsonl"
  for producers in 1 32; do for trial in $(seq $TRIALS); do for e in go rust; do
    d="$WORK/w-$e"; rm -rf "$d"; sync
    engine $e write --dir "$d" --producers $producers --seconds $SECONDS_EACH --payload $PAYLOAD | sed "s/^{/{\"trial\":$trial,/" >> "$OUT/write.jsonl"
    rm -rf "$d"
  done; done; done ;;
sustained)
  : > "$OUT/sustained.jsonl"; : > "$OUT/sustained-disk.txt"
  for e in $SUSTAINED_ORDER; do
    d="$WORK/s-$e"; rm -rf "$d"; sync
    # At full speed 30 minutes writes more than the test disk holds. Neither engine reads a sealed
    # segment while only publishing records without IDs, so a helper removes all but the newest
    # $KEEP_SEGMENTS sealed segment files (and their sidecars) every 5 seconds, for both engines alike.
    ( while sleep 5; do
        ls "$d/segments" 2>/dev/null | grep '^seg-.*\.log$' | sort | head -n -$KEEP_SEGMENTS | while read f; do
          b="${f%.log}"; rm -f "$d/segments/$f" "$d/segments/$b".*
        done
      done ) &
    trimmer=$!
    engine $e write --dir "$d" --producers 32 --seconds $SUSTAINED --payload $PAYLOAD >> "$OUT/sustained.jsonl"
    kill $trimmer 2>/dev/null || true
    echo "$e: $(ls "$d/segments" | grep -c '\.log$') segment files kept, newest $(ls "$d/segments" | grep '\.log$' | sort | tail -1), $(du -sm "$d" | cut -f1) MiB on disk at the end" >> "$OUT/sustained-disk.txt"
    rm -rf "$d"
  done ;;
reopen)
  : > "$OUT/reopen.jsonl"; : > "$OUT/fill.jsonl"
  for n in 1000000 10000000; do for e in go rust; do
    d="$WORK/r-$e-$n"; rm -rf "$d"
    engine $e fill --dir "$d" --records $n --payload $PAYLOAD --batch 1000 >> "$OUT/fill.jsonl"
    sleep 2  # lets the Go engine's background sidecar writes finish
    for cache in cold warm; do for rep in 1 2 3 4 5 6 7 8 9 10; do
      [ $cache = cold ] && drop
      engine $e reopen --dir "$d" --offset $((n-1)) | sed "s/^{/{\"records\":$n,\"cache\":\"$cache\",\"fetch\":\"newest\",/" >> "$OUT/reopen.jsonl"
    done; done
    for cache in cold warm; do for rep in 1 2 3; do
      [ $cache = cold ] && drop
      engine $e reopen --dir "$d" --offset 0 | sed "s/^{/{\"records\":$n,\"cache\":\"$cache\",\"fetch\":\"oldest\",/" >> "$OUT/reopen.jsonl"
    done; done
    for rep in 1 2 3; do
      engine $e reopen --dir "$d" --offset $((n-1)) --publish 1 --payload $PAYLOAD | sed "s/^{/{\"records\":$n,\"cache\":\"warm\",\"fetch\":\"newest+publish\",/" >> "$OUT/reopen.jsonl"
    done
  done; done ;;
read)
  : > "$OUT/read.jsonl"
  for e in go rust; do
    d="$WORK/r-$e-10000000"
    drop; engine $e read --dir "$d" | sed 's/^{/{"cache":"cold",/' >> "$OUT/read.jsonl"
    for rep in 1 2 3; do engine $e read --dir "$d" | sed 's/^{/{"cache":"warm",/' >> "$OUT/read.jsonl"; done
  done
  for e in go rust; do rm -rf "$WORK/r-$e-1000000" "$WORK/r-$e-10000000"; done ;;
esac; done
echo "stages done: $STAGES"
