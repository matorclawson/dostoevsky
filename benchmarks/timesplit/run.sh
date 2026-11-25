#!/bin/sh
# PLAN-time-split-v1, steps T0 to T4. MEASUREMENT BRANCH ONLY: never merged. Run from the project root on a quiet host,
# with TMPDIR on the data disk:   sh benchmarks/timesplit/run.sh OUTDIR [T0 T1 T2 T3 T4]
# T0 uses the UNPROBED engine at ee4a24c (built from a git archive); T1 to T3 use this checkout.
# A failed trial is written as a FAILED line, counted and never retried; the script then exits 1.
set -u
out=${1:?usage: run.sh OUTDIR [steps]}
shift
steps=${*:-T0 T1 T2 T3 T4}
base_commit=ee4a24c95d0e75a07ebbc3e6902bcdde40d5fddf
go=${GO:-go}
export GOMAXPROCS="${GOMAXPROCS:-2}"
mkdir -p "$out" || exit 2
out=$(cd "$out" && pwd) || exit 2
[ -e "$out/meta.txt" ] && { echo "$out already holds a run" >&2; exit 2; }
root=$(pwd)

build() { # source-dir bin-dir
 mkdir -p "$2" &&
 (cd "$1/benchmarks/embedded" && CGO_ENABLED=0 "$go" build -mod=readonly -buildvcs=false -o "$2/" ./cmd/dost ./cmd/gateway) &&
 (cd "$1" && CGO_ENABLED=0 "$go" build -mod=readonly -buildvcs=false -o "$2/dostoevsky-gateway" ./cmd/dostoevsky-gateway)
}
mkdir -p "$out/base-src" && git archive "$base_commit" | tar -x -C "$out/base-src" || exit 2
build "$out/base-src" "$out/base-bin" || exit 2
build "$root" "$out/probe-bin" || exit 2
CGO_ENABLED=0 "$go" build -buildvcs=false -o "$out/probe-bin/syncbench" ./benchmarks/timesplit/syncbench || exit 2

{
 echo "base_commit=$base_commit"
 echo "probe_commit=$(git rev-parse HEAD)"
 echo "dirty_files=$(git status --porcelain --untracked-files=no | wc -l | tr -d ' ')"
 echo "go=$("$go" version)"
 echo "uname=$(uname -a)"
 echo "harness_gomaxprocs=$GOMAXPROCS"
 echo "steps=$steps"
 echo "start=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
 echo "load_start=$(uptime)"
} > "$out/meta.txt"

failed=0
# one step trial build engine workers seconds gather_us(or -) profile-prefix(or -)
one() {
 build_name=$3
 bin="$out/$3-bin"
 engine=$4
 tag="step=$1 trial=$2 build=$3 row=$engine workers=$5 gather_us=$7"
 gather=$7
 [ "$gather" = - ] && gather=
 profile=$8
 [ "$profile" = - ] && profile=
 probes="$out/probe-line.tmp"
 rm -f "$probes"
 case $engine in
  dostoevsky) set -- env "$bin/dost" -workers "$5" -window "$6s" -bucket 1h ;;
  gateway) set -- env BENCH_SERVER_BIN="$bin/dostoevsky-gateway" "$bin/gateway" -workers "$5" -window "$6s" -bucket 1h ;;
  gateway-pinned1) set -- env GOMAXPROCS=1 BENCH_GATEWAY_GOMAXPROCS=1 BENCH_SERVER_BIN="$bin/dostoevsky-gateway" "$bin/gateway" -workers "$5" -window "$6s" -bucket 1h ;;
 esac
 if TIMESPLIT_OUT="$probes" TIMESPLIT_GATHER_US="$gather" TIMESPLIT_PROFILE="$profile" "$@" >"$out/trial.tmp" 2>"$out/error.tmp" &&
  line=$(grep '^sustained ' "$out/trial.tmp") &&
  case $line in *" stop=window "*|*" stop=reached "*) true ;; *) false ;; esac
 then
  printf '%s %s\n' "$tag" "$line" >> "$out/results.txt"
 else
  failed=$((failed + 1))
  printf '%s FAILED %s %s\n' "$tag" "$(grep '^sustained ' "$out/trial.tmp")" "$(tr '\n' ' ' < "$out/error.tmp" | cut -c1-200)" >> "$out/results.txt"
 fi
 if [ -s "$probes" ]; then
  while IFS= read -r p; do printf '%s %s\n' "$tag" "$p"; done < "$probes" >> "$out/probes.txt"
 elif [ "$build_name" = probe ]; then
  failed=$((failed + 1))
  printf '%s FAILED no probe line\n' "$tag" >> "$out/probes.txt"
 fi
 rm -f "$probes" "$out/trial.tmp" "$out/error.tmp"
 tail -n 1 "$out/results.txt"
}

# Smoke: every build and row for 5 s before anything is timed. It is not part of the results.
for b in base probe; do
 for e in dostoevsky gateway gateway-pinned1; do
  one smoke 0 "$b" "$e" 8 5 - -
 done
done
mv "$out/results.txt" "$out/smoke.txt"
[ -f "$out/probes.txt" ] && mv "$out/probes.txt" "$out/smoke-probes.txt"
if [ "$failed" -ne 0 ]; then echo "smoke failed ($failed): nothing was timed" >&2; exit 3; fi

# order: rotate "dostoevsky gateway" and the gather values by round
for s in $steps; do
 case $s in
  T0|T1)
   if [ "$s" = T0 ]; then b=base; secs=${T0_SECONDS:-120}; else b=probe; secs=${T1_SECONDS:-60}; fi
   for r in 1 2 3; do
    if [ $((r % 2)) -eq 1 ]; then order="dostoevsky gateway"; else order="gateway dostoevsky"; fi
    for w in 1 8 32; do
     for e in $order; do one "$s" "$r" "$b" "$e" "$w" "$secs" - -; done
    done
    [ "$s" = T1 ] && one T1 "$r" probe gateway-pinned1 8 "$secs" - -
   done ;;
  T2)
   for r in 1 2 3; do
    case $r in 1) gathers="0 100 500 1000 2000" ;; 2) gathers="500 1000 2000 0 100" ;; *) gathers="2000 0 100 500 1000" ;; esac
    for g in $gathers; do
     for w in 8 32; do
      for e in dostoevsky gateway; do one T2 "$r" probe "$e" "$w" "${T2_SECONDS:-60}" "$g" -; done
     done
    done
   done ;;
  T3)
   # The profile is of the first process of each trial (the workload); for gateway rows it is the gateway process.
   one T3 1 probe dostoevsky 8 60 - "$out/profile-dostoevsky"
   one T3 1 probe gateway 8 60 - "$out/profile-gateway" ;;
  T4)
   "$out/probe-bin/syncbench" -dir "${TMPDIR:-/tmp}" > "$out/syncbench.txt" || failed=$((failed + 1)) ;;
  *) echo "unknown step $s" >&2; exit 2 ;;
 esac
done
{
 echo "end=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
 echo "load_end=$(uptime)"
 echo "failed=$failed"
} >> "$out/meta.txt"
rm -rf "$out/base-src"
(cd "$out" && for f in meta.txt results.txt probes.txt syncbench.txt smoke.txt smoke-probes.txt profile-*; do [ -f "$f" ] && { command -v sha256sum >/dev/null 2>&1 && sha256sum "$f" || shasum -a 256 "$f"; }; done > SHA256SUMS)
[ "$failed" -eq 0 ]
