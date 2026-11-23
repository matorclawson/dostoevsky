#!/bin/sh
# Compare separate processes. Emit every trial before computing medians.
set -eu
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
count=${BENCH_COUNT:-16000}
for rep in 1 2 3 4 5; do
 case "$rep" in
  1|3|5) order='dost jet' ;;
  *) order='jet dost' ;;
 esac
 for w in 1 8 32; do
  for engine in $order; do
   line=$(".dev/embedded/$engine" -workers "$w" -count "$count")
   printf 'trial=%s %s\n' "$rep" "$line"
   printf '%s\n' "$line" >> "$out/$engine-$w"
  done
 done
done
median() {
 awk -v key="$2" '{for(i=1;i<=NF;i++){split($i,a,"="); if(a[1]==key) print a[2]}}' "$1" |
  sort -n | awk 'NR==3 {median=$1} END {if(NR!=5) exit 1; print median}'
}
status=0
for w in 1 8 32; do
 d=$(median "$out/dost-$w" records/s)
 j=$(median "$out/jet-$w" records/s)
 dm=$(median "$out/dost-$w" peakRSS_KiB)
 jm=$(median "$out/jet-$w" peakRSS_KiB)
 printf 'workers=%s records/s=%s/%s peakRSS_KiB=%s/%s (Dostoevsky/JetStream)\n' "$w" "$d" "$j" "$dm" "$jm"
 if [ "$d" -le "$j" ] || [ "$dm" -ge "$jm" ]; then status=1; fi
done
exit "$status"
