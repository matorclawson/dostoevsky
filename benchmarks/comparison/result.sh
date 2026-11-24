#!/bin/sh
# Print the medians of a comparison log, one line per engine and producer count.
#   sh benchmarks/comparison/result.sh comparison.log
# Medians only: no ranking and no verdict. FAILED lines are counted, never dropped.
# total_rss_KiB is used when the line has it (server engines), else peakRSS_KiB.
set -eu
log=${1:?usage: result.sh comparison.log}
awk '
function median(key,   n, i, j, t, v) {
  n = cnt[key]
  for (i = 1; i <= n; i++) v[i] = val[key, i]
  for (i = 2; i <= n; i++) { t = v[i]; for (j = i - 1; j >= 1 && v[j] > t; j--) v[j + 1] = v[j]; v[j + 1] = t }
  if (n == 0) return "NA"
  if (n % 2) return v[(n + 1) / 2]
  return (v[n / 2] + v[n / 2 + 1]) / 2
}
function add(key, x) { cnt[key]++; val[key, cnt[key]] = x }
/^trial=/ && / FAILED / { failed++; lines++; next }
/^trial=/ {
  lines++
  delete kv
  for (i = 1; i <= NF; i++) { split($i, a, "="); kv[a[1]] = a[2] }
  e = kv["engine"]; w = kv["workers"]
  if (e == "" || w == "") next
  group = e " " w
  if (!(group in seen)) { seen[group] = 1; order[++ngroups] = group }
  trials[group]++
  add(group SUBSEP "rps", kv["records/s"])
  add(group SUBSEP "p99", kv["p99_us"])
  add(group SUBSEP "rss", ("total_rss_KiB" in kv) ? kv["total_rss_KiB"] : kv["peakRSS_KiB"])
  add(group SUBSEP "reopen", kv["reopen_ms"])
}
END {
  printf "lines=%d failed=%d\n", lines, failed
  for (g = 1; g <= ngroups; g++) {
    k = order[g]; split(k, p, " ")
    printf "engine=%s workers=%s trials=%d records/s=%s p99_us=%s total_rss_KiB=%s reopen_ms=%s\n", p[1], p[2], trials[k], median(k SUBSEP "rps"), median(k SUBSEP "p99"), median(k SUBSEP "rss"), median(k SUBSEP "reopen")
  }
}' "$log"
