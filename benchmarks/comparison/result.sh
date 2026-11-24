#!/bin/sh
# Print the medians of a comparison log, one line per engine and producer count.
#   sh benchmarks/comparison/result.sh comparison.log
# Fails closed, like the plots checker: every engine present must have exactly
# EXPECT_TRIALS (default 5) distinct, fully parsed trials at every producer count
# in EXPECT_PRODUCERS (default "1 8 32"); a malformed trial line, a repeated trial
# number, a FAILED trial or a missing group makes the script exit 1 and name the
# problem on stderr, with no table. RESULT_ALLOW_INCOMPLETE=1 prints the table
# anyway, with every problem listed first as a PROBLEM line and exit status 0.
# Medians only: no ranking and no verdict. total_rss_KiB is used when the line has
# it (server engines), else peakRSS_KiB.
set -eu
log=${1:?usage: result.sh comparison.log}
EXPECT_TRIALS=${EXPECT_TRIALS:-5} EXPECT_PRODUCERS=${EXPECT_PRODUCERS:-"1 8 32"} ALLOW=${RESULT_ALLOW_INCOMPLETE:-0} \
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
function num(x) { return x ~ /^[0-9]+(\.[0-9]+)?$/ }
function problem(msg) { problems[++nproblems] = msg }
BEGIN { want = ENVIRON["EXPECT_TRIALS"] + 0; nprod = split(ENVIRON["EXPECT_PRODUCERS"], prod, " ") }
/^[ \t]*$/ { next }
/^trial=/ && / FAILED/ {
  lines++; failed++
  n = split($0, f, " "); e = ""; w = ""
  for (i = 1; i <= n; i++) { split(f[i], a, "="); if (a[1] == "engine") e = a[2]; if (a[1] == "workers") w = a[2] }
  if (e != "") { if (!(e in engines)) { engines[e] = 1; engorder[++nengines] = e } }
  problem(sprintf("line %d: FAILED trial (%s, %s producers): %s", NR, e, w, substr($0, 1, 120)))
  next
}
/^trial=/ {
  lines++
  delete kv
  for (i = 1; i <= NF; i++) { split($i, a, "="); kv[a[1]] = a[2] }
  e = kv["engine"]; w = kv["workers"]; t = kv["trial"]
  rss = ("total_rss_KiB" in kv) ? kv["total_rss_KiB"] : kv["peakRSS_KiB"]
  if (e == "" || !num(w) || !num(t) || !num(kv["records/s"]) || !num(kv["p99_us"]) || !num(rss) || !num(kv["reopen_ms"])) {
    problem(sprintf("line %d: malformed trial line: %s", NR, substr($0, 1, 120)))
    next
  }
  if (!(e in engines)) { engines[e] = 1; engorder[++nengines] = e }
  group = e " " w
  if (!(group in seen)) { seen[group] = 1; order[++ngroups] = group }
  if ((group SUBSEP t) in trialseen) {
    problem(sprintf("line %d: duplicate trial number %s for %s at %s producers", NR, t, e, w))
    next
  }
  trialseen[group SUBSEP t] = 1
  trials[group]++
  add(group SUBSEP "rps", kv["records/s"])
  add(group SUBSEP "p99", kv["p99_us"])
  add(group SUBSEP "rss", rss)
  add(group SUBSEP "reopen", kv["reopen_ms"])
  next
}
{ problem(sprintf("line %d: not a trial line: %s", NR, substr($0, 1, 120))) }
END {
  for (g = 1; g <= nengines; g++) {
    for (q = 1; q <= nprod; q++) {
      k = engorder[g] " " prod[q]
      if (!(k in seen)) problem(sprintf("missing: %s at %s producers has no valid trial", engorder[g], prod[q]))
      else if (trials[k] != want) problem(sprintf("%s at %s producers has %d valid trials, want %d", engorder[g], prod[q], trials[k], want))
    }
  }
  if (nproblems > 0 && ENVIRON["ALLOW"] != "1") {
    for (i = 1; i <= nproblems; i++) print "result.sh: " problems[i] > "/dev/stderr"
    exit 1
  }
  for (i = 1; i <= nproblems; i++) print "PROBLEM: " problems[i]
  printf "lines=%d failed=%d\n", lines, failed
  for (g = 1; g <= ngroups; g++) {
    k = order[g]; split(k, p, " ")
    printf "engine=%s workers=%s trials=%d records/s=%s p99_us=%s total_rss_KiB=%s reopen_ms=%s\n", p[1], p[2], trials[k], median(k SUBSEP "rps"), median(k SUBSEP "p99"), median(k SUBSEP "rss"), median(k SUBSEP "reopen")
  }
}' "$log"
