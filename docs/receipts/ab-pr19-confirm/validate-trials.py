import collections
import itertools
import pathlib
import re
import sys

lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
engines = ("baseline", "candidate")
expected = set(itertools.product(engines, (32,), range(1, 6)))
seen = collections.Counter()
issues = []
failures = []
low_memory = []
for line in lines:
    match = re.match(r"trial=(\d+) engine=(\S+) workers=(\d+)(?: |$)", line)
    if match is None:
        issues.append(f"Unexpected line {line}")
        continue
    trial, engine, workers = match.groups()
    seen[engine, int(workers), int(trial)] += 1
    if re.search(r"(?:^| )FAILED(?: |$)", line):
        failures.append(line)
        if re.search(r"low.memory|MemAvailable|memory floor|memory pressure", line, re.I):
            low_memory.append(line)
        continue
    if not re.search(r"(?:^| )verified=16000(?: |$)", line):
        issues.append(f"Verification count differs from 16000 in {line}")
for key in sorted(expected | seen.keys()):
    if seen[key] != int(key in expected):
        issues.append(f"{key} occurred {seen[key]} times")
print(f"trial_lines={sum(seen.values())} failed_trials={len(failures)} low_memory_aborts={len(low_memory)} group_issues={len(issues)}")
for issue in issues:
    print(issue)
for failure in failures:
    print(failure)
sys.exit(bool(issues))
