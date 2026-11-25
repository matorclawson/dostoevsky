import pathlib
import sys

print("round\tengine\tworkers\tverified\trecords_per_second\tp99_microseconds\tstatus")
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    cols = line.split()
    values = dict(col.split("=", 1) for col in cols if "=" in col)
    order = ("trial", "engine", "workers", "verified", "records/s", "p99_us")
    print(*(values.get(col, "") for col in order), "FAILED" if "FAILED" in cols else "OK", sep="\t")
