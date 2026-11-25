"""Retain observable Kafka smoke files while the unmodified harness removes trial directories."""
import os
from pathlib import Path
import sys
import time

stage = Path(sys.argv[1])
stderr = stage / "comparison.err"
archive = stage / "smoke-kafka"
archive.mkdir(mode=0o700)
trials = set()
files = ("kafka-logs", "logs", "server.log", "server.properties")

def preserve(src, dst):
    if src.is_symlink():
        return
    if src.is_dir():
        dst.mkdir(exist_ok=True)
        for child in src.iterdir():
            preserve(child, dst / child.name)
    elif src.is_file():
        try:
            if src.samefile(dst):
                return
        except FileNotFoundError:
            pass
        tmp = dst.with_name(dst.name + ".capture-tmp")
        os.link(src, tmp, follow_symlinks=False)
        os.replace(tmp, dst)

while True:
    if stderr.exists():
        observed = [
            line for line in stderr.read_text(errors="replace").splitlines()
            if line.startswith("smoke ") and "engine=kafka " in line
        ]
        if len(observed) == 3:
            print(f"Observed {len(trials)} Kafka smoke trial directories", flush=True)
            break
    for trial in (stage / "tmp").glob("embedded-bench-*"):
        if trial not in trials:
            if len(trials) == 3 or not (trial / "kafka-logs").is_dir():
                continue
            trials.add(trial)
            print(f"Preserving {trial.name}", flush=True)
        dest = archive / trial.name
        dest.mkdir(exist_ok=True)
        for name in files:
            try:
                preserve(trial / name, dest / name)
            except FileNotFoundError:
                # The harness can remove a trial during an observation.
                continue
    time.sleep(0.20)
