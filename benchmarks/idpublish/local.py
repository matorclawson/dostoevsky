"""Create comparable legacy/segmented stores on the same volume, then alternate fresh processes."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("fixture", type=Path)
    p.add_argument("runner", type=Path)
    p.add_argument("parent", type=Path)
    p.add_argument("history", type=int)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--samples", type=int, default=2048)
    a = p.parse_args()
    if min(a.history, a.samples, a.rounds) < 1:
        p.error("counts must be positive")
    if shutil.disk_usage(a.parent).free < (1 << 30) + 350 * a.history:
        p.error("insufficient space for both formats and 1 GiB headroom")
    with tempfile.TemporaryDirectory(dir=a.parent, prefix="id-publish-") as tmp:
        root = Path(tmp) / "stores"
        subprocess.run([a.fixture.resolve(), "-dir", root, "-n", str(a.history)], check=True)
        def trial(engine, workers, mode, count, run):
            print(f"engine={engine} trial={run}", flush=True)
            env = {**os.environ, "DOST_ID_DIR": str(root / engine), "DOST_ID_HISTORY": str(a.history),
                   "DOST_ID_WORKERS": str(workers), "DOST_ID_RUN": run, "DOST_ID_MODE": mode,
                   "DOST_ID_COUNT": str(count)}
            subprocess.run([a.runner.resolve(), "-test.run", "^TestIdentifiedPublishWorkload$"], env=env, check=True)
        for r in range(a.rounds):
            for w in [1, 32]:
                for mode in ["new", "recent", "oldest"]:
                    for engine in (["segmented", "legacy"] if r % 2 == 0 else ["legacy", "segmented"]):
                        trial(engine, w, mode, a.samples, f"r{r}-w{w}-{mode}")
        for w in [1, 32]:
            trial("segmented", w, "roll", 20000, f"roll-w{w}")
        env = {**os.environ, "DOST_ID_CRASH_DIR": str(root / "segmented"), "DOST_ID_CRASH_PREFIX": "crash-oracle"}
        subprocess.run([a.runner.resolve(), "-test.run", "^TestDeepIDCrash$"], env=env, check=True)


if __name__ == "__main__":
    main()
