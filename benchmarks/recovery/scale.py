"""Benchmark a disposable fixture on the operator-selected volume, leaving disk headroom."""
import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile
import sys


def main():
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("binary", type=Path)
    args.add_argument("parent", type=Path)
    args.add_argument("records", type=int)
    args.add_argument("size", type=int)
    args.add_argument("--reserve-bytes", type=int, default=1800000000, help="disk headroom (minimum 1 GiB)")
    cfg = args.parse_args()
    if cfg.records < 1 or not 8 <= cfg.size <= 512:
        args.error("records must be positive and size must be 8 through 512")
    if cfg.reserve_bytes < 1 << 30:
        args.error("reserve must be at least 1 GiB")
    reserve = cfg.reserve_bytes
    # Single partition, no producer IDs, 38 frame bytes plus allowance for indexes.
    per_record = cfg.size + 40
    def require(space):
        if shutil.disk_usage(cfg.parent).free < space + reserve:
            raise SystemExit("insufficient free space for fixture and requested reserve")
    require(cfg.records * per_record + 8 * 1024 * 1024)
    with tempfile.TemporaryDirectory(dir=cfg.parent, prefix="recovery-") as store:
        base = [str(cfg.binary.resolve()), "-dir", store, "-value-bytes", str(cfg.size)]
        for start in range(0, cfg.records, 1000000):
            end = min(cfg.records, start + 1000000)
            require((end - start) * per_record + 8 * 1024 * 1024)
            subprocess.run(base + ["-segmented", "-mode", "seed", "-start", str(start), "-n", str(end)], check=True)
        for mode in ["open"] * 5 + ["scrub", "pages"]:
            subprocess.run(base + ["-n", str(cfg.records), "-mode", mode], check=True)
        subprocess.run([sys.executable, str(Path(__file__).with_name("kill_history.py")),
                        str(cfg.binary), store, str(cfg.records), str(cfg.size)], check=True)


if __name__ == "__main__":
    main()
