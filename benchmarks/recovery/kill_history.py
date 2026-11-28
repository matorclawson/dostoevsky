"""Interrupt a publishing process on an existing disposable scale fixture; validate all observed acknowledgements."""
import argparse
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("dir", type=Path)
    parser.add_argument("count", type=int)
    parser.add_argument("value_bytes", type=int)
    args = parser.parse_args()
    base = [str(args.binary.resolve()), "-dir", str(args.dir), "-value-bytes", str(args.value_bytes)]
    # Check the recorded history ends exactly where this run expects it.
    subprocess.run(base + ["-n", str(args.count)], check=True)
    p = subprocess.Popen(base + ["-mode", "append-live", "-start", str(args.count),
                                 "-n", str(args.count + 1000000)], stdout=subprocess.PIPE, text=True)
    observed = []
    try:
        for _ in range(8):
            line = p.stdout.readline()
            if not line.startswith("ack_next="):
                raise RuntimeError("writer exited before receipt")
            observed.append(line)
    finally:
        p.kill()
        tail, _ = p.communicate()
        observed.extend(tail.splitlines())
    if p.returncode != -9:
        raise RuntimeError(f"writer exit status {p.returncode}")
    last = args.count
    for line in observed:
        if not line.startswith("ack_next="):
            raise RuntimeError(f"unexpected receipt {line!r}")
        end = int(line.split("=")[1])
        if end != last + 256:
            raise RuntimeError(f"receipt gap after {last}")
        last = end
    subprocess.run(base + ["-mode", "acknowledged", "-start", str(args.count), "-n", str(last)], check=True)
    print(f"history={args.count} signal=SIGKILL acknowledged_tail={last-args.count} verified_tail={last-args.count}")


if __name__ == "__main__":
    main()
