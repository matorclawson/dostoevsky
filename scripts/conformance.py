#!/usr/bin/env python3
"""Runs the conformance cases against one or more engines. Standard library only.

    python3 scripts/conformance.py --engine go=PATH/TO/dost-conform [--engine other=PATH] [--case NAME]

Each case runs once per storage format it lists and per ordered pair (writer, reader) of engines, so that with two
engines every store written by one is also opened by the other. The case files and the line protocol are described in
cmd/dost-conform/README.md. Every case runs in a new temporary directory.
"""
import argparse
import base64
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

CASES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cmd", "dost-conform", "cases")
TIMEOUT = 120


class Failed(Exception):
    pass


class Engine:
    def __init__(self, name, path):
        self.name = name
        self.process = subprocess.Popen([path], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def call(self, request):
        timer = threading.Timer(TIMEOUT, self.process.kill)
        timer.start()
        try:
            self.process.stdin.write(json.dumps(request).encode() + b"\n")
            self.process.stdin.flush()
            line = self.process.stdout.readline()
        finally:
            timer.cancel()
        if not line:
            raise Failed("the %s driver stopped answering during %s" % (self.name, request["op"]))
        return json.loads(line)

    def stop(self):
        """Ends the driver cleanly: end of input closes an open node."""
        if self.process.poll() is None:
            self.process.stdin.close()
            if self.process.wait(TIMEOUT) != 0:
                raise Failed("the %s driver exited with status %d" % (self.name, self.process.returncode))

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait()


def text(value):
    return base64.b64encode(value.encode()).decode()


def encoded(value):
    """A string is sent as its UTF-8 bytes; {"base64": ...} names bytes that are not text."""
    return value["base64"] if isinstance(value, dict) else text(value)


def expected(want):
    """Encodes the byte strings of an expectation about a workflow result: input, and the data list."""
    if not isinstance(want, dict):
        return want
    out = {}
    for key, value in want.items():
        if key == "input":
            out[key] = encoded(value)
        elif key == "data":
            out[key] = [encoded(v) for v in value]
        else:
            out[key] = expected(value)
    return out


def matches(want, got, where):
    if isinstance(want, dict):
        if not isinstance(got, dict):
            raise Failed("%s: want an object, got %r" % (where, got))
        for key, value in want.items():
            if key not in got:
                raise Failed("%s: no %r in %r" % (where, key, got))
            matches(value, got[key], where + "." + key)
    elif want != got:
        raise Failed("%s: want %r, got %r" % (where, want, got))


class Run:
    def __init__(self, case, fmt, writer, reader, root):
        self.case, self.format, self.root = case, fmt, root
        self.engines = [writer, reader]
        self.turn = 0
        self.driver = None
        self.saved = {}
        self.marks = {}
        self.vars = {}

    def path(self, value):
        return value.replace("$root", self.root)

    def call(self, request):
        if self.driver is None:
            name, binary = self.engines[self.turn % 2]
            self.driver = Engine(name, binary)
        return self.driver.call(request)

    def ok(self, request):
        answer = self.call(request)
        if not answer["ok"]:
            raise Failed("%s failed: %s: %s" % (request["op"], answer.get("error"), answer.get("message")))
        return answer.get("result")

    def request(self, step):
        request = {"op": step["do"]}
        for key, value in step.items():
            if key in ("do", "error", "fails", "expect", "save"):
                continue
            if isinstance(value, str) and value.startswith("$") and value[1:] in self.vars:
                value = self.vars[value[1:]]
            request[key] = self.path(value) if isinstance(value, str) else value
        if step["do"] == "wf_open":
            request.setdefault("dir", self.path("$root/store"))
        if step["do"] == "open":
            request.setdefault("dir", self.path("$root/store"))
            request.setdefault("segmented", self.format == "segmented")
            request.setdefault("node", "a")
            request.setdefault("nodes", ["a"])
            request.setdefault("topics", {"events": 1})
        for key in ("dir", "dest"):
            if key in request:
                request[key] = self.path(request[key])
        if step["do"] in ("publish", "read", "read_batch", "position", "commit_batch"):
            request.setdefault("topic", "events")
        if "value_size" in request:
            request["value"] = "v" * request.pop("value_size")
        for key in ("value", "state"):
            if key in request:
                request[key] = text(request[key])
        for key in ("input", "failure"):
            if key in request:
                request[key] = encoded(request[key])
        if "items" in request:
            request["items"] = [dict(m, value=text(m.get("value", "")), topic=m.get("topic", "events"))
                                for m in request["items"]]
        if "floors" in request:
            request["floors"] = [dict({"topic": "events", "partition": 0}, **f) for f in request["floors"]]
        return request

    def records(self, start):
        found = []
        while True:
            answer = self.call({"op": "read_batch", "topic": "events", "offset": start, "records": 1000,
                                "bytes": 8 << 20})
            if not answer["ok"]:
                if answer["error"] == "empty":
                    return found
                raise Failed("read_batch at %d: %s: %s" % (start, answer.get("error"), answer.get("message")))
            page = answer["result"]
            if not page["records"]:
                return found
            found += page["records"]
            start = page["next"]

    def snapshot(self, step):
        records = self.records(step.get("from", 0))
        groups = {g: self.ok({"op": "position", "topic": "events", "group": g}) for g in step.get("groups", [])}
        return {"records": records, "groups": groups}

    def histories(self, runs):
        return {"runs": {run: self.ok({"op": "wf_history", "run": run, "after": 0, "limit": 1000}) for run in runs}}

    def one_file(self, pattern):
        found = sorted(glob.glob(os.path.join(self.root, pattern)))
        if not found:
            raise Failed("no file matches %s" % pattern)
        return found[-1]

    def step(self, step):
        do = step["do"]
        if do == "switch":
            if self.driver is not None:
                self.driver.stop()
                self.driver = None
            self.turn += 1
        elif do == "kill":
            self.driver.kill()
            self.driver = None
        elif do == "publish_many":
            # step: count, size, ids (bool). Values and IDs depend only on the position, so every run writes the same.
            items = []
            for i in range(step["start"], step["start"] + step["count"]):
                value = ("%08d" % i) * (step.get("size", 8) // 8)
                items.append({"topic": "events", "value": text(value), "id": "id-%d" % i if step.get("ids") else ""})
                if len(items) == step.get("batch", 1000):
                    self.ok({"op": "publish_batch", "items": items})
                    items = []
            if items:
                self.ok({"op": "publish_batch", "items": items})
        elif do == "snapshot":
            self.saved[step["as"]] = self.snapshot(step)
        elif do == "sleep_ms":
            time.sleep(step["ms"] / 1000)
        elif do == "wf_snapshot":
            self.saved[step["as"]] = self.histories(step["runs"])
        elif do == "same" and "runs" in self.saved[step["as"]]:
            want = self.saved[step["as"]]
            got = self.histories(want["runs"])
            if want != got:
                raise Failed("histories differ from %s: %r, want %r" % (step["as"], got, want))
        elif do == "same":
            want = self.saved[step["as"]]
            got = self.snapshot(step)
            if "drop_last" in step:
                want = {"records": want["records"][:-step["drop_last"]], "groups": want["groups"]}
            if want != got:
                raise Failed("store differs from %s: %d records and groups %r, want %d and %r" % (
                    step["as"], len(got["records"]), got["groups"], len(want["records"]), want["groups"]))
        elif do == "count":
            got = len(self.records(step.get("from", 0)))
            if got != step["is"]:
                raise Failed("%d records, want %d" % (got, step["is"]))
        elif do == "mark":
            self.marks[step["as"]] = os.path.getsize(self.one_file(step["file"]))
        elif do == "truncate":
            path = self.one_file(step["file"])
            os.truncate(path, os.path.getsize(path) - step["bytes"])
        elif do == "flip":
            with open(self.one_file(step["file"]), "r+b") as f:
                f.seek(self.marks[step["at"]] + step.get("plus", 0))
                byte = f.read(1)
                f.seek(-1, os.SEEK_CUR)
                f.write(bytes([byte[0] ^ 0x01]))
        elif do == "remember_bytes":
            with open(self.one_file(step["file"]), "rb") as f:
                self.saved[step["as"]] = f.read()
        elif do == "bytes_unchanged":
            with open(self.one_file(step["file"]), "rb") as f:
                if f.read() != self.saved[step["as"]]:
                    raise Failed("%s changed" % step["file"])
        elif do == "make_file":
            path = os.path.join(self.root, step["file"])
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(step.get("content", "").encode())
        elif do == "make_dir":
            os.makedirs(os.path.join(self.root, step["file"]))
        elif do == "remove_first":
            found = sorted(glob.glob(os.path.join(self.root, step["file"])))
            if len(found) < step.get("at_least", 1):
                raise Failed("want at least %d files matching %s, found %d" % (step.get("at_least", 1), step["file"], len(found)))
            os.remove(found[0])
        elif do == "absent":
            if glob.glob(os.path.join(self.root, step["file"])):
                raise Failed("%s exists" % step["file"])
        else:
            answer = self.call(self.request(step))
            if "error" in step:
                if answer["ok"] or answer["error"] != step["error"]:
                    raise Failed("want error %s, got %r" % (step["error"], answer))
            elif step.get("fails"):
                if answer["ok"]:
                    raise Failed("want a failure, got %r" % answer)
            elif not answer["ok"]:
                raise Failed("%s: %s" % (answer.get("error"), answer.get("message")))
            if "expect" in step:
                want = step["expect"]
                if step["do"].startswith("wf_"):
                    want = expected(want)
                matches(want, answer.get("result"), "result")
            for name, path in step.get("save", {}).items():
                found = answer.get("result")
                for key in path.split("."):
                    if not isinstance(found, dict) or key not in found:
                        raise Failed("cannot save %s: no %s in %r" % (name, path, answer))
                    found = found[key]
                self.vars[name] = found

    def run(self):
        try:
            for number, step in enumerate(self.case["steps"], 1):
                if step.get("only", self.format) != self.format:
                    continue
                step = {k: v for k, v in step.items() if k != "only"}
                try:
                    self.step(step)
                except Failed as error:
                    raise Failed("step %d (%s): %s" % (number, step["do"], error))
            if self.driver is not None:
                self.driver.stop()
                self.driver = None
        finally:
            if self.driver is not None:
                self.driver.kill()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--engine", action="append", required=True, metavar="NAME=PATH")
    parser.add_argument("--case", action="append", help="run only this case (file name without .json)")
    parser.add_argument("--parent", help="directory for the temporary stores")
    args = parser.parse_args()
    engines = [(name, os.path.abspath(path)) for name, path in (e.split("=", 1) for e in args.engine)]
    files = sorted(glob.glob(os.path.join(CASES, "*.json")))
    if args.case:
        files = [f for f in files if os.path.basename(f)[:-5] in args.case]
    if not files:
        sys.exit("no cases found")
    passed = failed = 0
    for file in files:
        with open(file, encoding="utf-8") as f:
            case = json.load(f)
        for fmt in case["formats"]:
            for writer in engines:
                for reader in engines:
                    label = "%s [%s, %s then %s]" % (os.path.basename(file)[:-5], fmt, writer[0], reader[0])
                    root = tempfile.mkdtemp(prefix="conform-", dir=args.parent)
                    try:
                        Run(case, fmt, writer, reader, root).run()
                        passed += 1
                        print("PASS", label, flush=True)
                    except Failed as error:
                        failed += 1
                        print("FAIL", label, "-", error, flush=True)
                    finally:
                        shutil.rmtree(root, ignore_errors=True)
    print("%d passed, %d failed" % (passed, failed))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
