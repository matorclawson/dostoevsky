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
import signal
import subprocess
import sys
import tempfile
import threading
import time

CASES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cmd", "dost-conform", "cases")
TIMEOUT = 120
FORMATS = ("legacy", "segmented", "workflow")
# The runner's own actions and the keys each takes, besides "do" and "only".
ACTIONS = {
    "switch": set(), "kill": set(), "publish_many": {"start", "count", "size", "ids", "batch"},
    "snapshot": {"as", "from", "groups"}, "same": {"as", "from", "groups", "drop_last"}, "count": {"is", "from"},
    "mark": {"file", "as"}, "truncate": {"file", "bytes"}, "flip": {"file", "at", "plus"},
    "remember_bytes": {"file", "as"}, "bytes_unchanged": {"file", "as"}, "make_file": {"file", "content"},
    "make_dir": {"file"}, "remove_first": {"file", "at_least"}, "absent": {"file"},
    "sleep_ms": {"ms"}, "compare": {"marks", "is"}, "bytes_changed": {"file", "as"},
    "equal_files": {"files"}, "wf_snapshot": {"as", "runs"}, "at_least": {"mark", "bytes"},
}
# Engine requests. The driver itself refuses a field it does not know.
OPERATIONS = {"open", "close", "publish", "publish_batch", "read", "read_batch", "position", "commit_batch", "retain",
              "backup", "migrate", "verify", "inspect", "repair", "upgrade",
              "wf_open", "wf_close", "wf_register", "wf_start_once", "wf_poll", "wf_finish", "wf_signal", "wf_history",
              "fix_inputs", "advance", "arm", "disarm"}


class Failed(Exception):
    pass


class Engine:
    def __init__(self, name, path):
        self.name = name
        # Standard error goes to a file: a driver armed to kill says there which point it died at. The driver leads its
        # own process group, so that a deadline also ends whatever it started and nothing keeps its output open.
        self.errors = tempfile.TemporaryFile()
        self.process = subprocess.Popen([path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors,
                                        start_new_session=True)

    def end_group(self):
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    def written(self):
        """How many bytes the driver has written to its standard error so far."""
        return os.fstat(self.errors.fileno()).st_size

    def said(self, since=0):
        """The driver's standard error from the byte at since. Read in place: the driver shares the file's position."""
        size = self.written()
        return os.pread(self.errors.fileno(), size - since, since).decode("utf-8", "replace") if size > since else ""

    def exchange(self, request):
        """Writes one request and reads one line, ending the process group after TIMEOUT. Returns the line (empty when
        there was none) and whether the deadline was what ended it."""
        expired = threading.Event()

        def deadline():
            expired.set()
            self.end_group()

        timer = threading.Timer(TIMEOUT, deadline)
        timer.start()
        try:
            try:
                self.process.stdin.write(json.dumps(request).encode() + b"\n")
                self.process.stdin.flush()
                line = self.process.stdout.readline()
            except BrokenPipeError:
                line = b""
        finally:
            timer.cancel()
        return line, expired.is_set()

    def call(self, request):
        line, expired = self.exchange(request)
        if expired:
            raise Failed("the %s driver did not answer %s in time" % (self.name, request["op"]))
        if not line:
            raise Failed("the %s driver stopped answering during %s: %s" % (self.name, request["op"], self.said()[-300:]))
        return json.loads(line)

    def die(self, request, point, passes):
        """Sends a request the driver is to die in, at the named point on the given pass. It must not answer, must say
        that this point fired, and must end by SIGKILL. A driver the deadline had to end hung, which is not a death."""
        if self.process.poll() is not None:
            raise Failed("the %s driver had already gone before %s" % (self.name, request["op"]))
        before = self.written()  # only what is written during this request can be about this death
        line, expired = self.exchange(request)
        if expired:
            raise Failed("the %s driver hung in %s instead of dying" % (self.name, request["op"]))
        if line:
            raise Failed("the %s driver answered %s instead of dying: %r" % (self.name, request["op"], line[:200]))
        status = self.process.wait(TIMEOUT)
        self.end_group()
        want = "seam: killing at %s, pass %d" % (point, passes)
        if want not in self.said(before).splitlines():
            raise Failed("the %s driver died in %s without saying %r: %s" % (self.name, request["op"], want,
                                                                             self.said(before)[-300:]))
        if status != -signal.SIGKILL:
            raise Failed("the %s driver ended with status %d, not by SIGKILL" % (self.name, status))

    def stop(self):
        """Ends the driver cleanly (end of input closes an open node). It must exit with status 0, also when it had
        already exited."""
        try:
            self.process.stdin.close()
        except BrokenPipeError:
            pass
        try:
            status = self.process.wait(TIMEOUT)
        except subprocess.TimeoutExpired:
            self.end_group()
            raise Failed("the %s driver did not exit" % self.name)
        if status != 0:
            raise Failed("the %s driver exited with status %d: %s" % (self.name, status, self.said()[-300:]))

    def kill(self):
        """Kills a driver that must still be running: one that has already exited was not killed at this point."""
        status = self.process.poll()
        if status is not None:
            raise Failed("the %s driver had already exited with status %d before the kill" % (self.name, status))
        self.end_group()
        status = self.process.wait(TIMEOUT)
        if status != -signal.SIGKILL:
            raise Failed("the %s driver ended with status %d, not by the kill" % (self.name, status))

    def discard(self):
        """Cleanup after a failure: never raises, so the first error is the one reported."""
        self.end_group()
        try:
            self.process.wait(TIMEOUT)
        except subprocess.TimeoutExpired:
            pass


def text(value):
    return base64.b64encode(value.encode()).decode()


def byte_string(value):
    """A string is sent as its UTF-8 bytes; {"base64": ...} names bytes that are not text."""
    return value["base64"] if isinstance(value, dict) else text(value)


def expected(want):
    """Encodes the byte strings of an expectation about a workflow result: input, and the data list."""
    if not isinstance(want, dict):
        return want
    out = {}
    for key, value in want.items():
        if key == "input":
            out[key] = byte_string(value)
        elif key == "data":
            out[key] = [byte_string(v) for v in value]
        else:
            out[key] = expected(value)
    return out


def matches(want, got, where):
    """Every key of want is in got with an equal value of the same JSON type (true is not 1)."""
    if type(want) is not type(got):
        raise Failed("%s: want %r, got %r" % (where, want, got))
    if isinstance(want, dict):
        for key, value in want.items():
            if key not in got:
                raise Failed("%s: no %r in %r" % (where, key, got))
            matches(value, got[key], where + "." + key)
    elif want != got:
        raise Failed("%s: want %r, got %r" % (where, want, got))


def encoded(expect):
    """Expected byte strings are written as text in case files."""
    return {k: text(v) if k in ("value", "state") else v for k, v in expect.items()}


def check_case(name, case):
    def bad(why):
        sys.exit("case %s: %s" % (name, why))

    if set(case) != {"about", "covers", "formats", "steps"}:
        bad("want the keys about, covers, formats and steps")
    if not case["formats"] or any(f not in FORMATS for f in case["formats"]):
        bad("formats must name at least one of %s" % ", ".join(FORMATS))
    if not case["steps"]:
        bad("no steps")
    for fmt in case["formats"]:
        if not any(step.get("only", fmt) == fmt for step in case["steps"]):
            bad("no step runs for the format %s" % fmt)
    for number, step in enumerate(case["steps"], 1):
        do = step.get("do")
        if do not in ACTIONS and do not in OPERATIONS:
            bad("step %d: unknown action %r" % (number, do))
        if "only" in step and step["only"] not in case["formats"]:
            bad("step %d: only names a format the case does not run" % number)
        if "dies" in step and (do not in OPERATIONS or step["dies"] is not True
                               or set(step) & {"error", "fails", "expect", "nonzero", "save"}):
            bad("step %d: dies is true, on an engine request that expects nothing else" % number)
        # A comparison of nothing, or of a thing with itself, would always pass.
        if do == "wf_snapshot" and not step.get("runs"):
            bad("step %d: wf_snapshot names no run" % number)
        if do == "compare" and (len(step.get("marks", [])) != 2 or len(set(step["marks"])) != 2
                                or step.get("is") not in ("equal", "less")):
            bad("step %d: compare takes two different marks and is equal or less" % number)
        if do == "equal_files" and (len(step.get("files", [])) != 2
                                    or len({os.path.normpath(f) for f in step["files"]}) != 2):
            bad("step %d: equal_files takes two different files" % number)
        if do == "at_least" and (type(step.get("bytes")) is not int or step["bytes"] <= 0):
            bad("step %d: at_least takes a positive whole number of bytes" % number)
        if do == "disarm" and not isinstance(step.get("expect", {}).get("reached"), bool):
            bad("step %d: disarm states whether the point was reached" % number)
        if do in ACTIONS:
            unknown = set(step) - ACTIONS[do] - {"do", "only"}
            if unknown:
                bad("step %d: %s does not take %s" % (number, do, ", ".join(sorted(unknown))))


class Run:
    def __init__(self, case, fmt, writer, reader, root):
        self.case, self.format, self.root = case, fmt, root
        self.engines = [writer, reader]
        self.turn = 0
        self.driver = None
        self.saved = {}
        self.marks = {}
        self.vars = {}
        self.armed = None  # (point, kill, skip) armed in the running driver and not yet accounted for

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
            if key in ("do", "error", "fails", "expect", "nonzero", "save", "dies"):
                continue
            if isinstance(value, str) and value.startswith("$") and value != "$root" and not value.startswith("$root/"):
                if value[1:] not in self.vars:
                    raise Failed("nothing was saved as %s" % value)
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
        if "input_size" in request:
            # A JSON string of that many bytes.
            request["input"] = '"%s"' % ("x" * (request.pop("input_size") - 2))
        for key in ("input", "failure"):
            if key in request:
                request[key] = byte_string(request[key])
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

    def contents(self, pattern):
        with open(self.one_file(pattern), "rb") as f:
            return f.read()

    def one_file(self, pattern):
        found = sorted(glob.glob(os.path.join(self.root, pattern)))
        if not found:
            raise Failed("no file matches %s" % pattern)
        return found[-1]

    def check_armed(self):
        """A point armed in a driver that is about to end was never reached or never checked."""
        if self.armed is not None:
            raise Failed("%s was armed and neither killed the driver nor was checked with disarm" % self.armed[0])

    def step(self, step):
        do = step["do"]
        if do in ("switch", "kill"):
            self.check_armed()
        if do == "switch":
            if self.driver is not None:
                self.driver.stop()
                self.driver = None
            self.turn += 1
        elif do == "kill":
            if self.driver is None:
                raise Failed("no driver is running")
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
        elif do == "at_least":
            if self.marks[step["mark"]] < step["bytes"]:
                raise Failed("%s is %d, want at least %d" % (step["mark"], self.marks[step["mark"]], step["bytes"]))
        elif do == "equal_files":
            if os.path.samefile(*(self.one_file(name) for name in step["files"])):
                raise Failed("%s are one file" % " and ".join(step["files"]))
            first, second = (self.contents(name) for name in step["files"])
            if first != second:
                raise Failed("%s differ (%d and %d bytes)" % (" and ".join(step["files"]), len(first), len(second)))
        elif do == "wf_snapshot":
            self.saved[step["as"]] = self.histories(step["runs"])
        elif do == "same" and "runs" in self.saved[step["as"]]:
            want = self.saved[step["as"]]
            got = self.histories(list(want["runs"]))
            if want != got:
                raise Failed("histories differ from %s: %r, want %r" % (step["as"], got, want))
        elif do == "compare":
            first, second = (self.marks[name] for name in step["marks"])
            if not {"equal": first == second, "less": first < second}[step["is"]]:
                raise Failed("sizes %s are %d and %d, want %s" % (", ".join(step["marks"]), first, second, step["is"]))
        elif do == "bytes_changed":
            with open(self.one_file(step["file"]), "rb") as f:
                if f.read() == self.saved[step["as"]]:
                    raise Failed("%s did not change" % step["file"])
        elif do == "sleep_ms":
            time.sleep(step["ms"] / 1000)
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
        elif step.get("dies"):
            request = self.request(step)
            if self.driver is None:
                raise Failed("no driver is running")
            if self.armed is None or not self.armed[1]:
                raise Failed("no point is armed to kill")
            self.driver.die(request, self.armed[0], self.armed[2] + 1)
            self.driver, self.armed = None, None
        else:
            if do == "arm" and self.armed is not None:
                raise Failed("%s is still armed and unchecked: disarm it before arming %s" % (self.armed[0], step.get("point")))
            answer = self.call(self.request(step))
            if "error" in step:
                if answer["ok"] or answer["error"] != step["error"]:
                    raise Failed("want error %s, got %r" % (step["error"], answer))
            elif step.get("fails"):
                if answer["ok"]:
                    raise Failed("want a failure, got %r" % answer)
            elif not answer["ok"]:
                raise Failed("%s: %s" % (answer.get("error"), answer.get("message")))
            if answer["ok"] and step["do"] == "arm":
                self.armed = (step["point"], step.get("kill", False), step.get("skip", 0))
            elif answer["ok"] and step["do"] == "disarm":
                self.armed = None
            if "expect" in step:
                want = step["expect"]
                want = expected(want) if step["do"].startswith("wf_") else encoded(want)
                matches(want, answer.get("result"), "result")
            for key in step.get("nonzero", []):
                if not (answer.get("result") or {}).get(key):
                    raise Failed("result.%s is zero or missing in %r" % (key, answer.get("result")))
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
            self.check_armed()
            if self.driver is not None:
                self.driver.stop()
                self.driver = None
        finally:
            if self.driver is not None:
                self.driver.discard()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--engine", action="append", required=True, metavar="NAME=PATH")
    parser.add_argument("--case", action="append", help="run only this case (file name without .json)")
    parser.add_argument("--parent", help="directory for the temporary stores")
    args = parser.parse_args()
    engines = [(name, os.path.abspath(path)) for name, path in (e.split("=", 1) for e in args.engine)]
    available = {os.path.basename(f)[:-5]: f for f in glob.glob(os.path.join(CASES, "*.json"))}
    names = args.case or sorted(available)
    missing = [n for n in names if n not in available]
    if missing or not names:
        sys.exit("no such case: %s" % ", ".join(missing) if missing else "no cases found")
    cases = []
    for name in names:
        with open(available[name], encoding="utf-8") as f:
            cases.append((available[name], json.load(f)))
        check_case(name, cases[-1][1])
    passed = failed = 0
    for file, case in cases:
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
    sys.exit(1 if failed or not passed else 0)


if __name__ == "__main__":
    main()
