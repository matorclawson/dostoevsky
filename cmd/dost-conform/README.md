# Conformance suite

Cases that every implementation of the engine must pass, driven from outside: public operations, process kills and edits
to the files on disk. `dost-conform` is the driver for the Go engine. Another implementation joins by providing a program
that speaks the same lines.

```sh
make conformance                                          # the Go engine
python3 scripts/conformance.py --engine go=A --engine rust=B   # every case, each engine opening the other's stores
```

Linux only (the cases kill processes and edit files). Python 3.10 or later, standard library only.

## Driver protocol

One JSON object per line in, one per line out, until end of input (which closes an open node and exits 0).

Request: `{"op": NAME, ...}`. Byte strings (`value`, `state`) are base64.

| op | fields | result |
|---|---|---|
| `open` | `dir`, `segmented`, `node`, `nodes`, `topics` | `truncated_at`, `truncated_bytes` |
| `close` | | |
| `publish` | `topic`, `key`, `value`, `id` | record |
| `publish_batch` | `items`: `topic`, `key`, `value`, `id` | `records` |
| `read` | `topic`, `partition`, `offset` | record |
| `read_batch` | `topic`, `partition`, `offset`, `records`, `bytes` | `records`, `next` |
| `position` | `topic`, `partition`, `group` | `next`, `state` |
| `commit_batch` | `topic`, `partition`, `group`, `offset`, `next`, `state` | |
| `retain` | `floors`: `topic`, `partition`, `floor` | |
| `backup`, `migrate` | `dest` | |
| `verify` | | |
| `inspect`, `repair` | `dir`; `discard_later` for repair. No open node | `size`, `good_bytes`, `frames`, `damage`, `later_frames`, `quarantined` |
| `upgrade` | `dir`. No open node | `saved_copy` |

A record is `topic`, `partition`, `offset`, `key`, `value`, `id`, `time` (nanoseconds since the Unix epoch, as a string).

Reply: `{"ok": true, "result": ...}` or `{"ok": false, "error": CODE, "message": TEXT}`; a refused repair also carries a
result. Codes: `journal_full`, `limit`, `invalid`, `owner`, `closed`, `conflict`, `expired`, `empty`, `corrupt_log`,
`incomplete_backup`, `backup_not_durable`, `upgrade_required`, `segmented_journal`, `later_frames`, `not_repairable`,
`exists`, `other`. Only the code is compared, never the text. Damage to a legacy journal has no code of its own today, so
those cases require a failure and unchanged bytes.

## Workflow operations

A driver holds one open node or one open workflow store. `dir` defaults to `$root/store`.

| op | fields | result |
|---|---|---|
| `wf_open` | `dir` | |
| `wf_close` | | |
| `wf_register` | `name`, `version`, `steps`: `kind`, `name`, `attempts`, `backoff_ms` | |
| `wf_start_once` | `run`, `name`, `version`, `queue`, `input`, `attrs` | `created` |
| `wf_poll` | `queue`, `lease_ms` | `task`: null, or `run`, `activity`, `token`, `step`, `attempt`, `input` |
| `wf_finish` | `run`, `token`, `value`, `failure` | |
| `wf_signal` | `run`, `step`, `name`, `value` | |
| `wf_history` | `run`, `after`, `limit` | `kinds`, `data`, `events`: `sequence`, `time` (string, ms), `kind`, `step`, `data` |

`input`, `failure` and history data are base64 on the wire. In a case file they are written as text, or as
`{"base64": ...}` for bytes that are not text. Additional codes: `start_conflict`, `stale`, `missing`.

Workflow cases have `"formats": ["workflow"]`. Their runner steps: `"save": {NAME: "task.token"}` on a request and
`"$NAME"` in a later one (a name nothing saved fails the case), and `sleep_ms`.

The `workflow-kill-*` cases kill the driver at each of the four workflow commit points during a start, poll, finish,
failure, lease expiry and signal; `workflow-fail-append` makes the append fail instead. Before reopening, the kill cases
compare the files: whether the witness changed, and with `compare` (two `mark` sizes, `equal` or `less`) how much of the
group reached the segment. `bytes_changed` is the opposite of `bytes_unchanged`. They show where a point sits relative
to the bytes written, not whether those bytes were synced. After the post-witness kill the segment, witness and replayed
history must equal those of a second store given the same requests without the kill (`fix_inputs`, `equal_files`,
`wf_snapshot` with `runs`, `same`).

The `workflow-rollover-*` cases kill at each point during the commit that starts a second segment. A start step may give
`input_size` instead of `input`. Where the roll falls is arithmetic of this format: a segment rolls once the frames
appended to it reach 8 MiB, an `input_size` of 189000 makes a record of about 252 KiB (base64 in JSON), so 34 such
starts fill the first segment and the 35th starts the second. The cases check that the successor is absent after 34;
a change to the threshold or the encoding fails them there. The rollover cases therefore depend on the Go engine's frame
sizes and roll threshold, which the Rust engine must reproduce. `at_least` requires a `mark` of that many bytes. The runner
refuses a `wf_snapshot` of no run, and a `compare` or `equal_files` of one thing with itself.

The other workflow cases stay within one segment, and no case covers a third segment.

Not covered for workflows: concurrent polls, size limits, damaged and half-initialized stores, and any past workflow
fix (their `covers` lists are empty).

## Test seam requests

| op | fields | result |
|---|---|---|
| `fix_inputs` | `start`, `step` (ns), `seed` (unsigned 64-bit number); only with nothing open | |
| `advance` | `nanoseconds` | |
| `arm` | `point`, `kill`, `skip` | |
| `disarm` | | `reached` |

They set the clock, randomness and the named points of `internal/seam`, whose comments are the contract; another engine
uses the same point names at the same boundaries. `arm` is refused with code `other`, first for a name that is not a
point, then while a point is armed that `disarm` has not reported, whether or not it has fired. A step with `"dies": true` is a request the driver must not answer:
it must write `seam: killing at POINT, pass N` to its standard error and end by SIGKILL. A point armed to fail is
followed by `disarm` with the expected `reached` before the driver ends; the runner fails a case that leaves one armed, and one that arms again before the first is accounted for.

## Case files

`cases/*.json`: `formats` (`legacy`, `segmented`), `covers` (commits that fixed what the case checks) and `steps`. A step
is an engine request (with `error`, `fails` or `expect`), or an action of the runner: `switch` (close, continue with the
other engine), `kill`, `publish_many`, `snapshot`, `same`, `count`, and edits to files (`mark`, `truncate`, `flip`,
`remember_bytes`, `bytes_unchanged`, `make_file`, `make_dir`, `remove_first`, `absent`). `only` limits a step to one format.
`nonzero` names result fields that must be present and not zero. In `expect`, `value` and `state` are written as text, and
values must have the same JSON type as the reply's. The runner refuses a case with an unknown format, action or action
key, refuses an unknown `--case` name, and fails when nothing ran. A driver must exit with status 0 at end of input.

## Coverage of past fixes

This is the first set. It covers only what can be provoked through public operations and file edits.

| Fix | Defect | Case |
|---|---|---|
| `bd7477d` | a second retention could lose later writes | `retention-twice-then-reopen` |
| `825a794` | backup could replace an existing directory | `backup-restores` (for an empty directory Go's rename refuses too; see the case) |
| `d8a84ab` | an interrupted backup opened as an empty node | `incomplete-backup-directory` |
| `f20ffa0` | no way back from a damaged journal | `damaged-frame-before-later-frames` |
| `614c3ed` | recovery stops at a damaged frame | `damaged-frame-before-later-frames` (the offset in the message is not compared) |
| `5a25d90` | a partial backup was visible; the parent directory was not synced | `backup-interrupted` (the sync failure is injected after the rename; leftovers of a failed copy are not checked) |
| `5563045` | the destination was visible and empty during the copy | `backup-interrupted` (killed before the rename) |

Not yet covered, by what each needs:

- **File edits and old-format fixtures:** `d810947`, `05b5b04`, `f5e3a70`, `7b176a7`, `4885808`, `1988457`, `42c1cc3`,
  `ccbedeb`, `737c853`, `af93562`, `08d8da9`, `a323ebe`, `28bfbbd`, `f189e01`, `3af0eee`, and two checks of crafted
  segments (a retention floor past a lagging group; a fence frame with a body).
- **Cases using the named points, not yet written** (some may need further points): `f65462a`, `ea2240a`, `1092515`,
  `cc081d4`, `ddc0cc8`, `d711c94`, `a0f24fa`, `b845b69` (as byte vectors), `a0d8420`.
- **No test of any kind today:** `ed99753`.

Also not covered: the wire protocol, interrupted migration, retention that removes whole segment files (the retention
case stays within one segment), and whether removed sidecar files are written again.
