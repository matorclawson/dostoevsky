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

Not covered for workflows: named kill and failure points, concurrent polls, size limits, damaged and half-initialized
stores, and any past workflow fix (their `covers` lists are empty).

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

Not yet covered, by what each needs:

- **File edits and old-format fixtures:** `d810947`, `05b5b04`, `f5e3a70`, `7b176a7`, `4885808`, `1988457`, `42c1cc3`,
  `ccbedeb`, `737c853`, `af93562`, `08d8da9`, `a323ebe`, `28bfbbd`, `f189e01`, `3af0eee`, and two checks of crafted
  segments (a retention floor past a lagging group; a fence frame with a body).
- **Named failure or kill points in the engine, not yet built:** `f65462a`, `ea2240a`, `5a25d90`, `1092515`, `5563045`,
  `cc081d4`, `ddc0cc8`, `d711c94`, `a0f24fa`, `b845b69` (as byte vectors), `a0d8420`.
- **No test of any kind today:** `ed99753`.

Also not covered: the wire protocol, interrupted migration, retention that removes whole segment files (the retention
case stays within one segment), and whether removed sidecar files are written again.
