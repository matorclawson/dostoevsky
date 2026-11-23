# Retention crash points and durability assumptions

`Retain` builds a replacement journal in a temporary file next to the live one, then swaps it in. The steps, in order, and what is tested at each (see `retention_fault_test.go`):

| Step | Failure injected | State after the failure | Test |
| --- | --- | --- | --- |
| Write each frame to the temp file | first write, a later write | Old journal untouched, temp file removed, node usable, same request succeeds on retry | `TestRetentionFaultBeforeRename` |
| `fsync` the temp file | sync error | same as above | `TestRetentionFaultBeforeRename` |
| `rename` over `journal.v1` | rename error | same as above | `TestRetentionFaultBeforeRename` |
| `fsync` the directory | directory sync error | Retention **took effect** but `Retain` returns an error; old readers get `ErrExpired`; reopening keeps the new floor | `TestRetentionFaultDirectorySync` |
| Process death at each of the four steps | data directory copied at the step and opened | Before the rename: full old history and checkpoints. After the rename: retained history and checkpoints. Never a mix. A leftover `.retention-*` temp file does not prevent opening | `TestRetentionCrashPoints` |

Producer behaviour (`TestRetentionGroupsFloorsAndIDs`): a group with no checkpoint starts at offset 0 and gets `ErrExpired` below the floor; repeating `Retain` with the same floor is a no-op; a lower floor is `ErrInvalid`; an ID whose record expired is accepted as a new record, while an ID still retained returns the original; an emptied partition keeps counting offsets; a backup taken before retention keeps the whole history and one taken after honours the floor.

## What is not covered and depends on the filesystem

- Power loss. The tests simulate the process dying at each step by copying the directory, which assumes the file contents at that step are what the kernel had accepted. They do not show that `fsync` and directory `fsync` reach stable storage on a given filesystem or device.
- Rename atomicity and directory-entry durability are assumed from POSIX semantics on a local filesystem. Network filesystems and some overlay setups are not supported claims.
- A crash can leave a `.retention-*` temp file behind. Open ignores it; nothing removes it. Operators can delete it while the node is stopped.
- Disk full during the temp write returns the write error and removes the temp file; the temp file needs roughly the retained journal size in free space. Not exercised with a real full disk.
- Concurrent opens and mixed-version processes are out of scope here.
