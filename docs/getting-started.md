# Getting started

This guide takes you from an empty directory to a durable event log you can restart, replay, trim and back up. Every code block is copied from [`getting_started_test.go`](../getting_started_test.go), where `go test` runs it, and `TestDocSnippetsMatch` fails if a copy here drifts.

## 1. Install

In an empty directory:

```sh
go mod init example.com/guide
go get github.com/rodriveracom/dostoevsky@latest
```

Go 1.22 or later. The module has no dependencies outside the standard library.

## 2. Open a store

A store is a directory. The configuration names this node, lists all nodes (one, here) and declares each topic with its partition count. Paste this once at the top of `func main()`; every step below is pasted after it, as its own block:

```go
dir, err := os.MkdirTemp("", "dostoevsky-guide")
if err != nil {
	panic(err)
}
cfg := dostoevsky.Config{Node: "app", Nodes: []string{"app"}, Topics: map[string]int{"orders": 1}}
node, err := dostoevsky.Open(dir, cfg)
if err != nil {
	panic(err)
}
defer node.Close()
ctx := context.Background()
p := dostoevsky.Partition{Topic: "orders", Number: 0}
backupDir := dir + "-backup" // step 7's destination; it must not exist
```

**Each step is self-contained**: its block publishes whatever records it needs, so you can paste any step after the setup on its own. The steps are not one continuous session: pasted one after another against the same store, later steps would meet the records and cursors of earlier ones (for example, step 5 commits offset 0 of a group that step 4 already moved). The test `TestGuideCodeBlocksCompile` pastes every block in reading order, each step in its own braces, and compiles the result with `go vet`. Only one process can open a directory at a time. A second `Open` fails on the lock instead of corrupting the journal.

## 3. Publish and read

`Publish` returns after the record is on disk. A stable, non-empty `ID` makes a retry safe, so a client that lost a response can send the same message again:

<!-- snippet:publish -->
```go
rec, err := node.Publish(ctx, "orders", dostoevsky.Message{Key: "account-42", ID: "order-1", Value: []byte("paid")})
if err != nil {
	panic(err)
}
// Retrying with the same ID returns the original record instead of a duplicate.
again, err := node.Publish(ctx, "orders", dostoevsky.Message{Key: "account-42", ID: "order-1", Value: []byte("paid")})
if err != nil {
	panic(err)
}
got, err := node.Read(ctx, p, rec.Offset, false)
if err != nil {
	panic(err)
}
fmt.Println(rec.Offset, again.Offset, string(got.Value))
```

A record's key picks its partition, and its offset is its position in that partition. Deduplication applies within the partition the key routes to and only for a supplied, non-empty ID, until retention removes the record. Reusing an ID in that partition with a different key or payload returns `ErrConflict`.

## 4. Consumer groups

A consumer group has its own cursor per partition, so groups never interfere. `Commit` moves the cursor from the offset you expect to the next one, and it only succeeds if nobody moved it first:

<!-- snippet:groups -->
```go
for _, id := range []string{"a", "b", "c"} {
	if _, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: id, Value: []byte(id)}); err != nil {
		panic(err)
	}
}
// "billing" consumes two records. "audit" has not started and still sees all three.
for offset := int64(0); offset < 2; offset++ {
	if err := node.Commit(ctx, p, "billing", offset, nil); err != nil {
		panic(err)
	}
}
billing, _ := node.Position(ctx, p, "billing")
audit, _ := node.Position(ctx, p, "audit")
// A second commit at an offset that already moved loses: one winner per cursor.
stale := node.Commit(ctx, p, "billing", 0, nil)
fmt.Println(billing.Next, audit.Next, errors.Is(stale, dostoevsky.ErrConflict))
```

For stateful processing use `Fold`, which commits your reducer's state and the cursor together; see the [quickstart in the README](../README.md) and [how it works](how-it-works.md#fold-state-and-offset-move-together).

## 5. Restart and resume

Nothing is lost when the process stops. Reopen the directory with the same configuration and every record, offset and group state is back:

<!-- snippet:restart -->
```go
for _, id := range []string{"a", "b"} {
	if _, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: id, Value: []byte(id)}); err != nil {
		panic(err)
	}
}
if err := node.Commit(ctx, p, "billing", 0, []byte(`{"seen":1}`)); err != nil {
	panic(err)
}
if err := node.Close(); err != nil {
	panic(err)
}
// Reopening the same directory recovers records, offsets and group state.
node, err = dostoevsky.Open(dir, cfg)
if err != nil {
	panic(err)
}
pos, _ := node.Position(ctx, p, "billing")
fmt.Println(pos.Next, string(pos.State))
```

Opening scans the journal, so reopen time grows with the retained history. See [memory and limits](how-it-works.md#memory-what-is-in-ram-and-what-is-not).

## 6. Retention

History grows until you trim it. `Retain` removes everything below an absolute offset per partition. It refuses a floor above any group's cursor, so it cannot remove records a known consumer still needs. A group that starts later begins at offset zero and must handle `ErrExpired`:

<!-- snippet:retention -->
```go
for i := 0; i < 5; i++ {
	if _, err := node.Publish(ctx, "orders", dostoevsky.Message{Value: []byte(fmt.Sprint("event-", i))}); err != nil {
		panic(err)
	}
}
for offset := int64(0); offset < 3; offset++ {
	if err := node.Commit(ctx, p, "billing", offset, nil); err != nil {
		panic(err)
	}
}
// Remove every record below offset 3. A floor above any group's cursor is refused.
if err := node.Retain(ctx, map[dostoevsky.Partition]int64{p: 3}); err != nil {
	panic(err)
}
_, expired := node.Read(ctx, p, 0, false)
kept, _ := node.Read(ctx, p, 3, false)
fmt.Println(errors.Is(expired, dostoevsky.ErrExpired), string(kept.Value))
```

Retention rewrites the journal and needs room for a second copy while it runs. Producer IDs expire with their records, so keep the floor older than your longest retry horizon.

## 7. Backup

`Backup` copies a consistent journal into a new directory and syncs it before returning. It pauses reads and writes while it copies:

<!-- snippet:backup -->
```go
if _, err := node.Publish(ctx, "orders", dostoevsky.Message{ID: "order-1", Value: []byte("paid")}); err != nil {
	panic(err)
}
// The destination must not exist. Backup syncs the copy before it returns.
if err := node.Backup(ctx, backupDir); err != nil {
	panic(err)
}
copyNode, err := dostoevsky.Open(backupDir, cfg)
if err != nil {
	panic(err)
}
defer copyNode.Close()
rec, _ := copyNode.Read(ctx, p, 0, false)
fmt.Println(string(rec.Value))
```

Open the copy with the same configuration. The destination must be a path nobody else touches until `Backup` returns. After an error, delete any hidden `.<name>.backup-*` directory it left.

## Next

- [Integrations](integrations.md): an HTTP handler, a multi-node mesh, the task queue, and coming from NATS.
- [How it works](how-it-works.md): group commit, the durability contract, sketches and memory.
