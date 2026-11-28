# Native server review, 27 November 2026

**Disposition.** Carry the server forward on the staging branch. Final merge approval is pending independent Unix socket tests and a repeat on the target host.

I reviewed PR 72 at `3ccefe101939840cb97c47d460804c2decee59bf`, including token handling, TLS and listener policy, authentication and request deadlines, connection windows, pool accounting, queued writes, reply ownership, shutdown, journal durability and local socket replacement.

## Findings and changes

- PR 72 bounded connections, connection-level outstanding requests and long polls. Immediately dispatched read and position handlers could still accumulate across connections while waiting for reply capacity. Staging now takes a server-wide slot before creating each handler and releases it when the handler exits. Unit tests exercise capacity refusal, continued publication and recovery of capacity.
- The store already had a journal ceiling. The native command now exposes it as `-journal-cap` in bytes. Zero leaves it unlimited. A test confirms that a publication refused by the durable store returns the limit code. Existing records and administrative metadata require disk space of their own.
- The server compares token digests in constant time. Plain TCP is restricted to loopback, and TLS listeners require at least TLS 1.2. The token grants access to the entire store. Request/reply pools represent protocol bytes, and Go stacks, buffers, copied data, metadata and file-backed pages can increase process RSS beyond those budgets.
- The socket path checks include directory ownership and permissions, stale socket handling and exclusive ownership. The host denied creating Unix sockets. The Linux TCP, TLS, codec, race and fuzz results do not establish that local socket behavior passed.
- The existing Linux handshake and frame fuzz targets passed on the original review head, with 115,180 and 79,062 executions respectively. Three codec fuzz targets also ran. A second handshake/frame pass ran on staging after the fixes. The raw results are held with the review materials and are not published here.
- A Linux process-kill test issued concurrent durable publications, killed the native server, reopened the same directory, and checked both bytes and idempotent retries for every receipt observed by the client. A process kill does not emulate hardware power loss.

Run `go test -race ./wire ./wireserver ./cmd/dostoevsky-server` on a Linux host that permits local sockets. Run the Java and Rust interop suites there with DOSTOEVSKY_SERVER and separate disposable DOSTOEVSKY_WORK directories. Keep the staging and source-review commits identifiable in the merge record.
