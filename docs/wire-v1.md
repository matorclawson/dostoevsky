# Dostoevsky wire protocol, version 1 (draft)

**Status: draft.** The [`wire`](../wire) Go package encodes and decodes it; no server speaks it yet. It describes a native binary protocol that will sit beside the [HTTP API](clients.md), which stays. It makes no speed claim; any such claim will come from a checked-in receipt.

Test vectors for every message are in [`wire/testdata/vectors.json`](../wire/testdata/vectors.json). The file's `note` says what a receiver does with each invalid one. Where this text and a vector disagree, that is a bug to report, not a choice for the implementer.

## Transport

A TCP or unix-domain stream connection. One connection carries many requests at once. All integers are unsigned little endian unless stated. The default address is `127.0.0.1:8788`.

**The token is a reusable credential for the whole store**: every topic, group and operation. Whoever receives it has all of that until it is changed. Publish deduplication by id does nothing against a stolen token. So a client sends HELLO only over one of these:

- **TLS 1.2 or later to the intended server.** The client verifies the certificate chain and that the certificate names the host it meant to reach, against the system roots or a trust anchor or pin the user configured. There is no option to skip verification. The TLS handshake completes before HELLO is written. No data is sent or accepted as TLS early (0-RTT) data, because it can be replayed.
- **Plaintext on a loopback address**, judged by the address actually connected to, not by the name asked for.
- **Plaintext on a unix-domain socket.**

The two plaintext cases are a choice to trust the local machine: a process that can listen at that address or path before the server does receives the token. A server refuses to listen in plaintext anywhere else. A client never falls back from TLS to plaintext.

**Unix socket.** The server creates it inside a directory that only the server's user can enter (mode 0700), so that no other user can reach it at any moment, and gives the socket mode 0600. It does not follow a symbolic link. If anything already exists at the path it refuses to start, except that it may remove a socket owned by its own user while it holds the lock of its data directory.

## Handshake

The client speaks first, once per connection.

HELLO:

| field | size | meaning |
|---|---|---|
| magic | 4 | the ASCII bytes `DSTV` |
| max_version | u16 | highest version the client speaks; it speaks every version from 1 to this one |
| flags | u16 | 0 |
| token_len | u16 | at most 4096 |
| token | token_len | the bearer token |

WELCOME, always 17 bytes:

| field | size | meaning |
|---|---|---|
| magic | 4 | `DSTV` |
| status | u8 | 0, or an error code from the table below |
| version | u16 | the highest version both speak |
| max_body | u32 | largest frame body the server accepts; 1,056,768 in version 1 |
| window | u16 | requests a connection may have unanswered |
| window_bytes | u32 | request bytes a connection may have unanswered |

With a status other than 0, every later field is 0 and the server closes. A wrong magic, nonzero flags or a token_len above 4096 closes the connection with no reply. Before authentication a server reads at most 4,106 bytes. A token shorter than 16 bytes is `unauthorized`. The token is checked before the version. A server compares tokens in constant time (compare their SHA-256 digests, so length does not leak either) and never logs them.

A client validates WELCOME before it sends anything else: the magic; status 0; 1 <= version <= the max_version it sent; max_body exactly 1,056,768 in version 1; window at least 1; window_bytes at least max_body + 10, so that every legal frame can be admitted. Otherwise it closes. It does not retry with weaker transport security.

The client sends no frame until it has read and accepted WELCOME.

## Frames

After the handshake, both directions carry frames:

| field | size | meaning |
|---|---|---|
| length | u32 | bytes of body, not counting this 10-byte header |
| type | u8 | below |
| flags | u8 | 0 |
| request_id | u32 | chosen by the client, not 0, not reused while unanswered; the reply repeats it |
| body | length | |

Below, `str8` is a u8 length then that many bytes, `str16` a u16 length then that many bytes, and `rest` is every remaining byte of the body.

| type | name | body |
|---|---|---|
| 0x01 | PUBLISH | topic str8, key str16, id str16, value rest |
| 0x81 | PUBLISH_OK | partition u16, offset u64 |
| 0x02 | READ | topic str8, partition u16, offset u64, wait_ms u16 |
| 0x82 | READ_OK | found u8; if found is 1: partition u16, offset u64, time i64 (signed, nanoseconds since the Unix epoch, UTC), key str16, id str16, value rest |
| 0x03 | POSITION | topic str8, partition u16, group str8 |
| 0x83 | POSITION_OK | next u64, state rest |
| 0x04 | COMMIT | topic str8, partition u16, group str8, expected u64, state rest |
| 0x84 | COMMIT_OK | empty |
| 0x05 | PING | empty |
| 0x85 | PING_OK | empty |
| 0xFF | ERROR | code u8 |

The operations are those of the HTTP API. PUBLISH routes by key and PUBLISH_OK names the partition and offset. READ returns one record. A new group's position is 0 with empty state. COMMIT moves a group's cursor from `expected` to `expected` + 1 and stores `state` in the same commit.

`found` is 0 or 1; 0 means the read reached the end, including expiry of a bounded wait, and nothing follows it. An empty value is still found.

## Values

The same rules as the Go API and the HTTP API:

| value | rule |
|---|---|
| topic, group | 1 to 96 of `a-z A-Z 0-9 _ -` |
| key | valid UTF-8, at most 4096 bytes |
| id | valid UTF-8, at most 256 bytes; empty means no deduplication |
| value, state | any bytes, at most 1 MiB |
| offset, next, expected | below 2^63 |
| wait_ms | at most 30000; above that is `invalid` |

A topic or group outside its rule is `invalid`, whatever its length, as in the HTTP API.

A COMMIT whose `expected` is 2^63 - 1 has no representable next position: it is `limit`, and nothing is changed.

## Errors

An ERROR carries a code and never a message.

| code | name |
|---|---|
| 1 | invalid |
| 2 | unauthorized |
| 3 | conflict |
| 4 | wrong_owner |
| 5 | expired |
| 6 | limit |
| 7 | closed |
| 8 | timeout |
| 9 | canceled |
| 10 | storage_failure |
| 11 | protocol |
| 12 | unsupported_version |

**Request errors.** A frame that parses but breaks a value rule gets ERROR with its request_id (`limit` for something too long, otherwise `invalid`, or what the store reports). The connection stays open.

**Connection errors.** A frame from the client that does not parse (nonzero flags, request_id 0, a reused unanswered request_id, an unknown or wrong-direction type, lengths that overrun the body, bytes left over) gets ERROR with request_id 0 and code `protocol`, and the connection closes. A length above max_body is the same with code `limit`; the server rejects it from the header, before allocating or reading the body.

**What a client accepts.** The client checks a reply's length against max_body from the header, before allocating or reading the body. Each reply must answer exactly one unanswered request, by request_id, with that request's reply type or ERROR. An unknown or repeated request_id, a wrong type, nonzero flags, or a body that does not parse ends the connection: the client closes and sends nothing back. An ERROR with request_id 0 also ends it. Requests still unanswered then have unknown outcomes.

## What an acknowledgment means

- PUBLISH_OK and COMMIT_OK are sent only after the disk sync that covers the write. Pipelining never changes this.
- Deduplication is per partition and lasts only while the record is retained. A publish whose id repeats a retained record of its partition with the same key and value returns that record's partition and offset; a different key or value is `conflict`.
- If the connection is lost with requests unanswered, their outcome is **unknown**: they may or may not be stored. Retry a publish with the same topic, key, id and bytes; after a commit, reread the position. A client library never retries on its own a publish that has no id.

## Pipelining and order

- A client may send further requests before earlier ones are answered, within `window` and `window_bytes` (the sum of the unanswered frames, headers included). Replies may arrive in any order.
- PUBLISH and COMMIT frames of one connection take effect in the order the server receives them.
- A failed request does not cancel the ones behind it. A sender that needs a gapless sequence must keep one publish unanswered at a time, or detect the failure and repair.
- A READ or POSITION is guaranteed to see a write only if it was sent after that write's acknowledgment was received.
- A client library must keep reading replies while it writes; otherwise both sides can block.
- There is no cancel frame. Closing the connection ends waiting reads; a publish already received may still be stored.
- A waiting READ counts against the window.

## Server limits

Defaults, each configurable within the constraints given.

| limit | default | constraint |
|---|---|---|
| connections | 1024 | |
| connections not yet authenticated | 64 | |
| window per connection | 256 requests | at least 1 |
| window_bytes per connection | 4 MiB | at least max_body + 10 |
| request pool, whole server | 16 MiB | at least max_body + 10 |
| reply pool, whole server | 16 MiB | at least max_body + 10 |
| reply pool held by one connection | 2 maximum frames | at least 1 maximum frame |

A server refuses to start with a configuration that breaks a constraint.

### Admission and progress

Only authenticated connections draw on these budgets. Intake and replies have **separate** ones, so that pausing intake never stops admitted work from finishing.

- **Window.** A request counts against its connection's window and window_bytes from its header until its reply has been written. When the next frame does not fit, the server stops reading that connection, so TCP pushes back; it is not an error.
- **Request pool.** A frame's bytes are taken after its header is read and before its body is read, and given back once the server no longer holds those bytes: for PUBLISH and COMMIT after the write is durable or has failed, for READ and POSITION once the request is decoded. Nothing that holds request pool waits for any other budget.
- **Reply pool.** Before the store is read for a READ or POSITION reply, a whole maximum frame is taken; it is given back when the reply has been written or the connection has closed. **A READ that is waiting for a record holds none**: it takes it only once the record exists. All other replies are at most 20 bytes and come from a fixed reserve of window x 20 bytes per connection.
- Replies are written whether or not the connection is being read.
- So every holder of a budget can finish: a request needs no further budget, and a reply needs only the socket to drain, which has a deadline.
- When a connection closes, its waiting reads end and everything it holds is given back. A publish or commit already admitted still runs to completion and gives back its bytes.

### Connections and deadlines

Every deadline is absolute: it is set once and **receiving or sending part of the data does not extend it**. It also runs while the server is not reading or writing for its own reasons.

| deadline | from | to | default |
|---|---|---|---|
| authentication | accept | WELCOME fully written, TLS handshake and HELLO included, a refusal too | 5 s |
| one request frame | its first header byte | its last body byte, time waiting for admission included | 40 s |
| one reply | the reply is ready | its last byte written | 45 s |
| idle | nothing unanswered and no partial frame | the next frame's first byte | 60 s |

Passing one of the first three closes the connection; after the idle one the server may close it. A connection takes its slot at accept. With no slot free, either overall or among the unauthenticated, the server closes it at once without reading or writing. A server holds no unsent output for a closed connection.

## Versions

A server answers with the highest version both sides speak, or `unsupported_version`. Anything that changes bytes on the wire needs a new version; version 1 receivers reject flags and unknown types.
