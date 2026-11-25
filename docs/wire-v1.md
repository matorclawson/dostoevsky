# Dostoevsky wire protocol, version 1 (draft)

**Status: draft. Nothing implements this yet.** It describes a native binary protocol that will sit beside the [HTTP API](clients.md), which stays. It makes no speed claim; any such claim will come from a checked-in receipt.

Test vectors for every message are in [`wire/testdata/vectors.json`](../wire/testdata/vectors.json). The file's `note` says what a receiver does with each invalid one. Where this text and a vector disagree, that is a bug to report, not a choice for the implementer.

## Transport

A TCP or unix-domain stream connection. One connection carries many requests at once. All integers are unsigned little endian unless stated.

- Plaintext is allowed only on a unix socket or a loopback address. A server refuses to listen anywhere else without TLS 1.2 or later, and a client refuses to send its token anywhere else without TLS.
- A unix socket is created with mode 0600.
- The default address is `127.0.0.1:8788`.

## Handshake

The client speaks first, once per connection.

HELLO:

| field | size | meaning |
|---|---|---|
| magic | 4 | the ASCII bytes `DSTV` |
| max_version | u16 | highest version the client speaks |
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

The client sends no frame until it has read WELCOME.

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
| wait_ms | at most 30000 |

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
| 13 | busy |

**Request errors.** A frame that parses but breaks a value rule gets ERROR with its request_id (`limit` for something too long, otherwise `invalid`, or what the store reports). The connection stays open.

**Connection errors.** A frame that does not parse (nonzero flags, request_id 0, a reused unanswered request_id, an unknown or wrong-direction type, lengths that overrun the body, bytes left over) gets ERROR with request_id 0 and code `protocol`, and the connection closes. A length above max_body is the same with code `limit`; the receiver rejects it from the header, before allocating or reading the body. A client that receives a bad frame closes.

## What an acknowledgment means

- PUBLISH_OK and COMMIT_OK are sent only after the disk sync that covers the write. Pipelining never changes this.
- A publish with an id that repeats a retained record with the same key and value returns that record's partition and offset. A different key or value is `conflict`.
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

Defaults, each configurable. When a window or budget is full the server stops reading the connection, so TCP pushes back; it is not an error.

| limit | default |
|---|---|
| connections | 1024; over it, WELCOME status `busy` |
| connections not yet authenticated | 64 |
| window per connection | 256 requests, 4 MiB |
| request and reply bytes buffered, whole server | 32 MiB; a reply buffer is reserved before a record is read into it |
| handshake | complete within 5 s |
| one frame | complete within 40 s of its first byte |
| a reply the client does not drain | close after 45 s |
| idle, nothing unanswered | the server may close after 60 s |

## Versions

A server answers with the highest version both sides speak, or `unsupported_version`. Anything that changes bytes on the wire needs a new version; version 1 receivers reject flags and unknown types.
