# Workflow examples

Small programs on the experimental [`workflow`](../../workflow) package. Each one mirrors a pattern from
[restatedev/examples](https://github.com/restatedev/examples) (MIT), written from the pattern's behavior with our own
code. Restate is a server reached over HTTP; these are Go programs that embed the store.

| Directory | Pattern | Limit |
|---|---|---|
| `durablerpc` | durable RPC with idempotency | a one-shot claim, not per-key state |
| `queue` | message queue | immediate sends only; no delayed start |
| `webhook` | webhook callbacks | intake only; see trust below |
| `saga` | sagas | refusals are data; no compensation if a step uses up its attempts |
| `paymentcallback` | signals for payment callbacks | asynchronous path only; early callbacks must be redelivered; no deadline |
| `textingest` | RAG ingestion, text path | local stand-ins; no fan-out; an object name must always hold the same content |

Every directory has a test that restarts the store and loses an activity's result after its effect. These tests close
and reopen the store in one process; they do not kill the process. An activity can run again, so each outside call takes
a key and must be idempotent for it.

Trust: `webhook` accepts an event only with an HMAC-SHA256 of the body under a shared secret (`WEBHOOK_SECRET`, header
`X-Signature`, lowercase hex). It serves plain HTTP: listen on loopback, or put TLS in front of it. The other programs listen on
nothing. None of these is a production service.

Not here because the package lacks what they need: per-key objects, delayed sends and timers, child runs with joins,
buffered promises, compensation on failure, cancel, and stream-triggered starts.
