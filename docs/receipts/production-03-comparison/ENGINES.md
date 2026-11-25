# Final comparison engine labels

- Dostoevsky at `6f35c74b`, JetStream and SQLite run in process. SQLite uses a single writer.
- Redis 8.12.0 and Valkey 9.2.0 use AOF with fsync always.
- Kafka 4.4.0 uses `acks=all`. Fsync per message is inferred from `log.flush.interval.messages=1`.
- RabbitMQ 4.4.1 uses the vendor default durability, unverified. Erlang/OTP 27.3.4.20 is the Hex.pm prebuilt. The archive SHA-256 matched the Hex.pm build index. No independent checksum file is published.
