package io.github.rodriveracom.dostoevsky.bench;

import io.github.rodriveracom.dostoevsky.Client;
import io.github.rodriveracom.dostoevsky.NativeClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One adapter per engine. Identities come from the dependency lock (deps.lock, the file the jars were
 * verified against) and from our client's own pom and git, never from typed names.
 */
public final class Adapters {
    private Adapters() {}

    static final String TOPIC = "events";

    /** "artifact-version" of a locked dependency, read from deps.lock (lines: group:artifact:version sha256). */
    static String locked(String groupArtifact) throws Exception {
        Path lock = Path.of(System.getProperty("bench.deps", "deps.lock"));
        for (String line : Files.readAllLines(lock)) {
            String[] f = line.trim().split("\\s+");
            if (f.length >= 2 && f[0].startsWith(groupArtifact + ":")) {
                String[] gav = f[0].split(":");
                return gav[1] + "-" + gav[2];
            }
        }
        throw new IllegalStateException(groupArtifact + " is not in " + lock);
    }

    /** Our client: name and version from clients/java/pom.xml, build from git. */
    static String ownClientName() throws Exception {
        String pom = Files.readString(Path.of(System.getProperty("bench.repo", "."), "clients/java/pom.xml"));
        Matcher a = Pattern.compile("<artifactId>([^<]+)</artifactId>\\s*<version>([^<]+)</version>").matcher(pom);
        if (!a.find()) throw new IllegalStateException("cannot read the client's identity from its pom");
        return a.group(1) + "-" + a.group(2);
    }

    static String buildId() {
        return Build.id(Path.of(System.getProperty("bench.repo", "."), "clients/java"));
    }

    /** An engine's timeout means no answer in time: the message may have been stored. Say so. */
    static RuntimeException ambiguousIfTimeout(Throwable e) {
        String s = e.getClass().getName() + " " + e.getMessage();
        if (s.toLowerCase().matches("(?s).*(timeout|timed out).*")) return new Core.AmbiguousOutcome("no answer before the deadline", e);
        return e instanceof RuntimeException r ? r : new RuntimeException(e);
    }

    // ---- our client through the gateway ----
    static Core.Adapter dostoevsky(String addr, String token, int workers) throws Exception {
        Client c = new Client("http://" + addr, token);
        String name = ownClientName(), build = buildId();
        return new Core.Adapter() {
            public long publish(byte[] data) throws Exception { return c.publish(TOPIC, data, "", "").offset(); }

            public byte[] read(long offset) throws Exception {
                return c.read(TOPIC, 0, offset, 0).orElseThrow(() -> new IllegalStateException("offset is past the end")).value();
            }

            public void close() {}

            public String clientName() { return name; }

            public String clientBuild() { return build; }
        };
    }

    static Core.Adapter nativeSocket(String addr, String token, int workers) throws Exception {
        int split = addr.lastIndexOf(':');
        String name = ownClientName(), build = buildId();
        NativeClient c = NativeClient.connect(addr.substring(0, split), Integer.parseInt(addr.substring(split+1)), token);
        return new Core.Adapter() {
            public long publish(byte[] data) throws Exception {
                try { return c.publish(TOPIC, data, "", "").offset(); }
                catch (NativeClient.ConnectionLost e) {
                    if (e.sent) throw new Core.AmbiguousOutcome("connection ended after submission", e);
                    throw e;
                }
            }
            public byte[] read(long offset) throws Exception {
                return c.read(TOPIC, 0, offset, 0).orElseThrow().value();
            }
            public void close() { c.close(); }
            public String clientName() { return name; }
            public String clientBuild() { return build; }
        };
    }

    // ---- kafka-clients: acks=all, no retries, no idempotence, one record per batch ----
    static Core.Adapter kafka(String addr, String token, int workers) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", addr);
        p.put("acks", "all");
        p.put("enable.idempotence", "false");
        p.put("retries", "0");
        p.put("linger.ms", "0");
        p.put("batch.size", "1");
        p.put("delivery.timeout.ms", "60000");
        p.put("request.timeout.ms", "30000");
        p.put("max.block.ms", "60000");
        p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(p);
        String name = locked("org.apache.kafka:kafka-clients");
        return new Core.Adapter() {
            org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer;
            final java.util.Map<Long, byte[]> loaded = new java.util.HashMap<>();

            public long publish(byte[] data) throws Exception {
                try {
                    return producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, data)).get(60, TimeUnit.SECONDS).offset();
                } catch (java.util.concurrent.ExecutionException e) {
                    throw ambiguousIfTimeout(e.getCause() == null ? e : e.getCause());
                } catch (java.util.concurrent.TimeoutException e) {
                    throw ambiguousIfTimeout(e);
                }
            }

            public byte[] read(long offset) throws Exception {
                if (consumer == null) {
                    Properties c = new Properties();
                    c.put("bootstrap.servers", addr);
                    c.put("group.id", "verify");
                    c.put("enable.auto.commit", "false");
                    c.put("auto.offset.reset", "earliest");
                    c.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                    c.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                    consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(c);
                    org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition(TOPIC, 0);
                    consumer.assign(List.of(tp));
                    consumer.seekToBeginning(List.of(tp));
                }
                long deadline = System.nanoTime() + 60_000_000_000L;
                while (!loaded.containsKey(offset)) {
                    for (org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofSeconds(1))) loaded.put(r.offset(), r.value());
                    if (System.nanoTime() > deadline) throw new java.util.concurrent.TimeoutException("offset not found within the verification deadline");
                }
                return loaded.remove(offset);
            }

            public void close() {
                if (consumer != null) consumer.close(Duration.ofSeconds(5));
                producer.close(Duration.ofSeconds(5));
            }

            public String clientName() { return name; }
        };
    }

    // ---- jnats: JetStream publish, synchronous ----
    static Core.Adapter nats(String addr, String token, int workers) throws Exception {
        io.nats.client.Options o = new io.nats.client.Options.Builder().server("nats://" + addr).token(token.toCharArray())
            .noReconnect().connectionTimeout(Duration.ofSeconds(10)).build();
        io.nats.client.Connection nc = io.nats.client.Nats.connect(o);
        io.nats.client.JetStreamManagement jsm = nc.jetStreamManagement();
        jsm.addStream(io.nats.client.api.StreamConfiguration.builder().name("EVENTS").subjects(TOPIC)
            .storageType(io.nats.client.api.StorageType.File).replicas(1).build());
        io.nats.client.JetStream js = nc.jetStream(io.nats.client.JetStreamOptions.builder().requestTimeout(Duration.ofSeconds(60)).build());
        String name = locked("io.nats:jnats");
        return new Core.Adapter() {
            public long publish(byte[] data) throws Exception {
                try {
                    return js.publish(TOPIC, data).getSeqno();
                } catch (java.io.IOException | RuntimeException e) {
                    throw ambiguousIfTimeout(e);
                }
            }

            public byte[] read(long offset) throws Exception { return jsm.getMessage("EVENTS", offset).getData(); }

            public void close() throws Exception { nc.close(); }

            public String clientName() { return name; }
        };
    }

    // ---- Jedis: XADD and XRANGE through sendCommand, binary-safe ----
    static Core.Adapter redis(String addr, String token, int workers) throws Exception {
        String host = addr.substring(0, addr.lastIndexOf(':'));
        int port = Integer.parseInt(addr.substring(addr.lastIndexOf(':') + 1));
        redis.clients.jedis.JedisPoolConfig cfg = new redis.clients.jedis.JedisPoolConfig();
        cfg.setMaxTotal(workers + 8);
        redis.clients.jedis.JedisPool pool = new redis.clients.jedis.JedisPool(cfg, host, port, 60_000, token);
        String name = locked("redis.clients:jedis");
        byte[] key = TOPIC.getBytes(StandardCharsets.UTF_8);
        return new Core.Adapter() {
            public long publish(byte[] data) throws Exception {
                try (redis.clients.jedis.Jedis j = pool.getResource()) {
                    Object id = j.sendCommand(redis.clients.jedis.Protocol.Command.XADD, key, "*".getBytes(), "d".getBytes(), data);
                    return Core.packId(new String((byte[]) id, StandardCharsets.UTF_8));
                } catch (RuntimeException e) {
                    throw ambiguousIfTimeout(e);
                }
            }

            @SuppressWarnings("unchecked")
            public byte[] read(long offset) throws Exception {
                byte[] id = Core.unpackId(offset).getBytes(StandardCharsets.UTF_8);
                try (redis.clients.jedis.Jedis j = pool.getResource()) {
                    List<Object> rows = (List<Object>) j.sendCommand(redis.clients.jedis.Protocol.Command.XRANGE, key, id, id);
                    if (rows.size() != 1) throw new IllegalStateException("stream id has " + rows.size() + " entries");
                    List<Object> fields = (List<Object>) ((List<Object>) rows.get(0)).get(1);
                    return (byte[]) fields.get(1);
                }
            }

            public void close() { pool.close(); }

            public String clientName() { return name; }
        };
    }

    public static final Map<String, Worker.Factory> ENGINES = Map.of(
        "native", Adapters::nativeSocket, "dostoevsky", Adapters::dostoevsky, "kafka", Adapters::kafka, "nats", Adapters::nats, "redis", Adapters::redis);
}
