//! One adapter per engine. Client identities come from the pinned manifest and our client's own Cargo.toml and git,
//! never from typed names; engine error text never leaves this file (only the class and a kind).
use dostoevsky_bench_core::{pack_id, unpack_id, Adapter, EngineError, Factory, Kind};
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

const TOPIC: &str = "events";

/// Where the repository is, for our own client's manifest and git state.
fn repo() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var("BENCH_REPO").unwrap_or_else(|_| ".".into()))
}

/// "name-version" from a Cargo.toml's [package] table (the first name and version lines).
fn manifest_identity(path: &std::path::Path) -> Result<String, String> {
    let text = std::fs::read_to_string(path).map_err(|_| "cannot read the client's manifest".to_string())?;
    let field = |key: &str| text.lines().find_map(|l| l.trim().strip_prefix(&format!("{key} = \"")).and_then(|r| r.strip_suffix('"')).map(String::from));
    match (field("name"), field("version")) {
        (Some(n), Some(v)) => Ok(format!("{n}-{v}")),
        _ => Err("no name and version in the client's manifest".into()),
    }
}

/// The git commit of the checkout holding `dir` (12 hex), "-dirty" when that directory has uncommitted changes,
/// "unknown" when git cannot say. Asks about "." inside `dir` and discards git's own error text.
fn build_id(dir: &std::path::Path) -> String {
    let git = |args: &[&str]| -> Option<String> {
        let out = std::process::Command::new("git").arg("-C").arg(dir).args(args).stderr(std::process::Stdio::null()).output().ok()?;
        out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    match (git(&["rev-parse", "--short=12", "HEAD"]), git(&["status", "--porcelain", "--untracked-files=no", "--", "."])) {
        (Some(sha), Some(dirty)) => format!("{sha}{}", if dirty.is_empty() { "" } else { "-dirty" }),
        _ => "unknown".to_string(),
    }
}

/// The lockfile this binary was built from, embedded at build time: the client names in receipts come from the exact
/// versions that were compiled in, and cannot depend on a file that may be missing at run time.
const LOCK: &str = include_str!("../Cargo.lock");

/// "crate-version" of a locked dependency, or an error: the worker refuses to run rather than report "unknown".
fn locked(krate: &str) -> Result<String, String> {
    let wanted = format!("name = \"{krate}\"");
    let mut lines = LOCK.lines();
    while let Some(l) = lines.next() {
        if l == wanted {
            if let Some(v) = lines.next().and_then(|x| x.strip_prefix("version = \"")).and_then(|x| x.strip_suffix('"')) {
                return Ok(format!("{krate}-{v}"));
            }
        }
    }
    Err(format!("{krate} is not in the embedded Cargo.lock"))
}

fn host_port(addr: &str) -> Result<(String, u16), String> {
    let (h, p) = addr.rsplit_once(':').ok_or("address has no port")?;
    Ok((h.to_string(), p.parse().map_err(|_| "bad port")?))
}

// ---- our client through the gateway ----
struct Own {
    client: dostoevsky_client::Client,
    name: String,
    build: String,
}

impl Adapter for Own {
    fn publish(&self, data: &[u8]) -> Result<u64, EngineError> {
        self.client.publish(TOPIC, data, "", "").map(|r| r.offset as u64).map_err(own_error)
    }
    fn read(&self, offset: u64) -> Result<Vec<u8>, EngineError> {
        match self.client.read(TOPIC, 0, offset as i64, 0) {
            Ok(Some(r)) => Ok(r.value),
            Ok(None) => Err(EngineError::new("PastTheEnd", Kind::Other)),
            Err(e) => Err(own_error(e)),
        }
    }
    fn close(&self) {
        self.client.close();
    }
    fn client_name(&self) -> String {
        self.name.clone()
    }
    fn client_build(&self) -> String {
        self.build.clone()
    }
}

fn own_error(e: dostoevsky_client::Error) -> EngineError {
    match e {
        dostoevsky_client::Error::Api(..) => EngineError::new("ApiError", Kind::Other),
        dostoevsky_client::Error::Transport(m) if m.contains("timed out") || m.contains("WouldBlock") => EngineError::new("TransportError", Kind::AmbiguousTimeout),
        dostoevsky_client::Error::Transport(_) => EngineError::new("TransportError", Kind::Connection),
        dostoevsky_client::Error::InvalidResponse(_) => EngineError::new("InvalidResponse", Kind::Protocol),
    }
}

fn own(addr: &str, token: &str, _workers: usize) -> Result<Arc<dyn Adapter>, String> {
    let dir = repo().join("clients/rust");
    Ok(Arc::new(Own {
        client: dostoevsky_client::Client::new(&format!("http://{addr}"), token),
        name: manifest_identity(&dir.join("Cargo.toml"))?,
        build: build_id(&dir),
    }))
}

// ---- rdkafka: acks=all, no retries, no idempotence, one message per batch ----
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};

type Opaque = Box<mpsc::Sender<Result<u64, bool>>>; // Err(true): the broker gave no answer in time

struct Ctx;
impl ClientContext for Ctx {}
impl ProducerContext for Ctx {
    type DeliveryOpaque = Opaque;
    fn delivery(&self, result: &DeliveryResult<'_>, tx: Opaque) {
        let _ = tx.send(match result {
            Ok(m) => Ok(m.offset() as u64),
            Err((e, _)) => Err(e.to_string().to_lowercase().contains("timed out")),
        });
    }
}

struct Kafka {
    addr: String,
    producer: ThreadedProducer<Ctx>,
    consumer: Mutex<Option<BaseConsumer>>,
    loaded: Mutex<BTreeMap<u64, Vec<u8>>>,
    name: String,
    /// A delivery wait ran out: the broker may or may not have stored that message, and work may still be queued.
    timed_out: std::sync::atomic::AtomicBool,
}

impl Adapter for Kafka {
    fn publish(&self, data: &[u8]) -> Result<u64, EngineError> {
        let (tx, rx) = mpsc::channel();
        let record = BaseRecord::<(), [u8], Opaque>::with_opaque_to(TOPIC, Box::new(tx)).payload(data);
        if self.producer.send(record).is_err() {
            return Err(EngineError::new("KafkaError", Kind::Other));
        }
        match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(Ok(offset)) => Ok(offset),
            Ok(Err(true)) | Err(_) => {
                self.timed_out.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(EngineError::new("KafkaError", Kind::AmbiguousTimeout))
            }
            Ok(Err(false)) => Err(EngineError::new("KafkaError", Kind::Other)),
        }
    }
    fn read(&self, offset: u64) -> Result<Vec<u8>, EngineError> {
        let mut guard = self.consumer.lock().unwrap();
        if guard.is_none() {
            let c: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.addr)
                .set("group.id", "verify")
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .map_err(|_| EngineError::new("KafkaError", Kind::Connection))?;
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(TOPIC, 0, Offset::Beginning).map_err(|_| EngineError::new("KafkaError", Kind::Protocol))?;
            c.assign(&tpl).map_err(|_| EngineError::new("KafkaError", Kind::Protocol))?;
            *guard = Some(c);
        }
        let c = guard.as_ref().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(v) = self.loaded.lock().unwrap().remove(&offset) {
                return Ok(v);
            }
            if std::time::Instant::now() > deadline {
                return Err(EngineError::new("KafkaError", Kind::Timeout));
            }
            if let Some(Ok(m)) = c.poll(Duration::from_secs(1)) {
                self.loaded.lock().unwrap().insert(m.offset() as u64, m.payload().unwrap_or(&[]).to_vec());
            }
        }
    }
    fn close(&self) {
        if self.timed_out.load(std::sync::atomic::Ordering::SeqCst) {
            // After an ambiguous timeout nothing still queued may be sent now: drop it, then give the library a short moment.
            self.producer.purge(rdkafka::producer::PurgeConfig::default().queue().inflight());
        }
        let _ = self.producer.flush(Duration::from_secs(2));
    }
    fn client_name(&self) -> String {
        self.name.clone()
    }
}

fn kafka(addr: &str, _token: &str, _workers: usize) -> Result<Arc<dyn Adapter>, String> {
    let producer: ThreadedProducer<Ctx> = ClientConfig::new()
        .set("bootstrap.servers", addr)
        .set("acks", "all")
        .set("enable.idempotence", "false")
        .set("retries", "0")
        .set("linger.ms", "0")
        .set("batch.num.messages", "1")
        .set("delivery.timeout.ms", "60000")
        .create_with_context(Ctx)
        .map_err(|_| "kafka producer".to_string())?;
    let (_, librdkafka) = rdkafka::util::get_rdkafka_version();
    Ok(Arc::new(Kafka {
        addr: addr.to_string(),
        producer,
        consumer: Mutex::new(None),
        loaded: Mutex::new(BTreeMap::new()),
        timed_out: std::sync::atomic::AtomicBool::new(false),
        name: format!("{}+librdkafka-{}", locked("rdkafka")?, librdkafka),
    }))
}

// ---- async-nats on a small tokio runtime; each producer thread blocks on its own JetStream publish ----
struct Nats {
    rt: tokio::runtime::Runtime,
    js: async_nats::jetstream::Context,
    stream: async_nats::jetstream::stream::Stream,
    name: String,
}

fn nats_error(e: &dyn std::fmt::Display, kind_if_timeout: Kind) -> EngineError {
    let s = e.to_string().to_lowercase(); // inspected here, never printed
    if s.contains("timed out") || s.contains("timeout") {
        EngineError::new("NatsError", kind_if_timeout)
    } else if s.contains("connection") || s.contains("disconnected") {
        EngineError::new("NatsError", Kind::Connection)
    } else {
        EngineError::new("NatsError", Kind::Other)
    }
}

impl Adapter for Nats {
    fn publish(&self, data: &[u8]) -> Result<u64, EngineError> {
        let bytes = bytes::Bytes::from(data.to_vec());
        self.rt
            .block_on(async {
                let ack = self.js.publish(TOPIC, bytes).await.map_err(|e| nats_error(&e, Kind::AmbiguousTimeout))?;
                ack.await.map(|a| a.sequence).map_err(|e| nats_error(&e, Kind::AmbiguousTimeout))
            })
    }
    fn read(&self, offset: u64) -> Result<Vec<u8>, EngineError> {
        self.rt.block_on(async { self.stream.get_raw_message(offset).await.map(|m| m.payload.to_vec()).map_err(|e| nats_error(&e, Kind::Timeout)) })
    }
    fn client_name(&self) -> String {
        self.name.clone()
    }
}

fn nats(addr: &str, token: &str, workers: usize) -> Result<Arc<dyn Adapter>, String> {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(std::cmp::max(2, workers.min(8))).enable_time().enable_io().build().map_err(|_| "runtime".to_string())?;
    let (js, stream) = rt
        .block_on(async {
            let client = async_nats::ConnectOptions::with_token(token.to_string()).retry_on_initial_connect().connect(addr).await.map_err(|_| "connect".to_string())?;
            let js = async_nats::jetstream::new(client);
            let stream = js
                .create_stream(async_nats::jetstream::stream::Config {
                    name: "EVENTS".into(),
                    subjects: vec![TOPIC.into()],
                    storage: async_nats::jetstream::stream::StorageType::File,
                    num_replicas: 1,
                    ..Default::default()
                })
                .await
                .map_err(|_| "create stream".to_string())?;
            Ok::<_, String>((js, stream))
        })?;
    Ok(Arc::new(Nats { rt, js, stream, name: locked("async-nats")? }))
}

// ---- redis: XADD and XRANGE by raw command, so bytes stay bytes; one blocking connection per producer thread ----
struct Redis {
    client: redis::Client,
    idle: Mutex<Vec<redis::Connection>>,
    name: String,
}

fn redis_error(e: &redis::RedisError, publishing: bool) -> EngineError {
    if e.is_timeout() {
        EngineError::new("RedisError", if publishing { Kind::AmbiguousTimeout } else { Kind::Timeout })
    } else if e.is_connection_dropped() || e.is_connection_refusal() {
        EngineError::new("RedisError", Kind::Connection)
    } else {
        EngineError::new("RedisError", Kind::Other)
    }
}

impl Redis {
    fn with<T>(&self, publishing: bool, f: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>) -> Result<T, EngineError> {
        let taken = self.idle.lock().unwrap().pop();
        let mut con = match taken {
            Some(c) => c,
            None => {
                let c = self.client.get_connection_with_timeout(Duration::from_secs(10)).map_err(|e| redis_error(&e, publishing))?;
                let _ = c.set_read_timeout(Some(Duration::from_secs(60)));
                let _ = c.set_write_timeout(Some(Duration::from_secs(60)));
                c
            }
        };
        let r = f(&mut con);
        if r.is_ok() {
            self.idle.lock().unwrap().push(con);
        }
        r.map_err(|e| redis_error(&e, publishing))
    }
}

impl Adapter for Redis {
    fn publish(&self, data: &[u8]) -> Result<u64, EngineError> {
        let id: String = self.with(true, |c| redis::cmd("XADD").arg(TOPIC).arg("*").arg("d").arg(data).query(c))?;
        pack_id(&id).map_err(|_| EngineError::new("StreamId", Kind::Protocol))
    }
    fn read(&self, offset: u64) -> Result<Vec<u8>, EngineError> {
        let id = unpack_id(offset);
        let rows: Vec<(String, Vec<Vec<u8>>)> = self.with(false, |c| redis::cmd("XRANGE").arg(TOPIC).arg(&id).arg(&id).query(c))?;
        match rows.as_slice() {
            [(_, fields)] if fields.len() == 2 => Ok(fields[1].clone()),
            _ => Err(EngineError::new("StreamEntry", Kind::Protocol)),
        }
    }
    fn client_name(&self) -> String {
        self.name.clone()
    }
}

fn redis_adapter(addr: &str, token: &str, _workers: usize) -> Result<Arc<dyn Adapter>, String> {
    let (host, port) = host_port(addr)?;
    // serve's token is hex, so nothing in it needs URL encoding; it lives only in memory, never in an argument.
    let url = format!("redis://:{token}@{host}:{port}/");
    let client = redis::Client::open(url.as_str()).map_err(|_| "redis client".to_string())?;
    Ok(Arc::new(Redis { client, idle: Mutex::new(Vec::new()), name: locked("redis")? }))
}

pub fn engines() -> BTreeMap<String, Factory> {
    let mut m: BTreeMap<String, Factory> = BTreeMap::new();
    m.insert("dostoevsky".into(), Box::new(own));
    m.insert("kafka".into(), Box::new(kafka));
    m.insert("nats".into(), Box::new(nats));
    m.insert("redis".into(), Box::new(redis_adapter));
    m
}
