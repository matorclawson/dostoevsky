//! Native client for the Dostoevsky wire protocol, version 1 (`docs/wire-v1.md`): one connection,
//! many requests at once. Standard library only.
//!
//! PREVIEW: no released Dostoevsky server speaks this protocol yet.
//!
//! The standard library has no TLS, so this client speaks plaintext to a loopback address or a
//! unix socket only. Nothing is retried and there is no reconnect: after the connection is lost
//! every call fails, and a request that may have reached the server has an unknown outcome
//! ([`NativeError::ConnectionLost`] with `sent: true`).

use std::{
    collections::HashMap,
    fmt,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{Position, Receipt, Record};

const MAX_BODY: usize = 1_056_768;
const MAX_VALUE: usize = 1 << 20;
const MAX_KEY: usize = 4096;
const MAX_ID: usize = 256;
const MAX_TOKEN: usize = 4096;
const MAX_WAIT: u64 = 30_000; // milliseconds
const HEADER: usize = 10;
const WELCOME_BYTES: usize = 17;

const PUBLISH: u8 = 0x01;
const READ: u8 = 0x02;
const POSITION: u8 = 0x03;
const COMMIT: u8 = 0x04;
const PING: u8 = 0x05;
const ERROR: u8 = 0xFF;
const REPLY: u8 = 0x80;

// The client's own ceilings. The server's window can only lower what is in flight.
const MAX_IN_FLIGHT: usize = 256;
const MAX_IN_FLIGHT_BYTES: usize = 4 << 20;
const HANDSHAKE_TIME: Duration = Duration::from_secs(5);
const REQUEST_TIME: Duration = Duration::from_secs(40);
const CHUNK: usize = 65536;
const TICK: Duration = Duration::from_secs(1); // how often an idle connection checks deadlines
const KEEP_BUFFER: usize = 2 * CHUNK; // a larger receive buffer is given back once empty

/// What a native call can fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeError {
    /// An error code, from the server or from this client's own check of a value, with the HTTP
    /// status the gateway uses for the same name.
    Api(u16, &'static str),
    /// The connection ended. `sent` true: the request may have taken effect.
    ConnectionLost { sent: bool },
    /// The client will not connect: the address is not one the token may be sent to.
    Refused(String),
}

impl fmt::Display for NativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(status, code) => write!(f, "{status} {code}"),
            Self::ConnectionLost { sent: true } => f.write_str("connection lost; outcome unknown"),
            Self::ConnectionLost { sent: false } => f.write_str("connection lost; not sent"),
            Self::Refused(why) => f.write_str(why),
        }
    }
}
impl std::error::Error for NativeError {}

type Result<T> = std::result::Result<T, NativeError>;

const NOT_SENT: NativeError = NativeError::ConnectionLost { sent: false };
const SENT: NativeError = NativeError::ConnectionLost { sent: true };
const INVALID: NativeError = NativeError::Api(400, "invalid");
const LIMIT: NativeError = NativeError::Api(413, "limit");
const PROTOCOL: NativeError = NativeError::Api(400, "protocol");

fn failure(code: u8) -> Option<NativeError> {
    let (status, name) = match code {
        1 => (400, "invalid"),
        2 => (401, "unauthorized"),
        3 => (409, "conflict"),
        4 => (409, "wrong_owner"),
        5 => (410, "expired"),
        6 => (413, "limit"),
        7 => (503, "closed"),
        8 => (504, "timeout"),
        9 => (408, "canceled"),
        10 => (500, "storage_failure"),
        11 => (400, "protocol"),
        12 => (400, "unsupported_version"),
        _ => return None,
    };
    Some(NativeError::Api(status, name))
}

// -- encoding

fn hello(token: &str) -> Result<Vec<u8>> {
    if token.len() > MAX_TOKEN {
        return Err(LIMIT);
    }
    let mut out = b"DSTV".to_vec();
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(token.len() as u16).to_le_bytes());
    out.extend_from_slice(token.as_bytes());
    Ok(out)
}

/// A frame with request id 0 and no body yet; `finish_frame` writes the length.
fn frame(kind: u8, body_size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + body_size);
    out.resize(HEADER, 0);
    out[4] = kind;
    out
}

fn finish_frame(mut out: Vec<u8>) -> Result<Vec<u8>> {
    let length = out.len() - HEADER;
    if length > MAX_BODY {
        return Err(LIMIT);
    }
    out[..4].copy_from_slice(&(length as u32).to_le_bytes());
    Ok(out)
}

fn str8(out: &mut Vec<u8>, text: &str) -> Result<()> {
    if text.len() > 255 {
        return Err(LIMIT);
    }
    out.push(text.len() as u8);
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

fn str16(out: &mut Vec<u8>, text: &str, limit: usize) -> Result<()> {
    if text.len() > limit {
        return Err(LIMIT);
    }
    out.extend_from_slice(&(text.len() as u16).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

fn offset_bytes(offset: i64) -> Result<[u8; 8]> {
    if offset < 0 {
        return Err(INVALID);
    }
    Ok((offset as u64).to_le_bytes())
}

fn publish_frame(topic: &str, value: &[u8], key: &str, id: &str) -> Result<Vec<u8>> {
    if value.len() > MAX_VALUE {
        return Err(LIMIT);
    }
    let mut out = frame(
        PUBLISH,
        5 + topic.len() + key.len() + id.len() + value.len(),
    );
    str8(&mut out, topic)?;
    str16(&mut out, key, MAX_KEY)?;
    str16(&mut out, id, MAX_ID)?;
    out.extend_from_slice(value);
    finish_frame(out)
}

fn read_frame(topic: &str, partition: u16, offset: i64, wait: Duration) -> Result<Vec<u8>> {
    // Whole milliseconds, rounded up, so that a wait shorter than one is still a wait.
    let millis = wait.as_nanos().div_ceil(1_000_000);
    if millis > u128::from(MAX_WAIT) {
        return Err(INVALID);
    }
    let mut out = frame(READ, 13 + topic.len());
    str8(&mut out, topic)?;
    out.extend_from_slice(&partition.to_le_bytes());
    out.extend_from_slice(&offset_bytes(offset)?);
    out.extend_from_slice(&(millis as u16).to_le_bytes());
    finish_frame(out)
}

fn position_frame(topic: &str, partition: u16, group: &str) -> Result<Vec<u8>> {
    let mut out = frame(POSITION, 4 + topic.len() + group.len());
    str8(&mut out, topic)?;
    out.extend_from_slice(&partition.to_le_bytes());
    str8(&mut out, group)?;
    finish_frame(out)
}

fn commit_frame(
    topic: &str,
    partition: u16,
    group: &str,
    expected: i64,
    state: &[u8],
) -> Result<Vec<u8>> {
    if state.len() > MAX_VALUE {
        return Err(LIMIT);
    }
    let mut out = frame(COMMIT, 12 + topic.len() + group.len() + state.len());
    str8(&mut out, topic)?;
    out.extend_from_slice(&partition.to_le_bytes());
    str8(&mut out, group)?;
    out.extend_from_slice(&offset_bytes(expected)?);
    out.extend_from_slice(state);
    finish_frame(out)
}

fn ping_frame() -> Result<Vec<u8>> {
    finish_frame(frame(PING, 0))
}

// -- decoding

/// (window, window_bytes) from a WELCOME, within this client's ceilings.
fn welcome(data: &[u8; WELCOME_BYTES]) -> Result<(usize, usize)> {
    if &data[..4] != b"DSTV" {
        return Err(PROTOCOL);
    }
    let status = data[4];
    if status != 0 {
        // A refusal is a known status with every later field zero.
        return Err(match failure(status) {
            Some(error) if data[5..].iter().all(|&b| b == 0) => error,
            _ => PROTOCOL,
        });
    }
    let version = u16::from_le_bytes([data[5], data[6]]);
    let max_body = u32::from_le_bytes([data[7], data[8], data[9], data[10]]) as usize;
    let window = usize::from(u16::from_le_bytes([data[11], data[12]]));
    let window_bytes = u32::from_le_bytes([data[13], data[14], data[15], data[16]]) as usize;
    // Every legal frame must fit the byte window, or nothing more is sent.
    if version != 1 || max_body != MAX_BODY || window < 1 || window_bytes < MAX_BODY + HEADER {
        return Err(PROTOCOL);
    }
    Ok((
        window.min(MAX_IN_FLIGHT),
        window_bytes.min(MAX_IN_FLIGHT_BYTES),
    ))
}

struct Body<'a>(&'a [u8]);

impl<'a> Body<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.0.len() {
            return None;
        }
        let (out, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(out)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// An offset below 2^63.
    fn offset(&mut self) -> Option<i64> {
        i64::try_from(self.u64()?).ok()
    }

    fn string(&mut self, limit: usize) -> Option<String> {
        let n = usize::from(self.u16()?);
        if n > limit {
            return None;
        }
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }

    fn rest(&mut self, limit: usize) -> Option<Vec<u8>> {
        let n = self.0.len();
        if n > limit {
            return None;
        }
        Some(self.take(n)?.to_vec())
    }
}

#[derive(Debug)]
enum Reply {
    Published(Receipt),
    Read(Option<Record>),
    Position(Position),
    Done, // COMMIT and PING
    Failed(NativeError),
}

/// The body of a server frame, or None if it does not follow the protocol.
fn decode_reply(kind: u8, data: &[u8]) -> Option<Reply> {
    let mut body = Body(data);
    let out = match kind {
        k if k == REPLY | PUBLISH => Reply::Published(Receipt {
            partition: body.u16()?,
            offset: body.offset()?,
        }),
        k if k == REPLY | READ => match body.byte()? {
            0 => Reply::Read(None),
            1 => {
                let receipt = Receipt {
                    partition: body.u16()?,
                    offset: body.offset()?,
                };
                let time = rfc3339(body.u64()? as i64);
                Reply::Read(Some(Record {
                    receipt,
                    time,
                    key: body.string(MAX_KEY)?,
                    id: body.string(MAX_ID)?,
                    value: body.rest(MAX_VALUE)?,
                }))
            }
            _ => return None,
        },
        k if k == REPLY | POSITION => Reply::Position(Position {
            next: body.offset()?,
            state: body.rest(MAX_VALUE)?,
        }),
        k if k == REPLY | COMMIT || k == REPLY | PING => Reply::Done,
        ERROR => Reply::Failed(failure(body.byte()?)?),
        _ => return None,
    };
    body.0.is_empty().then_some(out)
}

/// UTC with nanoseconds and no trailing zeros: the text the HTTP API puts in Dostoevsky-Time.
fn rfc3339(nanos: i64) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let rest = nanos.rem_euclid(1_000_000_000);
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    // Civil date from days since 1970-01-01, counting years from 1 March.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let mut out = format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        day_seconds / 3600,
        day_seconds / 60 % 60,
        day_seconds % 60
    );
    if rest != 0 {
        let fraction = format!("{rest:09}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

// -- transport

enum Stream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
}

macro_rules! on_stream {
    ($self:expr, $s:ident => $call:expr) => {
        match $self {
            Stream::Tcp($s) => $call,
            #[cfg(unix)]
            Stream::Unix($s) => $call,
        }
    };
}

impl Stream {
    fn try_clone(&self) -> io::Result<Stream> {
        Ok(match self {
            Stream::Tcp(s) => Stream::Tcp(s.try_clone()?),
            #[cfg(unix)]
            Stream::Unix(s) => Stream::Unix(s.try_clone()?),
        })
    }
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        on_stream!(self, s => s.read(buf))
    }
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        on_stream!(self, s => s.write(buf))
    }
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        on_stream!(self, s => s.set_read_timeout(timeout))
    }
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        on_stream!(self, s => s.set_write_timeout(timeout))
    }
    fn shutdown(&self) {
        // Already closed by the peer is fine.
        let _ = on_stream!(self, s => s.shutdown(Shutdown::Both));
    }
}

/// Time left until the deadline; an error once it has passed.
fn left(deadline: Instant) -> io::Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(io::ErrorKind::TimedOut.into());
    }
    Ok(left)
}

/// Writes everything before the one absolute deadline, however slowly the peer takes it.
fn write_by(stream: &mut Stream, mut data: &[u8], deadline: Instant) -> io::Result<()> {
    while !data.is_empty() {
        stream.set_write_timeout(Some(left(deadline)?))?;
        match stream.write(data) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => data = &data[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn read_by(stream: &mut Stream, buf: &mut [u8], deadline: Instant) -> io::Result<()> {
    let mut got = 0;
    while got < buf.len() {
        stream.set_read_timeout(Some(left(deadline)?))?;
        match stream.read(&mut buf[got..]) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    left(deadline)?; // a WELCOME that arrives late is not accepted
    Ok(())
}

// -- shared state

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // No code holding these locks can leave them half-updated by panicking.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Where one request's answer is put.
#[derive(Default)]
struct Slot {
    answer: Mutex<Option<Result<Reply>>>,
    ready: Condvar,
}

impl Slot {
    fn put(&self, answer: Result<Reply>) {
        *lock(&self.answer) = Some(answer);
        self.ready.notify_all();
    }
}

struct Waiting {
    kind: u8,
    size: usize,
    sequence: u64,
    deadline: Instant,
    slot: Arc<Slot>,
}

#[derive(Default)]
struct State {
    dead: bool,
    pending: HashMap<u32, Waiting>,
    unanswered: usize, // bytes of the pending frames
    // Requests are numbered in the order they are written; one at a time is being written.
    reserved: u64,  // the latest number given out
    finishing: u64, // the latest request whose last write call has been made
    completed: u64, // the latest request written in full
    last_id: u32,
}

impl State {
    fn oldest(&self) -> Option<Instant> {
        self.pending.values().map(|w| w.deadline).min()
    }

    fn expired(&self) -> bool {
        self.oldest().is_some_and(|at| at <= Instant::now())
    }
}

struct Shared {
    state: Mutex<State>,
    room: Condvar, // signalled when a request is written or answered and when the connection dies
    control: Stream,
}

impl Shared {
    /// Ends the connection. Every unanswered request was handed to the socket, so its outcome
    /// is unknown.
    fn fail(&self) {
        let mut state = lock(&self.state);
        if state.dead {
            return;
        }
        state.dead = true;
        state.unanswered = 0;
        for (_, waiting) in state.pending.drain() {
            waiting.slot.put(Err(SENT));
        }
        drop(state);
        self.control.shutdown(); // wakes the reader and a blocked writer
        self.room.notify_all();
    }

    /// Writes one request's frame, noting when its last write call is made and when it returned.
    /// Until that call is made the server cannot have the whole request.
    fn send(
        &self,
        sequence: u64,
        data: &[u8],
        mut write: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut chunks = data.chunks(CHUNK).peekable();
        while let Some(chunk) = chunks.next() {
            if chunks.peek().is_none() {
                lock(&self.state).finishing = sequence;
            }
            write(chunk)?;
        }
        lock(&self.state).completed = sequence;
        self.room.notify_all();
        Ok(())
    }

    /// Delivers one server frame. False ends the connection.
    fn answer(&self, kind: u8, request_id: u32, body: &[u8], arrived: u64) -> bool {
        // Request id 0 is a connection error: the server closes after it.
        if request_id == 0 {
            return false;
        }
        let Some(reply) = decode_reply(kind, body) else {
            return false;
        };
        let mut state = lock(&self.state);
        // A reply only answers a request whose last write call had been made when the reply began
        // to arrive, with that request's reply type or ERROR, and before its deadline.
        let (sequence, deadline) = match state.pending.get(&request_id) {
            Some(w) if w.sequence <= arrived && (kind == ERROR || kind == REPLY | w.kind) => {
                (w.sequence, w.deadline)
            }
            _ => return false,
        };
        // A fast reply can be read before the writing thread has seen its last call return. It
        // is delivered only once that call has succeeded.
        while state.completed < sequence {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if state.dead || remaining.is_zero() {
                return false;
            }
            state = self
                .room
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        if Instant::now() >= deadline {
            return false;
        }
        let Some(waiting) = state.pending.remove(&request_id) else {
            return false;
        };
        state.unanswered -= waiting.size;
        waiting.slot.put(match reply {
            Reply::Failed(error) => Err(error),
            reply => Ok(reply),
        });
        drop(state);
        self.room.notify_all();
        true
    }

    fn receive(&self, mut stream: Stream) {
        let mut chunk = vec![0u8; CHUNK];
        let mut buf: Vec<u8> = Vec::new();
        let mut carried = 0; // `finishing` when the first byte of a partly received reply arrived
        loop {
            let read = stream.read(&mut chunk);
            // The connection owns every deadline: it ends even if nobody waits for the request.
            let (now, expired) = {
                let state = lock(&self.state);
                (state.finishing, state.expired())
            };
            if expired {
                break;
            }
            let n = match read {
                Ok(0) => break,
                Ok(n) => n,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue
                }
                Err(_) => break,
            };
            let mut arrived = if buf.is_empty() { now } else { carried };
            buf.extend_from_slice(&chunk[..n]);
            let mut at = 0;
            while buf.len() - at >= HEADER {
                let head = &buf[at..at + HEADER];
                let length = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
                let (kind, flags) = (head[4], head[5]);
                let request_id = u32::from_le_bytes([head[6], head[7], head[8], head[9]]);
                // The length is checked from the header, before the body is waited for.
                if length > MAX_BODY || flags != 0 {
                    return self.fail();
                }
                if buf.len() - at < HEADER + length {
                    break;
                }
                let body = &buf[at + HEADER..at + HEADER + length];
                if !self.answer(kind, request_id, body, arrived) {
                    return self.fail();
                }
                at += HEADER + length;
                arrived = now;
            }
            buf.drain(..at);
            carried = arrived;
            if buf.is_empty() && buf.capacity() > KEEP_BUFFER {
                buf = Vec::new();
            }
        }
        self.fail();
    }
}

/// A request handed to the connection and not yet collected.
struct Pending {
    shared: Arc<Shared>,
    slot: Arc<Slot>,
    deadline: Instant,
}

impl Pending {
    fn wait(self) -> Result<Reply> {
        let mut answer = lock(&self.slot.answer);
        loop {
            if let Some(answer) = answer.take() {
                return answer;
            }
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(answer);
                self.shared.fail(); // fills this slot unless the reply was just put there
                answer = lock(&self.slot.answer);
                return answer.take().unwrap_or(Err(SENT));
            }
            answer = self
                .slot
                .ready
                .wait_timeout(answer, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }
}

/// A publish handed to the connection; [`PendingPublish::wait`] waits for its receipt.
///
/// A reply is accepted only within 40 s of the request.
pub struct PendingPublish(Pending);

impl PendingPublish {
    pub fn wait(self) -> Result<Receipt> {
        match self.0.wait()? {
            Reply::Published(receipt) => Ok(receipt),
            _ => Err(PROTOCOL),
        }
    }
}

// -- the client

/// One connection to a Dostoevsky server. It may be shared between threads: requests of all of
/// them are in flight together, and a thread waiting in `read` does not hold up the others. One
/// further thread reads the replies.
pub struct NativeClient {
    shared: Arc<Shared>,
    writer: Mutex<Stream>, // held to send, so frames are whole and leave in the order taken
    window: usize,
    window_bytes: usize,
    request_time: Duration,
    reader: Option<JoinHandle<()>>,
}

impl NativeClient {
    /// Connects over TCP to `host:port`. Only loopback addresses are tried: the token travels in
    /// clear text.
    pub fn connect(address: &str, token: &str) -> Result<Self> {
        let first = hello(token)?;
        let deadline = Instant::now() + HANDSHAKE_TIME; // name resolution itself is not bounded
        let targets: Vec<SocketAddr> = address.to_socket_addrs().map_err(|_| NOT_SENT)?.collect();
        if targets.iter().any(|a| !a.ip().is_loopback()) {
            return Err(NativeError::Refused(
                "refusing to send the token: this client speaks plaintext to loopback only".into(),
            ));
        }
        for target in targets {
            let Ok(timeout) = left(deadline) else { break };
            if let Ok(stream) = TcpStream::connect_timeout(&target, timeout) {
                stream.set_nodelay(true).map_err(|_| NOT_SENT)?;
                return Self::start(Stream::Tcp(stream), &first, deadline);
            }
        }
        Err(NOT_SENT)
    }

    /// Connects to a unix socket. The 5 s handshake limit does not cover the connect itself (the
    /// standard library cannot bound it): it can block while the server's accept queue is full.
    #[cfg(unix)]
    pub fn connect_unix(path: impl AsRef<std::path::Path>, token: &str) -> Result<Self> {
        let first = hello(token)?;
        let deadline = Instant::now() + HANDSHAKE_TIME;
        let stream = std::os::unix::net::UnixStream::connect(path).map_err(|_| NOT_SENT)?;
        Self::start(Stream::Unix(stream), &first, deadline)
    }

    fn start(mut stream: Stream, first: &[u8], deadline: Instant) -> Result<Self> {
        let mut data = [0u8; WELCOME_BYTES];
        write_by(&mut stream, first, deadline).map_err(|_| NOT_SENT)?;
        read_by(&mut stream, &mut data, deadline).map_err(|_| NOT_SENT)?;
        let (window, window_bytes) = welcome(&data)?;
        stream.set_read_timeout(Some(TICK)).map_err(|_| NOT_SENT)?;
        let reading = stream.try_clone().map_err(|_| NOT_SENT)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            room: Condvar::new(),
            control: stream.try_clone().map_err(|_| NOT_SENT)?,
        });
        let theirs = Arc::clone(&shared);
        let reader = std::thread::Builder::new()
            .name("dostoevsky-native".into())
            .spawn(move || theirs.receive(reading))
            .map_err(|_| NOT_SENT)?;
        Ok(Self {
            shared,
            writer: Mutex::new(stream),
            window,
            window_bytes,
            request_time: REQUEST_TIME,
            reader: Some(reader),
        })
    }

    /// Sends one frame, first waiting for room in the server's window.
    fn submit(&self, mut data: Vec<u8>) -> Result<Pending> {
        let (kind, size) = (data[4], data.len());
        let mut writer = lock(&self.writer);
        let mut state = lock(&self.shared.state);
        loop {
            if state.dead {
                return Err(NOT_SENT);
            }
            if state.expired() {
                drop(state);
                self.shared.fail();
                return Err(NOT_SENT);
            }
            if state.pending.len() < self.window && state.unanswered + size <= self.window_bytes {
                break;
            }
            let wait = state.oldest().map_or(self.request_time, |at| {
                at.saturating_duration_since(Instant::now())
            });
            state = self
                .shared
                .room
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        let mut request_id = state.last_id;
        loop {
            request_id = request_id.checked_add(1).unwrap_or(1);
            if !state.pending.contains_key(&request_id) {
                break;
            }
        }
        state.last_id = request_id;
        data[6..HEADER].copy_from_slice(&request_id.to_le_bytes());
        // Reserved before the first byte is written: from here the outcome of a lost connection is
        // unknown. A reply is accepted only once `send` has reached the last write call.
        let deadline = Instant::now() + self.request_time;
        let slot = Arc::new(Slot::default());
        state.reserved += 1;
        let sequence = state.reserved;
        state.unanswered += size;
        state.pending.insert(
            request_id,
            Waiting {
                kind,
                size,
                sequence,
                deadline,
                slot: Arc::clone(&slot),
            },
        );
        drop(state);
        let written = self.shared.send(sequence, &data, |chunk| {
            write_by(&mut writer, chunk, deadline)
        });
        if written.is_err() {
            self.shared.fail();
        }
        Ok(Pending {
            shared: Arc::clone(&self.shared),
            slot,
            deadline,
        })
    }

    fn call(&self, data: Vec<u8>) -> Result<Reply> {
        self.submit(data)?.wait()
    }

    /// Hands a publish to the connection without waiting for its receipt (it waits only for room
    /// in the server's window). Publishes of one thread are appended in call order.
    pub fn publish_nowait(
        &self,
        topic: &str,
        value: &[u8],
        key: &str,
        id: &str,
    ) -> Result<PendingPublish> {
        self.submit(publish_frame(topic, value, key, id)?)
            .map(PendingPublish)
    }

    /// Appends one record. An empty `key` or `id` means none.
    pub fn publish(&self, topic: &str, value: &[u8], key: &str, id: &str) -> Result<Receipt> {
        self.publish_nowait(topic, value, key, id)?.wait()
    }

    /// One record, or None if the end was reached, after waiting at most `wait` (30 s or less).
    pub fn read(
        &self,
        topic: &str,
        partition: u16,
        offset: i64,
        wait: Duration,
    ) -> Result<Option<Record>> {
        match self.call(read_frame(topic, partition, offset, wait)?)? {
            Reply::Read(record) => Ok(record),
            _ => Err(PROTOCOL),
        }
    }

    pub fn position(&self, topic: &str, partition: u16, group: &str) -> Result<Position> {
        match self.call(position_frame(topic, partition, group)?)? {
            Reply::Position(position) => Ok(position),
            _ => Err(PROTOCOL),
        }
    }

    /// Moves the group's cursor from `expected` to `expected + 1` and stores `state` with it.
    pub fn commit(
        &self,
        topic: &str,
        partition: u16,
        group: &str,
        expected: i64,
        state: &[u8],
    ) -> Result<()> {
        self.call(commit_frame(topic, partition, group, expected, state)?)?;
        Ok(())
    }

    pub fn ping(&self) -> Result<()> {
        self.call(ping_frame()?)?;
        Ok(())
    }

    /// Ends the connection. Unanswered requests fail with an unknown outcome.
    pub fn close(&self) {
        self.shared.fail();
    }
}

impl Drop for NativeClient {
    fn drop(&mut self) {
        self.shared.fail();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    const TOKEN: &str = "example-token-not-a-secret";

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    fn with_id(mut data: Vec<u8>, id: u32) -> Vec<u8> {
        data[6..HEADER].copy_from_slice(&id.to_le_bytes());
        data
    }

    // The byte strings below are from wire/testdata/vectors.json.
    #[test]
    fn encodes_the_vectors() {
        assert_eq!(
            hello(TOKEN).unwrap(),
            hex("44535456010000001a006578616d706c652d746f6b656e2d6e6f742d612d736563726574")
        );
        assert_eq!(
            with_id(publish_frame("events", b"hi", "", "").unwrap(), 1),
            hex("0d000000010001000000066576656e7473000000006869")
        );
        assert_eq!(
            with_id(
                publish_frame("jobs", b"", "k\u{e9}", "once").unwrap(),
                0x01020304
            ),
            hex("10000000010004030201046a6f627303006bc3a904006f6e6365")
        );
        let second = Duration::from_secs(1);
        assert_eq!(
            with_id(read_frame("events", 0, 41, second).unwrap(), 2),
            hex("13000000020002000000066576656e747300002900000000000000e803")
        );
        assert_eq!(
            with_id(position_frame("events", 0, "workers").unwrap(), 4),
            hex("11000000030004000000066576656e7473000007776f726b657273")
        );
        assert_eq!(
            with_id(
                commit_frame("events", 0, "workers", 42, b"{\"n\":2}").unwrap(),
                5
            ),
            hex("20000000040005000000066576656e7473000007776f726b6572732a000000000000007b226e223a327d")
        );
        assert_eq!(
            with_id(ping_frame().unwrap(), 6),
            hex("00000000050006000000")
        );
    }

    #[test]
    fn refuses_values_it_will_not_send() {
        let long = "x".repeat(MAX_KEY + 1);
        assert_eq!(publish_frame("t", b"", &long, "").unwrap_err(), LIMIT);
        assert_eq!(
            publish_frame("t", b"", "", &long[..257]).unwrap_err(),
            LIMIT
        );
        assert_eq!(
            publish_frame("t", &vec![0; MAX_VALUE + 1], "", "").unwrap_err(),
            LIMIT
        );
        assert_eq!(read_frame("t", 0, -1, Duration::ZERO).unwrap_err(), INVALID);
        assert_eq!(
            read_frame("t", 0, 0, Duration::from_millis(30_001)).unwrap_err(),
            INVALID
        );
        // Less than a millisecond is still a wait.
        let short = read_frame("t", 0, 0, Duration::from_nanos(1)).unwrap();
        assert_eq!(&short[short.len() - 2..], &[1, 0]);
        assert_eq!(hello(&"t".repeat(MAX_TOKEN + 1)).unwrap_err(), LIMIT);
    }

    #[test]
    fn decodes_the_vectors() {
        let body = |text: &str| hex(text)[HEADER..].to_vec();
        match decode_reply(0x81, &body("0a00000081000100000003000100000000002000")) {
            Some(Reply::Published(r)) => {
                assert_eq!((r.partition, r.offset), (3, 9_007_199_254_740_993));
            }
            other => panic!("{other:?}"),
        }
        let found =
            body("1c0000008200020000000100002900000000000000158dc824e15e7b1801006b000000ff0a0d");
        match decode_reply(0x82, &found) {
            Some(Reply::Read(Some(r))) => {
                assert_eq!((r.receipt.partition, r.receipt.offset), (0, 41));
                assert_eq!((r.key.as_str(), r.id.as_str()), ("k", ""));
                assert_eq!(r.value, [0, 0xff, 0x0a, 0x0d]);
                assert_eq!(r.time, "2025-11-25T22:00:00.123456789Z");
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(decode_reply(0x82, &[0]), Some(Reply::Read(None))));
        assert!(matches!(
            decode_reply(ERROR, &[3]),
            Some(Reply::Failed(NativeError::Api(409, "conflict")))
        ));
        assert!(decode_reply(0x82, &[2]).is_none());
        assert!(decode_reply(0x81, &[0, 0]).is_none());
        assert!(decode_reply(0x85, &[0]).is_none());
        assert!(decode_reply(ERROR, &[13]).is_none());
        assert!(decode_reply(0x42, &[]).is_none());
    }

    #[test]
    fn formats_times() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(-1), "1969-12-31T23:59:59.999999999Z");
        assert_eq!(rfc3339(951_827_696_500_000_000), "2000-02-29T12:34:56.5Z");
    }

    const WELCOME: &str = "4453545600010000201000000100004000";

    #[test]
    fn checks_welcome() {
        let read = |text: &str| welcome(hex(text).as_slice().try_into().unwrap());
        assert_eq!(read(WELCOME), Ok((256, 4 << 20)));
        assert_eq!(
            read("4453545602000000000000000000000000"),
            Err(NativeError::Api(401, "unauthorized"))
        );
        // A refusal with a later field set, an unknown status, no window, a wrong version.
        assert_eq!(read("4453545602010000000000000000000000"), Err(PROTOCOL));
        assert_eq!(read("445354560d000000000000000000000000"), Err(PROTOCOL));
        assert_eq!(read("4453545600010000201000000000004000"), Err(PROTOCOL));
        assert_eq!(read("4453545600020000201000000100004000"), Err(PROTOCOL));
    }

    /// A server that answers HELLO with `welcome` and then does what `then` says.
    fn scripted(
        welcome: &'static str,
        then: impl FnOnce(&mut TcpStream) + Send + 'static,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut first = vec![0; 10 + TOKEN.len()];
            conn.read_exact(&mut first).unwrap();
            conn.write_all(&hex(welcome)).unwrap();
            then(&mut conn);
        });
        (address, server)
    }

    fn next_frame(conn: &mut TcpStream) -> (u8, u32) {
        let mut head = [0; HEADER];
        conn.read_exact(&mut head).unwrap();
        let length = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
        conn.read_exact(&mut vec![0; length]).unwrap();
        (head[4], u32::from_le_bytes(head[6..].try_into().unwrap()))
    }

    fn published(id: u32, offset: u64) -> Vec<u8> {
        let mut out = frame(REPLY | PUBLISH, 10);
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&offset.to_le_bytes());
        with_id(finish_frame(out).unwrap(), id)
    }

    fn drain(conn: &mut TcpStream) {
        let mut sink = [0; 1024];
        while matches!(conn.read(&mut sink), Ok(n) if n > 0) {}
    }

    #[test]
    fn matches_replies_in_any_order_and_split_anywhere() {
        let (address, server) = scripted(WELCOME, |conn| {
            let (_, a) = next_frame(conn);
            let (_, b) = next_frame(conn);
            let mut both = published(b, 1);
            both.extend(published(a, 0));
            for byte in both {
                conn.write_all(&[byte]).unwrap();
            }
            drain(conn);
        });
        let client = NativeClient::connect(&address, TOKEN).unwrap();
        let a = client.publish_nowait("t", b"a", "", "").unwrap();
        let b = client.publish_nowait("t", b"b", "", "").unwrap();
        assert_eq!(a.wait().unwrap().offset, 0);
        assert_eq!(b.wait().unwrap().offset, 1);
        drop(client);
        server.join().unwrap();
    }

    #[test]
    fn a_reply_that_answers_nothing_ends_the_connection() {
        for wrong in ["id", "type"] {
            let (address, server) = scripted(WELCOME, move |conn| {
                let (_, id) = next_frame(conn);
                let reply = if wrong == "id" {
                    published(id + 1, 0)
                } else {
                    with_id(ping_frame().unwrap(), id) // a request type, not a reply
                };
                conn.write_all(&reply).unwrap();
                drain(conn);
            });
            let client = NativeClient::connect(&address, TOKEN).unwrap();
            assert_eq!(client.publish("t", b"a", "", "").unwrap_err(), SENT);
            assert_eq!(client.ping().unwrap_err(), NOT_SENT);
            server.join().unwrap();
        }
    }

    #[test]
    fn a_lost_connection_leaves_the_outcome_unknown() {
        let (address, server) = scripted(WELCOME, |conn| {
            next_frame(conn);
        });
        let client = NativeClient::connect(&address, TOKEN).unwrap();
        assert_eq!(client.publish("t", b"a", "", "").unwrap_err(), SENT);
        assert_eq!(client.publish("t", b"a", "", "").unwrap_err(), NOT_SENT);
        server.join().unwrap();
    }

    #[test]
    fn no_more_than_the_window_is_unanswered() {
        // window 1
        let (address, server) = scripted("4453545600010000201000010000004000", |conn| {
            let (_, first) = next_frame(conn);
            conn.set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            assert!(conn.read(&mut [0; 1]).is_err(), "a second request was sent");
            conn.set_read_timeout(None).unwrap();
            conn.write_all(&published(first, 0)).unwrap();
            let (_, second) = next_frame(conn);
            conn.write_all(&published(second, 1)).unwrap();
            drain(conn);
        });
        let client = NativeClient::connect(&address, TOKEN).unwrap();
        let a = client.publish_nowait("t", b"a", "", "").unwrap();
        let b = client.publish_nowait("t", b"b", "", "").unwrap();
        assert_eq!(a.wait().unwrap().offset, 0);
        assert_eq!(b.wait().unwrap().offset, 1);
        drop(client);
        server.join().unwrap();
    }

    /// A client of a server that reads but never answers.
    fn silent() -> (NativeClient, JoinHandle<()>) {
        let (address, server) = scripted(WELCOME, |conn| {
            conn.set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut sink = [0; 1024];
            loop {
                match conn.read(&mut sink) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) => panic!("the client did not end the connection: {e}"),
                }
            }
        });
        let mut client = NativeClient::connect(&address, TOKEN).unwrap();
        client.request_time = Duration::from_millis(200);
        (client, server)
    }

    /// A client with no reader thread, so that nothing but the code under test ends it. The
    /// listener is returned to keep the other end open.
    fn unread() -> (NativeClient, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let client = NativeClient {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                room: Condvar::new(),
                control: Stream::Tcp(stream.try_clone().unwrap()),
            }),
            writer: Mutex::new(Stream::Tcp(stream)),
            window: MAX_IN_FLIGHT,
            window_bytes: MAX_IN_FLIGHT_BYTES,
            request_time: REQUEST_TIME,
            reader: None,
        };
        (client, listener)
    }

    fn reserve(shared: &Shared, id: u32, sequence: u64, deadline: Instant) -> Arc<Slot> {
        let slot = Arc::new(Slot::default());
        let mut state = lock(&shared.state);
        state.reserved = sequence;
        state.pending.insert(
            id,
            Waiting {
                kind: PING,
                size: 0,
                sequence,
                deadline,
                slot: Arc::clone(&slot),
            },
        );
        slot
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn a_request_counts_as_written_only_at_its_last_write() {
        let (client, _listener) = unread();
        let shared = &client.shared;
        let mut seen = Vec::new();
        let data = vec![0; 2 * CHUNK + 1];
        shared
            .send(1, &data, |chunk| {
                let state = lock(&shared.state);
                seen.push((chunk.len(), state.finishing, state.completed));
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, [(CHUNK, 0, 0), (CHUNK, 0, 0), (1, 1, 0)]);
        assert_eq!(lock(&shared.state).completed, 1);
        // A failed write never counts as completed.
        assert!(shared
            .send(2, &data, |_| Err(io::ErrorKind::BrokenPipe.into()))
            .is_err());
        assert_eq!(lock(&shared.state).completed, 1);
    }

    #[test]
    fn a_reply_waits_for_the_write_and_never_precedes_it() {
        let (client, _listener) = unread();
        let shared = &client.shared;
        // It began to arrive before the request's last write call: refused.
        reserve(shared, 7, 1, soon());
        assert!(!shared.answer(REPLY | PING, 7, &[], 0));
        // It began to arrive during that call: delivered once the call has returned.
        let slot = reserve(shared, 8, 2, soon());
        lock(&shared.state).finishing = 2;
        std::thread::scope(|scope| {
            let reply = scope.spawn(|| shared.answer(REPLY | PING, 8, &[], 2));
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                lock(&slot.answer).is_none(),
                "delivered before the write returned"
            );
            lock(&shared.state).completed = 2;
            shared.room.notify_all();
            assert!(reply.join().unwrap());
        });
        assert!(matches!(*lock(&slot.answer), Some(Ok(Reply::Done))));
    }

    #[test]
    fn a_late_reply_is_refused() {
        let (client, _listener) = unread();
        let shared = &client.shared;
        let slot = reserve(shared, 7, 1, Instant::now());
        {
            let mut state = lock(&shared.state);
            state.finishing = 1;
            state.completed = 1;
        }
        assert!(!shared.answer(REPLY | PING, 7, &[], 1));
        assert!(lock(&slot.answer).is_none());
    }

    #[test]
    fn nothing_more_is_sent_after_an_expired_request() {
        let (client, _listener) = unread();
        let slot = reserve(&client.shared, 7, 1, Instant::now());
        assert_eq!(client.ping().unwrap_err(), NOT_SENT);
        assert!(matches!(&*lock(&slot.answer), Some(Err(e)) if *e == SENT));
    }

    #[test]
    fn a_request_nobody_waits_for_still_expires() {
        let (client, server) = silent();
        drop(client.publish_nowait("t", b"a", "", "").unwrap());
        server.join().unwrap(); // the client, still open, has ended the connection
        assert_eq!(client.ping().unwrap_err(), NOT_SENT);
    }

    #[test]
    fn waiting_past_the_deadline_leaves_the_outcome_unknown() {
        let (client, server) = silent();
        assert_eq!(client.publish("t", b"a", "", "").unwrap_err(), SENT);
        assert_eq!(client.ping().unwrap_err(), NOT_SENT);
        server.join().unwrap();
    }

    #[test]
    fn reports_a_refusal_and_refuses_other_hosts() {
        let (address, server) = scripted("4453545602000000000000000000000000", drain);
        assert_eq!(
            NativeClient::connect(&address, TOKEN).err(),
            Some(NativeError::Api(401, "unauthorized"))
        );
        server.join().unwrap();
        assert!(matches!(
            NativeClient::connect("192.0.2.1:8788", TOKEN),
            Err(NativeError::Refused(_))
        ));
    }
}
