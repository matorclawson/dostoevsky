//! Synchronous client for Dostoevsky's HTTP API. Standard library only: no crates are
//! fetched. It speaks plain `http://` to loopback (HTTP/1.1 keep-alive over a small pool
//! of connections, so a `Client` can be shared between threads). For a TLS gateway, run a
//! local TLS-terminating proxy and point this client at it.
//! All payloads and checkpoint states are bytes.
use std::{
    fmt,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::Mutex,
    time::{Duration, Instant},
};

const MAX_BODY: usize = 1_048_576;
const MAX_LINE: u64 = 8192;
const MAX_HEADERS: usize = 100;
const MAX_IDLE: usize = 8; // idle connections kept per Client
const IDLE_LIMIT: Duration = Duration::from_secs(30); // the gateway closes idle connections at 60 s

type Conn = BufReader<TcpStream>;

#[derive(Debug)]
pub enum Error {
    Api(u16, String),
    Transport(String),
    InvalidResponse(String),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(status, code) => write!(f, "{status} {code}"),
            Self::Transport(message) | Self::InvalidResponse(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for Error {}

#[derive(Debug)]
pub struct Receipt {
    pub partition: u16,
    pub offset: i64,
}
#[derive(Debug)]
pub struct Record {
    pub receipt: Receipt,
    pub key: String,
    pub id: String,
    pub time: String,
    pub value: Vec<u8>,
}
#[derive(Debug)]
pub struct Position {
    pub next: i64,
    pub state: Vec<u8>,
}

pub struct Client {
    authority: String, // host:port
    prefix: String,    // path prefix, ending in the API version
    auth: String,
    timeout: Duration,
    idle: Mutex<Vec<(Conn, Instant)>>, // most recently used last
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>, // names lower-cased
    body: Vec<u8>,
}
impl Response {
    fn header(&self, name: &str) -> Result<&str, Error> {
        let want = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == want)
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| Error::InvalidResponse(format!("missing {name}")))
    }
}

/// The bearer token grants the whole store, and this client has no TLS: it may only talk to loopback.
fn is_loopback(authority: &str) -> bool {
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        authority.rsplit_once(':').map_or(authority, |(h, _)| h)
    };
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The address actually connected to must be loopback, whatever the host name said: the token
/// travels in clear text, so a name that resolves elsewhere must not receive it.
fn check_resolved(addr: &std::net::SocketAddr) -> Result<(), Error> {
    if addr.ip().is_loopback() {
        Ok(())
    } else {
        Err(Error::Transport(format!(
            "refusing to send the token: the address resolved to {} (not loopback); this client speaks plain http to loopback only",
            addr.ip()
        )))
    }
}

fn transport(e: std::io::Error) -> Error {
    Error::Transport(e.to_string())
}

/// Percent-encodes everything except RFC 3986 unreserved characters.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Decodes %XX sequences; a plus sign stays a plus sign (the server escapes path-style).
fn decode(s: &str) -> Result<String, Error> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| Error::InvalidResponse("bad percent escape".into()))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| Error::InvalidResponse(e.to_string()))
}

fn read_line<R: BufRead>(r: &mut R) -> Result<String, Error> {
    let mut buf = Vec::new();
    (&mut *r).take(MAX_LINE).read_until(b'\n', &mut buf).map_err(transport)?;
    if buf.last() != Some(&b'\n') {
        return Err(Error::InvalidResponse("truncated or oversized line".into()));
    }
    buf.pop();
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    String::from_utf8(buf).map_err(|_| Error::InvalidResponse("non-UTF-8 header".into()))
}

fn read_exact_limited<R: Read>(r: &mut R, n: usize) -> Result<Vec<u8>, Error> {
    let mut v = Vec::with_capacity(n.min(65536));
    let got = r.take(n as u64).read_to_end(&mut v).map_err(transport)?;
    if got != n {
        return Err(Error::InvalidResponse("body shorter than announced".into()));
    }
    Ok(v)
}

/// Returns the response and whether the connection may carry another request: the body was
/// delimited by its own framing, the server speaks HTTP/1.1 and did not say `Connection: close`.
fn read_response<R: BufRead>(r: &mut R) -> Result<(Response, bool), Error> {
    let mut interim = 0usize;
    // Interim replies (100 Continue, 103 Early Hints, ...) come before the final reply on the same
    // connection. Read past them: returning one as the answer would leave the real reply in the
    // stream for the next call on a pooled connection to pick up.
    let (status, headers, http11) = loop {
        let status_line = read_line(r)?;
        let mut parts = status_line.splitn(3, ' ');
        let version = parts.next().unwrap_or("");
        if !version.starts_with("HTTP/1.") {
            return Err(Error::InvalidResponse("not an HTTP/1.x response".into()));
        }
        let http11 = version == "HTTP/1.1";
        let status: u16 = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Error::InvalidResponse("bad status line".into()))?;
        let mut headers = Vec::new();
        loop {
            let line = read_line(r)?;
            if line.is_empty() {
                break;
            }
            if headers.len() >= MAX_HEADERS {
                return Err(Error::InvalidResponse("too many headers".into()));
            }
            let (k, v) = line
                .split_once(':')
                .ok_or_else(|| Error::InvalidResponse("bad header line".into()))?;
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
        if status == 101 {
            return Err(Error::InvalidResponse("unexpected protocol switch (101)".into()));
        }
        if (100..200).contains(&status) {
            interim += 1;
            if interim > MAX_HEADERS {
                return Err(Error::InvalidResponse("too many interim replies".into()));
            }
            continue;
        }
        break (status, headers, http11);
    };
    let find = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    let mut framed = true; // false when the body ran to the end of the stream
    let body = if status == 204 || status == 304 {
        Vec::new()
    } else if find("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        let mut body = Vec::new();
        loop {
            let size_line = read_line(r)?;
            let size_hex = size_line.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(size_hex, 16)
                .map_err(|_| Error::InvalidResponse("bad chunk size".into()))?;
            if size == 0 {
                // Trailers, if any, up to the blank line, and no more of them than headers.
                let mut trailers = 0;
                while !read_line(r)?.is_empty() {
                    trailers += 1;
                    if trailers > MAX_HEADERS {
                        return Err(Error::InvalidResponse("too many trailer lines".into()));
                    }
                }
                break;
            }
            if body.len().saturating_add(size) > MAX_BODY {
                return Err(Error::InvalidResponse("oversized response".into()));
            }
            body.extend(read_exact_limited(r, size)?);
            if !read_line(r)?.is_empty() {
                return Err(Error::InvalidResponse("missing chunk terminator".into()));
            }
        }
        body
    } else if let Some(n) = find("content-length") {
        let n: usize = n
            .parse()
            .map_err(|_| Error::InvalidResponse("bad content-length".into()))?;
        if n > MAX_BODY {
            return Err(Error::InvalidResponse("oversized response".into()));
        }
        read_exact_limited(r, n)?
    } else {
        framed = false;
        let mut v = Vec::new();
        r.take(MAX_BODY as u64 + 1).read_to_end(&mut v).map_err(transport)?;
        if v.len() > MAX_BODY {
            return Err(Error::InvalidResponse("oversized response".into()));
        }
        v
    };
    let closes = find("connection").is_some_and(|v| v.to_ascii_lowercase().contains("close"));
    Ok((Response { status, headers, body }, framed && http11 && !closes))
}

fn receipt(r: &Response) -> Result<Receipt, Error> {
    Ok(Receipt {
        partition: r
            .header("Dostoevsky-Partition")?
            .parse()
            .map_err(|_| Error::InvalidResponse("invalid partition".into()))?,
        offset: r
            .header("Dostoevsky-Offset")?
            .parse()
            .map_err(|_| Error::InvalidResponse("invalid offset".into()))?,
    })
}

impl Client {
    /// `base` is `http://host:port` with an optional path prefix, and the host must be
    /// loopback. Construction never fails: any other URL is reported as an
    /// `Error::Transport` by every call, before anything is sent.
    pub fn new(base: &str, token: &str) -> Self {
        let trimmed = base.trim_end_matches('/');
        let (scheme_ok, rest) = match trimmed.strip_prefix("http://") {
            Some(r) => (true, r),
            None => (false, trimmed),
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        Self {
            // An unsupported scheme is kept as an empty authority and reported by call().
            authority: if scheme_ok && is_loopback(authority) { authority.to_string() } else { String::new() },
            prefix: format!("{path}/dostoevsky/client/v1/"),
            auth: format!("Bearer {token}"),
            timeout: Duration::from_secs(40),
            idle: Mutex::new(Vec::new()),
        }
    }

    /// Drops every pooled connection. Connections are also dropped when the Client is.
    pub fn close(&self) {
        self.idle.lock().unwrap().clear();
    }

    fn take(&self) -> Option<Conn> {
        let mut idle = self.idle.lock().unwrap();
        while let Some((conn, since)) = idle.pop() {
            if since.elapsed() <= IDLE_LIMIT {
                return Some(conn);
            }
        }
        None
    }

    fn give(&self, conn: Conn) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < MAX_IDLE {
            idle.push((conn, Instant::now()));
        }
    }

    fn connect(&self) -> Result<Conn, Error> {
        let addr = self
            .authority
            .to_socket_addrs()
            .map_err(transport)?
            .next()
            .ok_or_else(|| Error::Transport("address did not resolve".into()))?;
        check_resolved(&addr)?;
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(10)).map_err(transport)?;
        stream.set_read_timeout(Some(self.timeout)).map_err(transport)?;
        stream.set_write_timeout(Some(self.timeout)).map_err(transport)?;
        // The head and the body are written separately: without this, the body can wait for the
        // server's delayed acknowledgment of the head.
        stream.set_nodelay(true).map_err(transport)?;
        Ok(BufReader::new(stream))
    }

    /// One request and its reply on `conn`. The flag in the error says that nothing of a reply
    /// was seen (the connection was dead or went away): the only case a repeat can be considered.
    fn exchange(conn: &mut Conn, head: &str, data: Option<&[u8]>) -> Result<(Response, bool), (Error, bool)> {
        let dead = |e: std::io::Error| {
            let timed_out = matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock);
            (transport(e), !timed_out)
        };
        let stream = conn.get_mut();
        stream.write_all(head.as_bytes()).map_err(dead)?;
        if let Some(bytes) = data {
            stream.write_all(bytes).map_err(dead)?;
        }
        stream.flush().map_err(dead)?;
        match conn.fill_buf() {
            Ok([]) => return Err((Error::Transport("connection closed before a reply".into()), true)),
            Err(e) => return Err(dead(e)),
            Ok(_) => {}
        }
        read_response(conn).map_err(|e| (e, false))
    }

    fn finish(&self, conn: Conn, res: Response, reusable: bool) -> Result<Response, Error> {
        if reusable {
            self.give(conn);
        }
        if (200..300).contains(&res.status) {
            Ok(res)
        } else {
            let end = res.body.len().min(256);
            let code = String::from_utf8_lossy(&res.body[..end]).trim().to_string();
            Err(Error::Api(res.status, code))
        }
    }

    /// One call. A request that fails on a REUSED connection before any reply is repeated once
    /// on a fresh connection, but only when `retry_safe`: reads, positions and publishes that
    /// carry an id. Anything else returns the error rather than risk a duplicate.
    fn call(&self, op: &str, args: &[(&str, &str)], data: Option<&[u8]>, retry_safe: bool) -> Result<Response, Error> {
        if self.authority.is_empty() {
            return Err(Error::Transport(
                "refusing to send the token: this client speaks plain http to loopback only; \
                 TLS needs a local TLS-terminating proxy (or the future tls feature)"
                    .into(),
            ));
        }
        let query: Vec<String> = args.iter().map(|(k, v)| format!("{k}={}", encode(v))).collect();
        let method = if data.is_some() { "POST" } else { "GET" };
        let mut head = format!(
            "{method} {}{op}?{} HTTP/1.1\r\nHost: {}\r\nAuthorization: {}\r\nContent-Type: application/octet-stream\r\n",
            self.prefix,
            query.join("&"),
            self.authority,
            self.auth
        );
        if let Some(bytes) = data {
            head.push_str(&format!("Content-Length: {}\r\n", bytes.len()));
        }
        head.push_str("\r\n");

        if let Some(mut conn) = self.take() {
            match Self::exchange(&mut conn, &head, data) {
                Ok((res, reusable)) => return self.finish(conn, res, reusable),
                Err((e, before_reply)) => {
                    if !(before_reply && retry_safe) {
                        return Err(e);
                    }
                }
            }
        }
        let mut conn = self.connect()?;
        let (res, reusable) = Self::exchange(&mut conn, &head, data).map_err(|(e, _)| e)?;
        self.finish(conn, res, reusable)
    }

    pub fn publish(&self, topic: &str, value: &[u8], key: &str, id: &str) -> Result<Receipt, Error> {
        receipt(&self.call("publish", &[("topic", topic), ("key", key), ("id", id)], Some(value), !id.is_empty())?)
    }
    pub fn read(&self, topic: &str, partition: u16, offset: i64, wait_ms: u32) -> Result<Option<Record>, Error> {
        let res = self.call(
            "read",
            &[
                ("topic", topic),
                ("partition", &partition.to_string()),
                ("offset", &offset.to_string()),
                ("wait_ms", &wait_ms.to_string()),
            ],
            None,
            true,
        )?;
        if res.status == 204 {
            return Ok(None);
        }
        Ok(Some(Record {
            receipt: receipt(&res)?,
            key: decode(res.header("Dostoevsky-Key")?)?,
            id: decode(res.header("Dostoevsky-ID")?)?,
            time: res.header("Dostoevsky-Time")?.to_string(),
            value: res.body,
        }))
    }
    pub fn position(&self, topic: &str, partition: u16, group: &str) -> Result<Position, Error> {
        let res = self.call(
            "position",
            &[("topic", topic), ("partition", &partition.to_string()), ("group", group)],
            None,
            true,
        )?;
        let next = res
            .header("Dostoevsky-Next")?
            .parse()
            .map_err(|_| Error::InvalidResponse("invalid cursor".into()))?;
        Ok(Position { next, state: res.body })
    }
    pub fn commit(&self, topic: &str, partition: u16, group: &str, expected: i64, state: &[u8]) -> Result<(), Error> {
        self.call(
            "commit",
            &[
                ("topic", topic),
                ("partition", &partition.to_string()),
                ("group", group),
                ("expected", &expected.to_string()),
            ],
            Some(state),
            false, // never repeated for you: a repeat after a lost reply would answer 409
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    /// Accepts one connection per plan entry and, on each, answers one request per response in
    /// that entry, then closes the connection. Returns the raw requests it saw, in order.
    fn stub(plan: Vec<Vec<Vec<u8>>>) -> (String, thread::JoinHandle<Vec<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let mut seen = Vec::new();
            for responses in plan {
                let (mut s, _) = listener.accept().unwrap();
                let mut r = BufReader::new(s.try_clone().unwrap());
                for response in responses {
                    let mut raw = Vec::new();
                    let mut length = 0usize;
                    loop {
                        let mut line = String::new();
                        if r.read_line(&mut line).unwrap() == 0 {
                            return seen; // the client went away
                        }
                        raw.extend(line.as_bytes());
                        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                            length = v.trim().parse().unwrap();
                        }
                        if line == "\r\n" {
                            break;
                        }
                    }
                    let mut body = vec![0u8; length];
                    r.read_exact(&mut body).unwrap();
                    raw.extend(body);
                    seen.push(raw);
                    s.write_all(&response).unwrap();
                }
            }
            seen
        });
        (url, handle)
    }
    #[test]
    fn connections_send_without_delay() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let conn = Client::new(&url, "token").connect().unwrap();
        assert!(conn.get_ref().nodelay().unwrap());
    }
    /// One connection per response, each told to close after its reply.
    fn one_each(responses: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
        responses
            .into_iter()
            .map(|mut r| {
                let at = r.windows(2).position(|w| w == b"\r\n").map_or(0, |i| i + 2);
                let tail = r.split_off(at);
                r.extend(b"Connection: close\r\n");
                r.extend(tail);
                vec![r]
            })
            .collect()
    }
    fn ok(headers: &str, body: &[u8]) -> Vec<u8> {
        let mut v = format!("HTTP/1.1 200 OK\r\n{headers}Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        v.extend(body);
        v
    }
    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        hay.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn percent_codec() {
        assert_eq!(encode("+/\u{3bb} a"), "%2B%2F%CE%BB%20a");
        assert_eq!(encode("AZaz09-._~"), "AZaz09-._~");
        assert_eq!(decode("+/%CE%BB%20x").unwrap(), "+/\u{3bb} x");
        assert!(decode("%zz").is_err());
        assert!(decode("%4").is_err());
        assert!(decode("%FF").is_err()); // not UTF-8
    }

    #[test]
    fn publish_sends_binary_body_token_and_escaped_params() {
        let (url, h) = stub(one_each(vec![ok("Dostoevsky-Partition: 0\r\nDostoevsky-Offset: 7\r\n", b"")]));
        let ack = Client::new(&url, "tok").publish("jobs", &[0, 255], "+/\u{3bb}", "once").unwrap();
        assert_eq!((ack.partition, ack.offset), (0, 7));
        let raw = h.join().unwrap().remove(0);
        assert!(raw.starts_with(b"POST /dostoevsky/client/v1/publish?topic=jobs&key=%2B%2F%CE%BB&id=once HTTP/1.1\r\n"));
        assert!(contains(&raw, b"Authorization: Bearer tok\r\n"));
        assert!(contains(&raw, b"Content-Length: 2\r\n"));
        assert!(raw.ends_with(&[0, 255]));
    }

    #[test]
    fn read_returns_binary_value_from_content_length_and_chunked() {
        let chunked = b"HTTP/1.1 200 OK\r\nDostoevsky-Partition: 0\r\nDostoevsky-Offset: 5\r\nDostoevsky-Key: +/%CE%BB%20x\r\nDostoevsky-ID: x\r\nDostoevsky-Time: t\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n\x00\xff\r\n1;ext=1\r\nA\r\n0\r\n\r\n".to_vec();
        let plain = ok("Dostoevsky-Partition: 0\r\nDostoevsky-Offset: 5\r\nDostoevsky-Key: k\r\nDostoevsky-ID: i\r\nDostoevsky-Time: t\r\n", &[0, 255, 65]);
        let (url, h) = stub(one_each(vec![chunked, plain]));
        let c = Client::new(&url, "tok");
        let r = c.read("jobs", 0, 5, 100).unwrap().unwrap();
        assert_eq!((r.receipt.offset, r.key.as_str(), r.id.as_str(), r.value.as_slice()), (5, "+/\u{3bb} x", "x", &[0u8, 255, 65][..]));
        let r = c.read("jobs", 0, 5, 0).unwrap().unwrap();
        assert_eq!(r.value, vec![0, 255, 65]);
        let seen = h.join().unwrap();
        assert!(seen[0].starts_with(b"GET /dostoevsky/client/v1/read?topic=jobs&partition=0&offset=5&wait_ms=100 HTTP/1.1\r\n"));
    }

    #[test]
    fn no_content_is_none_and_errors_carry_status_and_word() {
        let (url, h) = stub(one_each(vec![
            b"HTTP/1.1 204 No Content\r\n\r\n".to_vec(),
            b"HTTP/1.1 409 Conflict\r\nContent-Length: 9\r\n\r\nconflict\n".to_vec(),
            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 13\r\n\r\nunauthorized\n".to_vec(),
        ]));
        let c = Client::new(&url, "tok");
        assert!(c.read("jobs", 0, 99, 0).unwrap().is_none());
        match c.publish("jobs", b"", "", "clash") {
            Err(Error::Api(409, code)) => assert_eq!(code, "conflict"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(c.position("jobs", 0, "g"), Err(Error::Api(401, _))));
        h.join().unwrap();
    }

    #[test]
    fn position_and_commit() {
        let (url, h) = stub(one_each(vec![
            ok("Dostoevsky-Next: 3\r\n", &[1, 2]),
            b"HTTP/1.1 204 No Content\r\n\r\n".to_vec(),
        ]));
        let c = Client::new(&url, "tok");
        let p = c.position("jobs", 0, "g").unwrap();
        assert_eq!((p.next, p.state), (3, vec![1, 2]));
        c.commit("jobs", 0, "g", 3, &[7]).unwrap();
        let seen = h.join().unwrap();
        assert!(seen[1].starts_with(b"POST /dostoevsky/client/v1/commit?topic=jobs&partition=0&group=g&expected=3 HTTP/1.1\r\n"));
        assert!(seen[1].ends_with(&[7]));
    }

    #[test]
    fn oversized_and_malformed_responses_are_refused() {
        let (url, h) = stub(one_each(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n".to_vec(),
            b"garbage\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort".to_vec(),
        ]));
        let c = Client::new(&url, "tok");
        for _ in 0..4 {
            assert!(matches!(c.position("jobs", 0, "g"), Err(Error::InvalidResponse(_))));
        }
        h.join().unwrap();
    }

    #[test]
    fn only_loopback_http_may_carry_the_token() {
        for bad in ["https://example.com", "http://example.com:80", "http://10.0.0.5:8787", "http://0.0.0.0:1", "http://localhost.example.com:1", "http://127.0.0.1.example.com:1"] {
            let e = Client::new(bad, "tok").position("t", 0, "g").unwrap_err();
            assert!(matches!(e, Error::Transport(_)) && e.to_string().contains("refusing"), "{bad}: {e}");
        }
        for ok in ["http://127.0.0.1:8787", "http://localhost:1", "http://[::1]:1", "http://127.9.9.9:1"] {
            assert!(!Client::new(ok, "tok").authority.is_empty(), "{ok}");
        }
    }

    #[test]
    fn a_path_prefix_is_kept() {
        let (url, h) = stub(one_each(vec![ok("Dostoevsky-Next: 0\r\n", b"")]));
        Client::new(&format!("{url}/mount/"), "tok").position("t", 0, "g").unwrap();
        assert!(h.join().unwrap()[0].starts_with(b"GET /mount/dostoevsky/client/v1/position?"));
    }

    fn client(url: &str) -> Client {
        let mut c = Client::new(url, "tok");
        c.timeout = Duration::from_secs(3); // a wrongly opened extra connection fails fast, not after 40 s
        c
    }

    #[test]
    fn keep_alive_serves_many_calls_on_one_connection() {
        let next = || ok("Dostoevsky-Next: 3\r\n", &[1, 2]);
        let (url, h) = stub(vec![vec![next(), next(), next(), next()]]); // ONE connection, four replies
        let c = client(&url);
        for _ in 0..4 {
            assert_eq!(c.position("jobs", 0, "g").unwrap().next, 3);
        }
        assert_eq!(h.join().unwrap().len(), 4);
    }

    #[test]
    fn a_server_that_says_close_gets_a_new_connection_next_time() {
        let next = ok("Dostoevsky-Next: 3\r\n", &[]);
        let mut closing = next.clone();
        let at = closing.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
        closing.splice(at..at, b"Connection: close\r\n".iter().cloned());
        let (url, h) = stub(vec![vec![closing], vec![next]]);
        let c = client(&url);
        c.position("jobs", 0, "g").unwrap();
        c.position("jobs", 0, "g").unwrap();
        assert_eq!(h.join().unwrap().len(), 2);
    }

    #[test]
    fn a_dead_reused_connection_is_retried_for_safe_calls_only() {
        let next = || ok("Dostoevsky-Next: 3\r\n", &[]);
        let ack = || ok("Dostoevsky-Partition: 0\r\nDostoevsky-Offset: 7\r\n", b"");
        // Each connection answers once and is then closed by the stub, leaving a dead one pooled.
        let (url, h) = stub(vec![vec![next()], vec![next()], vec![ack()], vec![next()]]);
        let c = client(&url);
        c.position("jobs", 0, "g").unwrap(); // connection 1, pooled
        c.position("jobs", 0, "g").unwrap(); // pooled one is dead: retried on connection 2
        c.publish("jobs", b"x", "", "once").unwrap(); // connection 2 is dead; an id makes it safe: connection 3
        // Connection 3 is dead. An id-less publish and a commit must NOT be repeated for you.
        let e = c.publish("jobs", b"x", "", "").unwrap_err();
        assert!(matches!(e, Error::Transport(_)), "{e:?}");
        c.position("jobs", 0, "g").unwrap(); // the dead one was dropped; this safe call opens connection 4
        let e = c.commit("jobs", 0, "g", 3, b"s");
        assert!(matches!(e, Err(Error::Transport(_))), "{e:?}");
        drop(c);
        let seen = h.join().unwrap();
        assert!(!seen.iter().any(|r| r.starts_with(b"POST /dostoevsky/client/v1/commit")), "the commit reached the server");
    }

    #[test]
    fn many_threads_share_one_client() {
        let c = std::sync::Arc::new(client("http://127.0.0.1:1")); // refused: every call fails the same way
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let c = c.clone();
                thread::spawn(move || c.position("jobs", 0, "g").is_err())
            })
            .collect();
        assert!(workers.into_iter().all(|w| w.join().unwrap()));
    }

    #[test]
    fn only_a_loopback_resolved_address_receives_the_token() {
        for ok in ["127.0.0.1:80", "127.9.9.9:1", "[::1]:80"] {
            assert!(check_resolved(&ok.parse().unwrap()).is_ok(), "{ok}");
        }
        for bad in ["10.0.0.5:80", "0.0.0.0:80", "192.168.1.1:80", "[2001:db8::1]:80", "93.184.216.34:80"] {
            let e = check_resolved(&bad.parse().unwrap()).unwrap_err();
            assert!(e.to_string().contains("not loopback"), "{bad}: {e}");
        }
    }

    #[test]
    fn chunk_trailers_are_bounded() {
        let many: String = (0..(MAX_HEADERS + 5)).map(|i| format!("X-T{i}: v\r\n")).collect();
        let few = "X-A: 1\r\nX-B: 2\r\n";
        let resp = |trailers: &str| {
            format!("HTTP/1.1 200 OK\r\nDostoevsky-Next: 3\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nA\r\n0\r\n{trailers}\r\n").into_bytes()
        };
        let (url, h) = stub(one_each(vec![resp(few), resp(&many)]));
        let c = Client::new(&url, "tok");
        assert_eq!(c.position("jobs", 0, "g").unwrap().next, 3);
        assert!(matches!(c.position("jobs", 0, "g"), Err(Error::InvalidResponse(_))));
        h.join().unwrap();
    }

    #[test]
    fn interim_replies_are_read_past_so_a_pooled_connection_stays_in_step() {
        let first = ok("Dostoevsky-Next: 3\r\n", &[]);
        let second = ok("Dostoevsky-Next: 4\r\n", &[]);
        let mut interim_then_first = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n".to_vec();
        interim_then_first.extend(first);
        // One connection: the first reply is preceded by interim replies, then a second exchange follows.
        let (url, h) = stub(vec![vec![interim_then_first, second]]);
        let c = client(&url);
        assert_eq!(c.position("jobs", 0, "g").unwrap().next, 3);
        assert_eq!(c.position("jobs", 0, "g").unwrap().next, 4, "the second call read a stale reply");
        assert_eq!(h.join().unwrap().len(), 2);
    }

    #[test]
    fn a_protocol_switch_is_refused() {
        let (url, h) = stub(one_each(vec![b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\n\r\n".to_vec()]));
        assert!(matches!(client(&url).position("jobs", 0, "g"), Err(Error::InvalidResponse(_))));
        h.join().unwrap();
    }
}
