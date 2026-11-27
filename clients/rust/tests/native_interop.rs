//! The native client against a real `dostoevsky-server`. Skipped unless both are set:
//! `DOSTOEVSKY_SERVER`, the server binary, and `DOSTOEVSKY_WORK`, a new empty directory with a
//! short path (a unix socket path is limited to about 100 bytes).

use dostoevsky_client::native::{NativeClient, NativeError};
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

const TOKEN: &str = "interop-token-0123456789abcdef";
const NOW: Duration = Duration::ZERO;

struct Server {
    child: Child,
    address: String,
}

impl Server {
    fn start(binary: &Path, data: &Path, listen: [&str; 2]) -> Self {
        let mut child = Command::new(binary)
            .arg("-data")
            .arg(data)
            .args(listen)
            .env("DOSTOEVSKY_TOKEN", TOKEN)
            .stdout(Stdio::piped())
            .spawn()
            .expect("start the server");
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let address = line
            .strip_prefix("listening ")
            .and_then(|rest| rest.split(' ').next())
            .unwrap_or_else(|| panic!("server did not start: {line:?}"))
            .to_string();
        Self { child, address }
    }
}

impl Drop for Server {
    // A kill, not a shutdown: what was acknowledged must survive it.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn code<T: std::fmt::Debug>(result: Result<T, NativeError>) -> &'static str {
    match result {
        Err(NativeError::Api(_, code)) => code,
        other => panic!("wanted an error code, got {other:?}"),
    }
}

fn checks(client: &NativeClient, topic: &str) {
    client.ping().unwrap();

    let first = client.publish(topic, b"one", "k", "a").unwrap();
    assert_eq!((first.partition, first.offset), (0, 0));
    assert_eq!(client.publish(topic, b"one", "k", "a").unwrap().offset, 0);
    assert_eq!(code(client.publish(topic, b"other", "k", "a")), "conflict");

    let record = client.read(topic, 0, 0, NOW).unwrap().unwrap();
    assert_eq!((record.key.as_str(), record.id.as_str()), ("k", "a"));
    assert_eq!(record.value, b"one");
    assert!(record.time.ends_with('Z') && record.time.as_bytes()[10] == b'T');
    assert!(client.read(topic, 0, 1, NOW).unwrap().is_none());
    assert!(client
        .read(topic, 0, 1, Duration::from_millis(50))
        .unwrap()
        .is_none());
    assert_eq!(code(client.read(topic, 7, 0, NOW)), "invalid");

    // A waiting read does not hold up a publish on the same client.
    std::thread::scope(|scope| {
        let waiting = scope.spawn(|| client.read(topic, 0, 1, Duration::from_secs(20)));
        std::thread::sleep(Duration::from_millis(500));
        client.publish(topic, b"two", "", "").unwrap();
        assert_eq!(waiting.join().unwrap().unwrap().unwrap().value, b"two");
    });

    assert_eq!(client.position(topic, 0, "g").unwrap().next, 0);
    client.commit(topic, 0, "g", 0, b"state").unwrap();
    assert_eq!(code(client.commit(topic, 0, "g", 0, b"")), "conflict");
    let position = client.position(topic, 0, "g").unwrap();
    assert_eq!((position.next, position.state), (1, b"state".to_vec()));

    let piped = format!("{topic}-p");
    let pending: Vec<_> = (0..1000)
        .map(|i| {
            client
                .publish_nowait(&piped, i.to_string().as_bytes(), "", "")
                .unwrap()
        })
        .collect();
    for (i, request) in pending.into_iter().enumerate() {
        assert_eq!(request.wait().unwrap().offset, i as i64);
    }

    let big = format!("{topic}-big");
    let value = vec![7u8; 1 << 20];
    let pending: Vec<_> = (0..3)
        .map(|_| client.publish_nowait(&big, &value, "", "").unwrap())
        .collect();
    for (i, request) in pending.into_iter().enumerate() {
        assert_eq!(request.wait().unwrap().offset, i as i64);
    }
    for i in 0..3 {
        assert_eq!(client.read(&big, 0, i, NOW).unwrap().unwrap().value, value);
    }

    let shared = format!("{topic}-threads");
    let mut offsets: Vec<i64> = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..100)
                        .map(|_| client.publish(&shared, b"x", "", "").unwrap().offset)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect()
    });
    offsets.sort_unstable();
    assert_eq!(offsets, (0..800).collect::<Vec<_>>());
}

fn after_restart(client: &NativeClient, topic: &str) {
    assert_eq!(
        client.read(topic, 0, 0, NOW).unwrap().unwrap().value,
        b"one"
    );
    assert_eq!(client.position(topic, 0, "g").unwrap().next, 1);
    assert_eq!(client.publish(topic, b"one", "k", "a").unwrap().offset, 0);
}

fn setup() -> Option<(PathBuf, PathBuf)> {
    let binary = std::env::var_os("DOSTOEVSKY_SERVER");
    let work = std::env::var_os("DOSTOEVSKY_WORK");
    match (binary, work) {
        (Some(binary), Some(work)) => Some((binary.into(), work.into())),
        _ => {
            eprintln!("skipped: set DOSTOEVSKY_SERVER and DOSTOEVSKY_WORK");
            None
        }
    }
}

#[test]
fn over_tcp() {
    let Some((binary, work)) = setup() else {
        return;
    };
    let data = work.join("tcp-data");
    let listen = ["-listen", "127.0.0.1:0"];
    let server = Server::start(&binary, &data, listen);
    let wrong = NativeClient::connect(&server.address, "wrong-token-wrong-token");
    assert!(matches!(
        wrong,
        Err(NativeError::Api(401, "unauthorized"))
    ));
    let client = NativeClient::connect(&server.address, TOKEN).unwrap();
    checks(&client, "t-tcp");
    drop(server);
    assert!(matches!(
        client.ping(),
        Err(NativeError::ConnectionLost { .. })
    ));
    let server = Server::start(&binary, &data, listen);
    after_restart(
        &NativeClient::connect(&server.address, TOKEN).unwrap(),
        "t-tcp",
    );
    println!("passed over tcp");
}

#[cfg(unix)]
#[test]
fn over_a_unix_socket() {
    let Some((binary, work)) = setup() else {
        return;
    };
    let data = work.join("unix-data");
    let socket = work.join("run").join("s");
    let listen = ["-unix", socket.to_str().unwrap()];
    let server = Server::start(&binary, &data, listen);
    checks(
        &NativeClient::connect_unix(&socket, TOKEN).unwrap(),
        "t-unix",
    );
    drop(server);
    let _server = Server::start(&binary, &data, listen);
    after_restart(
        &NativeClient::connect_unix(&socket, TOKEN).unwrap(),
        "t-unix",
    );
    println!("passed over a unix socket");
}
