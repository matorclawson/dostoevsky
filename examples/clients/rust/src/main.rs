use dostoevsky_client::Client;
use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    let url = env::var("DOSTOEVSKY_URL").unwrap_or_else(|_| "http://127.0.0.1:8787".into());
    let client = Client::new(&url, &env::var("DOSTOEVSKY_TOKEN")?);
    let ack = client.publish("jobs", b"task from rust", "jobs+\u{03bb}", "rust-demo")?;
    let group = "rust-worker";
    let mut next = client.position("jobs", ack.partition, group)?.next;
    while next <= ack.offset {
        let record = client
            .read("jobs", ack.partition, next, 1000)?
            .ok_or("missing job")?;
        println!("{}: {}", record.id, String::from_utf8_lossy(&record.value));
        client.commit("jobs", ack.partition, group, next, record.id.as_bytes())?;
        next += 1;
    }
    Ok(())
}
