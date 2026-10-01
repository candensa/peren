use chrono::Duration;
use peren_queues::*;

fn send(queue: &str, body: &[u8]) -> Send {
    Send {
        queue: queue.into(),
        body: body.to_vec(),
        content_type: Some("application/octet-stream".into()),
        partition: None,
        delay: Duration::zero(),
        dedup_id: None,
    }
}

fn policy() -> Policy {
    Policy {
        max_retries: 1,
        retry_delay: Duration::seconds(5),
        dead_letter: Some("dead".into()),
    }
}

#[path = "broker/adapter.rs"]
mod adapter;
#[path = "broker/admin.rs"]
mod admin;
#[path = "broker/delivery.rs"]
mod delivery;
#[path = "broker/file.rs"]
mod file;
#[path = "broker/shard.rs"]
mod shard;
#[path = "broker/window.rs"]
mod window;
