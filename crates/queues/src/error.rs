use std::path::PathBuf;

use thiserror::Error;

use crate::Adapter;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("queue name cannot be empty")]
    EmptyQueue,
    #[error("queue shard must have a non-zero total and an index below the total")]
    InvalidShard,
    #[error("queue delay is outside the supported time range")]
    InvalidDelay,
    #[error("queue message is too large: {size} bytes exceeds {limit} bytes")]
    MessageTooLarge { size: usize, limit: usize },
    #[error("queue batch has too many messages: {count} exceeds {limit}")]
    BatchTooLarge { count: usize, limit: usize },
    #[error("queue batch payload is too large: {size} bytes exceeds {limit} bytes")]
    BatchBytesTooLarge { size: usize, limit: usize },
    #[error("queue lease is unknown or already concluded")]
    UnknownLease,
    #[error("queue deduplication id was reused with a different message")]
    DedupConflict,
    #[error("queue adapter {0:?} requires an endpoint")]
    MissingEndpoint(Adapter),
    #[error("queue adapter {adapter:?} endpoint must start with {expected:?}")]
    InvalidEndpoint {
        adapter: Adapter,
        expected: &'static str,
    },
    #[error("failed to connect queue broker: {0}")]
    Connect(String),
    #[error("failed to publish queue message to {queue}: {reason}")]
    Publish { queue: String, reason: String },
    #[error("queue broker operation failed: {0}")]
    Broker(String),
    #[error("failed to read queue broker {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create queue broker directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write queue broker {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
