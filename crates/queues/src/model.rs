use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::QueueError;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    pub ready: usize,
    pub delayed: usize,
    pub leased: usize,
    pub paused: bool,
    pub oldest_ready_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchWindow {
    pub max_size: usize,
    pub max_timeout: Duration,
}

impl BatchWindow {
    #[must_use]
    pub const fn new(max_size: usize, max_timeout: Duration) -> Self {
        Self {
            max_size,
            max_timeout,
        }
    }

    #[must_use]
    pub fn ready(self, stats: &Stats, now: DateTime<Utc>) -> bool {
        if stats.paused || stats.ready == 0 {
            return false;
        }
        if stats.ready >= self.max_size {
            return true;
        }
        stats
            .oldest_ready_at
            .is_none_or(|oldest| now.signed_duration_since(oldest) >= self.max_timeout)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Purge {
    pub queued: usize,
    pub leased: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Message {
    pub id: Uuid,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    pub queue: String,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub partition: Option<String>,
    pub attempts: u16,
    #[serde(default)]
    pub generation: u64,
    pub available_at: DateTime<Utc>,
}

impl Message {
    pub(crate) fn belongs_to(&self, shard: Shard) -> bool {
        let mut hash = Sha256::new();
        hash.update(self.queue.as_bytes());
        hash.update([0]);
        match &self.partition {
            Some(partition) => hash.update(partition.as_bytes()),
            None => hash.update(self.id.as_bytes()),
        }
        let digest = hash.finalize();
        let value = u64::from_be_bytes(
            digest[..8]
                .try_into()
                .expect("sha256 digest is wide enough"),
        );
        value % u64::from(shard.total) == u64::from(shard.index)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Send {
    pub queue: String,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub partition: Option<String>,
    pub delay: Duration,
    pub dedup_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Dedup {
    pub message: Uuid,
    pub digest: [u8; 32],
    pub created_at: DateTime<Utc>,
}

impl Dedup {
    #[must_use]
    pub fn new(message: Uuid, command: &Send, now: DateTime<Utc>) -> Self {
        Self {
            message,
            digest: command.digest(),
            created_at: now,
        }
    }

    #[must_use]
    pub fn matches(&self, command: &Send) -> bool {
        self.digest == command.digest()
    }
}

impl Send {
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(self.queue.as_bytes());
        hash.update([0]);
        hash.update(&self.body);
        hash.update([0]);
        if let Some(content) = &self.content_type {
            hash.update(content.as_bytes());
        }
        hash.update([0]);
        if let Some(partition) = &self.partition {
            hash.update(partition.as_bytes());
        }
        hash.update([0]);
        hash.update(self.delay.num_milliseconds().to_be_bytes());
        hash.finalize().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Policy {
    pub max_retries: u16,
    pub retry_delay: Duration,
    pub dead_letter: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Shard {
    pub index: u16,
    pub total: u16,
}

impl Shard {
    pub fn new(index: u16, total: u16) -> Result<Self, QueueError> {
        if total == 0 || index >= total {
            return Err(QueueError::InvalidShard);
        }
        Ok(Self { index, total })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Lease {
    pub id: Uuid,
    pub message: Message,
    pub generation: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Batch {
    pub queue: String,
    pub leases: Vec<Lease>,
    pub metrics: BatchMetrics,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchMetrics {
    pub ready: usize,
    pub delayed: usize,
    pub leased: usize,
    pub oldest_ready_at: Option<DateTime<Utc>>,
}

impl Batch {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.leases.len()
    }

    #[must_use]
    pub fn into_leases(self) -> Vec<Lease> {
        self.leases
    }
}

impl From<Stats> for BatchMetrics {
    fn from(stats: Stats) -> Self {
        Self {
            ready: stats.ready,
            delayed: stats.delayed,
            leased: stats.leased,
            oldest_ready_at: stats.oldest_ready_at,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ack,
    Retry,
    Fail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Conclusion {
    Acked,
    Retried,
    DeadLettered,
    Dropped,
}
