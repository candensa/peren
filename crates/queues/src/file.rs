use std::{fs, path::PathBuf};

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::{
    Conclusion, Lease, MemoryBroker, Message, Outcome, Policy, Purge, QueueError, Send, Shard,
    Stats,
};

pub struct FileBroker {
    path: PathBuf,
    broker: MemoryBroker,
}

impl FileBroker {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, QueueError> {
        let path = path.into();
        let broker = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => MemoryBroker::new(),
            Err(source) => {
                return Err(QueueError::Read { path, source });
            }
        };
        Ok(Self { path, broker })
    }

    pub fn send(&mut self, command: Send, now: DateTime<Utc>) -> Result<Uuid, QueueError> {
        let id = self.broker.send(command, now)?;
        self.save()?;
        Ok(id)
    }

    pub fn lease(
        &mut self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<Lease>, QueueError> {
        let leases = self.broker.lease(queue, limit, now);
        self.save()?;
        Ok(leases)
    }

    pub fn lease_shard(
        &mut self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Shard,
    ) -> Result<Vec<Lease>, QueueError> {
        let leases = self.broker.lease_shard(queue, limit, now, shard);
        self.save()?;
        Ok(leases)
    }

    pub fn conclude(
        &mut self,
        lease: Uuid,
        outcome: Outcome,
        policy: &Policy,
        now: DateTime<Utc>,
    ) -> Result<Conclusion, QueueError> {
        self.conclude_with_delay(lease, outcome, policy, now, None)
    }

    pub fn conclude_with_delay(
        &mut self,
        lease: Uuid,
        outcome: Outcome,
        policy: &Policy,
        now: DateTime<Utc>,
        retry_delay: Option<Duration>,
    ) -> Result<Conclusion, QueueError> {
        let conclusion =
            self.broker
                .conclude_with_delay(lease, outcome, policy, now, retry_delay)?;
        self.save()?;
        Ok(conclusion)
    }

    #[must_use]
    pub fn queued(&self, queue: &str) -> usize {
        self.broker.queued(queue)
    }

    #[must_use]
    pub fn stats(&self, queue: &str, now: DateTime<Utc>) -> Stats {
        self.broker.stats(queue, now)
    }

    pub fn pause(&mut self, queue: &str) -> Result<bool, QueueError> {
        let changed = self.broker.pause(queue)?;
        self.save()?;
        Ok(changed)
    }

    pub fn resume(&mut self, queue: &str) -> Result<bool, QueueError> {
        let changed = self.broker.resume(queue)?;
        self.save()?;
        Ok(changed)
    }

    pub fn purge(&mut self, queue: &str) -> Result<Purge, QueueError> {
        let report = self.broker.purge(queue);
        self.save()?;
        Ok(report)
    }

    pub fn redrive(
        &mut self,
        source: &str,
        target: &str,
        now: DateTime<Utc>,
    ) -> Result<usize, QueueError> {
        let count = self.broker.redrive(source, target, now)?;
        self.save()?;
        Ok(count)
    }

    #[must_use]
    pub fn inspect(&self, queue: &str, limit: usize) -> Vec<Message> {
        self.broker.inspect(queue, limit)
    }

    fn save(&self) -> Result<(), QueueError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| QueueError::Create {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let bytes = serde_json::to_vec_pretty(&self.broker)?;
        fs::write(&self.path, bytes).map_err(|source| QueueError::Write {
            path: self.path.clone(),
            source,
        })
    }
}
