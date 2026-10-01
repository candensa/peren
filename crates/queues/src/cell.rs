use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use peren_storage::{CellStorage, ListOptions, StorageError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Batch, Conclusion, Dedup, Lease, Message, Outcome, Policy, Purge, QueueError, Send, Shard,
    Stats, memory::RETENTION, validate_send,
};

const NAMESPACE: &str = "queue";
const LEASE_TIMEOUT: Duration = Duration::seconds(30);
const PAGE_LIMIT: usize = 1_000;

pub struct CellBroker {
    path: PathBuf,
}

impl CellBroker {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, QueueError> {
        let broker = Self::new(path);
        broker.with_storage(|_| Ok(()))?;
        Ok(broker)
    }

    pub fn send(&self, command: Send, now: DateTime<Utc>) -> Result<Uuid, QueueError> {
        validate_send(&command)?;
        let id = Uuid::new_v4();
        let available_at = now
            .checked_add_signed(command.delay)
            .ok_or(QueueError::InvalidDelay)?;
        let dedup = command
            .dedup_id
            .as_deref()
            .map(|key| (key.to_string(), Dedup::new(id, &command, now)));
        let message = Message {
            id,
            created_at: now,
            queue: command.queue.clone(),
            body: command.body,
            content_type: command.content_type,
            partition: command.partition,
            attempts: 0,
            generation: 0,
            available_at,
        };
        self.with_storage(|storage| {
            sweep(storage, now)?;
            if let Some((key, _)) = dedup.as_ref()
                && let Some(existing) = get_dedup(storage, &message.queue, key)?
            {
                return if existing.digest == dedup.as_ref().expect("dedup is present").1.digest {
                    Ok(existing.message)
                } else {
                    Err(QueueError::DedupConflict)
                };
            }
            put_message(storage, &message)?;
            if let Some((key, dedup)) = dedup.as_ref() {
                put_dedup(storage, &message.queue, key, dedup)?;
            }
            Ok(id)
        })
    }

    pub fn lease(
        &self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<Lease>, QueueError> {
        Ok(self.lease_batch(queue, limit, now)?.into_leases())
    }

    pub fn lease_batch(
        &self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Batch, QueueError> {
        self.lease_batch_filtered(queue, limit, now, None)
    }

    pub fn lease_shard(
        &self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Shard,
    ) -> Result<Vec<Lease>, QueueError> {
        Ok(self
            .lease_batch_shard(queue, limit, now, shard)?
            .into_leases())
    }

    pub fn lease_batch_shard(
        &self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Shard,
    ) -> Result<Batch, QueueError> {
        self.lease_batch_filtered(queue, limit, now, Some(shard))
    }

    fn lease_batch_filtered(
        &self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Option<Shard>,
    ) -> Result<Batch, QueueError> {
        self.with_storage(|storage| {
            sweep(storage, now)?;
            reclaim_expired(storage, now)?;
            if paused(storage, queue)? {
                return Ok(Batch {
                    queue: queue.to_string(),
                    leases: Vec::new(),
                    metrics: stats(storage, queue, now)?.into(),
                });
            }
            let mut leases = Vec::new();
            for (key, mut message) in messages(storage, Some(queue))? {
                if leases.len() == limit {
                    break;
                }
                if message.available_at > now
                    || shard.is_some_and(|shard| !message.belongs_to(shard))
                {
                    continue;
                }
                storage
                    .delete(NAMESPACE, &key)
                    .map_err(|error| cell_error(&error))?;
                message.generation = message.generation.saturating_add(1);
                let lease = Lease {
                    id: Uuid::new_v4(),
                    generation: message.generation,
                    expires_at: now + LEASE_TIMEOUT,
                    message,
                };
                put_lease(storage, &lease)?;
                leases.push(lease);
            }
            let metrics = stats(storage, queue, now)?.into();
            Ok(Batch {
                queue: queue.to_string(),
                leases,
                metrics,
            })
        })
    }

    pub fn conclude(
        &self,
        lease: Uuid,
        outcome: Outcome,
        policy: &Policy,
        now: DateTime<Utc>,
    ) -> Result<Conclusion, QueueError> {
        self.conclude_with_delay(lease, outcome, policy, now, None)
    }

    pub fn conclude_with_delay(
        &self,
        lease: Uuid,
        outcome: Outcome,
        policy: &Policy,
        now: DateTime<Utc>,
        delay: Option<Duration>,
    ) -> Result<Conclusion, QueueError> {
        self.with_storage(|storage| {
            sweep(storage, now)?;
            reclaim_expired(storage, now)?;
            let key = lease_key(lease);
            let mut lease = storage
                .get(NAMESPACE, &key)
                .map_err(|error| cell_error(&error))?
                .map(|bytes| serde_json::from_slice::<Lease>(&bytes))
                .transpose()
                .map_err(QueueError::Json)?
                .ok_or(QueueError::UnknownLease)?;
            storage
                .delete(NAMESPACE, &key)
                .map_err(|error| cell_error(&error))?;
            if storage
                .delete(NAMESPACE, &purged_key(lease.message.id))
                .map_err(|error| cell_error(&error))?
                .value
            {
                return Ok(Conclusion::Dropped);
            }
            match outcome {
                Outcome::Ack => Ok(Conclusion::Acked),
                Outcome::Retry if lease.message.attempts < policy.max_retries => {
                    lease.message.attempts += 1;
                    lease.message.available_at = now
                        .checked_add_signed(delay.unwrap_or(policy.retry_delay))
                        .ok_or(QueueError::InvalidDelay)?;
                    put_message(storage, &lease.message)?;
                    Ok(Conclusion::Retried)
                }
                Outcome::Retry | Outcome::Fail => match &policy.dead_letter {
                    Some(queue) => {
                        lease.message.queue.clone_from(queue);
                        lease.message.available_at = now;
                        put_message(storage, &lease.message)?;
                        Ok(Conclusion::DeadLettered)
                    }
                    None => Ok(Conclusion::Dropped),
                },
            }
        })
    }

    pub fn stats(&self, queue: &str, now: DateTime<Utc>) -> Result<Stats, QueueError> {
        self.with_storage(|storage| stats(storage, queue, now))
    }

    pub fn pause(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        self.with_storage(|storage| {
            let key = paused_key(queue);
            let changed = storage
                .get(NAMESPACE, &key)
                .map_err(|error| cell_error(&error))?
                .is_none();
            storage
                .put(NAMESPACE, &key, b"1")
                .map_err(|error| cell_error(&error))?;
            Ok(changed)
        })
    }

    pub fn resume(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        self.with_storage(|storage| {
            Ok(storage
                .delete(NAMESPACE, &paused_key(queue))
                .map_err(|error| cell_error(&error))?
                .value)
        })
    }

    pub fn purge(&self, queue: &str) -> Result<Purge, QueueError> {
        self.with_storage(|storage| {
            let queued_keys = messages(storage, Some(queue))?
                .into_iter()
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            let leased = leases(storage)?
                .into_iter()
                .filter_map(|(_, lease)| (lease.message.queue == queue).then_some(lease.message.id))
                .collect::<Vec<_>>();
            storage
                .delete_many(NAMESPACE, &queued_keys)
                .map_err(|error| cell_error(&error))?;
            for id in &leased {
                storage
                    .put(NAMESPACE, &purged_key(*id), b"1")
                    .map_err(|error| cell_error(&error))?;
            }
            Ok(Purge {
                queued: queued_keys.len(),
                leased: leased.len(),
            })
        })
    }

    pub fn redrive(
        &self,
        source: &str,
        target: &str,
        now: DateTime<Utc>,
    ) -> Result<usize, QueueError> {
        if source.is_empty() || target.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        self.with_storage(|storage| {
            sweep(storage, now)?;
            reclaim_expired(storage, now)?;
            let items = messages(storage, Some(source))?;
            let count = items.len();
            for (key, mut message) in items {
                storage
                    .delete(NAMESPACE, &key)
                    .map_err(|error| cell_error(&error))?;
                message.queue = target.to_string();
                message.available_at = now;
                put_message(storage, &message)?;
            }
            Ok(count)
        })
    }

    pub fn inspect(&self, queue: &str, limit: usize) -> Result<Vec<Message>, QueueError> {
        self.with_storage(|storage| {
            Ok(messages(storage, Some(queue))?
                .into_iter()
                .map(|(_, message)| message)
                .take(limit)
                .collect())
        })
    }

    fn with_storage<T>(
        &self,
        action: impl FnOnce(&mut CellStorage) -> Result<T, QueueError>,
    ) -> Result<T, QueueError> {
        create_parent(&self.path)?;
        let mut storage = CellStorage::open(&self.path).map_err(|error| cell_error(&error))?;
        action(&mut storage)
    }
}

#[derive(Deserialize, Serialize)]
struct MessageRecord {
    message: Message,
}

fn get_dedup(storage: &CellStorage, queue: &str, key: &str) -> Result<Option<Dedup>, QueueError> {
    storage
        .get(NAMESPACE, &dedup_key(queue, key))
        .map_err(|error| cell_error(&error))?
        .map(|bytes| serde_json::from_slice::<Dedup>(&bytes))
        .transpose()
        .map_err(QueueError::Json)
}

fn put_dedup(
    storage: &mut CellStorage,
    queue: &str,
    key: &str,
    dedup: &Dedup,
) -> Result<(), QueueError> {
    let bytes = serde_json::to_vec(dedup).map_err(QueueError::Json)?;
    storage
        .put(NAMESPACE, &dedup_key(queue, key), &bytes)
        .map_err(|error| cell_error(&error))?;
    Ok(())
}

fn dedup_key(queue: &str, key: &str) -> Vec<u8> {
    format!("d/{}/{}", escape(queue), escape(key)).into_bytes()
}

fn dedup_records(storage: &CellStorage) -> Result<Vec<(Vec<u8>, Dedup)>, QueueError> {
    let mut cursor = None;
    let mut values = Vec::new();
    loop {
        let page = storage
            .list(
                NAMESPACE,
                &ListOptions {
                    prefix: Some(b"d/"),
                    cursor: cursor.as_deref(),
                    limit: PAGE_LIMIT,
                    ..ListOptions::default()
                },
            )
            .map_err(|error| cell_error(&error))?;
        for (key, bytes) in page.entries {
            values.push((
                key,
                serde_json::from_slice::<Dedup>(&bytes).map_err(QueueError::Json)?,
            ));
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    Ok(values)
}

fn stats(storage: &CellStorage, queue: &str, now: DateTime<Utc>) -> Result<Stats, QueueError> {
    let (ready, delayed, oldest_ready_at) = messages(storage, Some(queue))?.into_iter().fold(
        (0, 0, None),
        |(ready, delayed, oldest), (_, message)| {
            if message.available_at <= now {
                (
                    ready + 1,
                    delayed,
                    Some(oldest.map_or(message.available_at, |seen: DateTime<Utc>| {
                        seen.min(message.available_at)
                    })),
                )
            } else {
                (ready, delayed + 1, oldest)
            }
        },
    );
    let leased = leases(storage)?
        .into_iter()
        .filter(|(_, lease)| lease.message.queue == queue)
        .count();
    Ok(Stats {
        ready,
        delayed,
        leased,
        paused: paused(storage, queue)?,
        oldest_ready_at,
    })
}

fn messages(
    storage: &CellStorage,
    queue: Option<&str>,
) -> Result<Vec<(Vec<u8>, Message)>, QueueError> {
    let prefix = queue.map_or_else(|| b"m/".to_vec(), message_prefix);
    let mut cursor = None;
    let mut values = Vec::new();
    loop {
        let page = storage
            .list(
                NAMESPACE,
                &ListOptions {
                    prefix: Some(&prefix),
                    cursor: cursor.as_deref(),
                    limit: PAGE_LIMIT,
                    ..ListOptions::default()
                },
            )
            .map_err(|error| cell_error(&error))?;
        for (key, bytes) in page.entries {
            let record =
                serde_json::from_slice::<MessageRecord>(&bytes).map_err(QueueError::Json)?;
            values.push((key, record.message));
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    Ok(values)
}

fn leases(storage: &CellStorage) -> Result<Vec<(Vec<u8>, Lease)>, QueueError> {
    let mut cursor = None;
    let mut values = Vec::new();
    loop {
        let page = storage
            .list(
                NAMESPACE,
                &ListOptions {
                    prefix: Some(b"l/"),
                    cursor: cursor.as_deref(),
                    limit: PAGE_LIMIT,
                    ..ListOptions::default()
                },
            )
            .map_err(|error| cell_error(&error))?;
        for (key, bytes) in page.entries {
            values.push((
                key,
                serde_json::from_slice::<Lease>(&bytes).map_err(QueueError::Json)?,
            ));
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    Ok(values)
}

fn put_message(storage: &mut CellStorage, message: &Message) -> Result<(), QueueError> {
    let record = MessageRecord {
        message: message.clone(),
    };
    let bytes = serde_json::to_vec(&record).map_err(QueueError::Json)?;
    storage
        .put(NAMESPACE, &message_key(message), &bytes)
        .map_err(|error| cell_error(&error))?;
    Ok(())
}

fn put_lease(storage: &mut CellStorage, lease: &Lease) -> Result<(), QueueError> {
    let bytes = serde_json::to_vec(lease).map_err(QueueError::Json)?;
    storage
        .put(NAMESPACE, &lease_key(lease.id), &bytes)
        .map_err(|error| cell_error(&error))?;
    Ok(())
}

fn reclaim_expired(storage: &mut CellStorage, now: DateTime<Utc>) -> Result<(), QueueError> {
    for (key, lease) in leases(storage)? {
        if lease.expires_at > now {
            continue;
        }
        storage
            .delete(NAMESPACE, &key)
            .map_err(|error| cell_error(&error))?;
        if storage
            .delete(NAMESPACE, &purged_key(lease.message.id))
            .map_err(|error| cell_error(&error))?
            .value
        {
            continue;
        }
        put_message(storage, &lease.message)?;
    }
    Ok(())
}

fn sweep(storage: &mut CellStorage, now: DateTime<Utc>) -> Result<(), QueueError> {
    let retain_after = now - RETENTION;
    let mut deleted_leases = Vec::new();
    for (key, lease) in leases(storage)? {
        if lease.message.created_at < retain_after {
            storage
                .delete(NAMESPACE, &key)
                .map_err(|error| cell_error(&error))?;
            deleted_leases.push(lease.message.id);
        }
    }
    for (key, message) in messages(storage, None)? {
        if message.created_at < retain_after {
            storage
                .delete(NAMESPACE, &key)
                .map_err(|error| cell_error(&error))?;
        }
    }
    for id in deleted_leases {
        storage
            .delete(NAMESPACE, &purged_key(id))
            .map_err(|error| cell_error(&error))?;
    }
    for (key, dedup) in dedup_records(storage)? {
        if dedup.created_at < retain_after {
            storage
                .delete(NAMESPACE, &key)
                .map_err(|error| cell_error(&error))?;
        }
    }
    Ok(())
}

fn paused(storage: &CellStorage, queue: &str) -> Result<bool, QueueError> {
    Ok(storage
        .get(NAMESPACE, &paused_key(queue))
        .map_err(|error| cell_error(&error))?
        .is_some())
}

fn message_key(message: &Message) -> Vec<u8> {
    let mut key = message_prefix(&message.queue);
    key.extend_from_slice(format!("{}:{}", timestamp(message.available_at), message.id).as_bytes());
    key
}

fn message_prefix(queue: &str) -> Vec<u8> {
    format!("m/{}/", escape(queue)).into_bytes()
}

fn lease_key(id: Uuid) -> Vec<u8> {
    format!("l/{id}").into_bytes()
}

fn paused_key(queue: &str) -> Vec<u8> {
    format!("p/{}", escape(queue)).into_bytes()
}

fn purged_key(id: Uuid) -> Vec<u8> {
    format!("x/{id}").into_bytes()
}

fn timestamp(value: DateTime<Utc>) -> String {
    let sortable = i128::from(value.timestamp_millis()) - i128::from(i64::MIN);
    format!("{sortable:020}")
}

fn escape(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .flat_map(|byte| format!("{byte:02x}").into_bytes())
        .map(char::from)
        .collect()
}

fn create_parent(path: &Path) -> Result<(), QueueError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| QueueError::Create {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

fn cell_error(error: &StorageError) -> QueueError {
    QueueError::Broker(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send(queue: &str, body: &[u8]) -> Send {
        Send {
            queue: queue.into(),
            body: body.to_vec(),
            content_type: None,
            partition: None,
            delay: Duration::zero(),
            dedup_id: None,
        }
    }

    fn policy() -> Policy {
        Policy {
            max_retries: 1,
            retry_delay: Duration::seconds(1),
            dead_letter: Some("dead".into()),
        }
    }

    #[test]
    fn cell_broker_persists_queue_lifecycle_in_cell_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let now = Utc::now();
        let lease = {
            let broker = CellBroker::open(&path).unwrap();
            broker.send(send("jobs", b"work"), now).unwrap();
            broker.lease("jobs", 1, now).unwrap().remove(0)
        };

        let broker = CellBroker::open(&path).unwrap();
        assert_eq!(broker.stats("jobs", now).unwrap().leased, 1);
        assert_eq!(
            broker
                .conclude(lease.id, Outcome::Retry, &policy(), now)
                .unwrap(),
            Conclusion::Retried
        );
        let retry = broker
            .lease("jobs", 1, now + Duration::seconds(1))
            .unwrap()
            .remove(0);
        assert_eq!(lease.generation, 1);
        assert_eq!(retry.generation, 2);
        assert_eq!(retry.message.body, b"work");
        assert_eq!(
            broker
                .conclude(retry.id, Outcome::Fail, &policy(), now)
                .unwrap(),
            Conclusion::DeadLettered
        );
        assert_eq!(broker.redrive("dead", "jobs", now).unwrap(), 1);
        assert_eq!(broker.purge("jobs").unwrap().queued, 1);
    }

    #[test]
    fn cell_broker_persists_producer_deduplication() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let now = Utc::now();
        let command = Send {
            dedup_id: Some("once".into()),
            ..send("jobs", b"work")
        };
        let first = CellBroker::open(&path)
            .unwrap()
            .send(command.clone(), now)
            .unwrap();
        let second = CellBroker::open(&path)
            .unwrap()
            .send(command, now + Duration::seconds(1))
            .unwrap();
        let conflict = CellBroker::open(&path).unwrap().send(
            Send {
                dedup_id: Some("once".into()),
                ..send("jobs", b"other")
            },
            now,
        );

        assert_eq!(first, second);
        assert!(matches!(conflict, Err(QueueError::DedupConflict)));
        assert_eq!(
            CellBroker::open(&path)
                .unwrap()
                .stats("jobs", now)
                .unwrap()
                .ready,
            1
        );
    }

    #[test]
    fn cell_broker_uses_record_oriented_cell_storage_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let broker = CellBroker::open(&path).unwrap();
        broker.send(send("jobs", b"work"), Utc::now()).unwrap();

        let storage = CellStorage::open(&path).unwrap();
        let page = storage.list(NAMESPACE, &ListOptions::default()).unwrap();
        assert_eq!(page.entries.len(), 1);
        assert!(String::from_utf8_lossy(&page.entries[0].0).starts_with("m/"));
    }

    #[test]
    fn cell_broker_batch_reports_backlog_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let broker = CellBroker::open(&path).unwrap();
        let now = Utc::now();
        broker.send(send("jobs", b"a"), now).unwrap();
        broker.send(send("jobs", b"b"), now).unwrap();
        broker
            .send(
                Send {
                    delay: Duration::seconds(10),
                    ..send("jobs", b"later")
                },
                now,
            )
            .unwrap();

        let batch = broker.lease_batch("jobs", 1, now).unwrap();

        assert_eq!(batch.queue, "jobs");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.metrics.ready, 1);
        assert_eq!(batch.metrics.delayed, 1);
        assert_eq!(batch.metrics.leased, 1);
        assert_eq!(batch.metrics.oldest_ready_at, Some(now));
    }

    #[test]
    fn cell_broker_leases_deterministic_shards() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let broker = CellBroker::open(&path).unwrap();
        let now = Utc::now();

        for index in 0..24 {
            let mut command = send("jobs", format!("work-{index}").as_bytes());
            command.partition = Some(format!("partition-{index}"));
            broker.send(command, now).unwrap();
        }

        let first = broker
            .lease_shard("jobs", 100, now, Shard::new(0, 2).unwrap())
            .unwrap();
        let second = broker
            .lease_shard("jobs", 100, now, Shard::new(1, 2).unwrap())
            .unwrap();

        assert!(!first.is_empty());
        assert!(!second.is_empty());
        assert!(first.iter().all(|lease| lease.generation == 1));
        assert!(
            second
                .iter()
                .all(|lease| lease.message.generation == lease.generation)
        );
        assert_eq!(first.len() + second.len(), 24);
        assert!(broker.lease("jobs", 1, now).unwrap().is_empty());
    }

    #[test]
    fn purged_leases_are_fenced_after_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let broker = CellBroker::open(&path).unwrap();
        let now = Utc::now();
        broker.send(send("jobs", b"leased"), now).unwrap();
        let lease = broker.lease("jobs", 1, now).unwrap().remove(0);
        assert_eq!(broker.purge("jobs").unwrap().leased, 1);
        assert_eq!(
            broker
                .conclude(lease.id, Outcome::Ack, &policy(), now)
                .unwrap(),
            Conclusion::Dropped
        );
        assert!(
            broker
                .lease("jobs", 1, now + Duration::seconds(31))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn expired_cell_leases_are_redelivered_with_generation_fencing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite");
        let broker = CellBroker::open(&path).unwrap();
        let now = Utc::now();
        broker.send(send("jobs", b"work"), now).unwrap();
        let stale = broker.lease("jobs", 1, now).unwrap().remove(0);

        let redelivered = broker
            .lease("jobs", 1, now + Duration::seconds(31))
            .unwrap()
            .remove(0);

        assert_eq!(stale.message.id, redelivered.message.id);
        assert_eq!(redelivered.generation, stale.generation + 1);
        assert_eq!(redelivered.message.generation, redelivered.generation);
        assert_ne!(stale.id, redelivered.id);
        assert!(matches!(
            broker.conclude(stale.id, Outcome::Ack, &policy(), now),
            Err(QueueError::UnknownLease)
        ));
        assert_eq!(
            broker
                .conclude(redelivered.id, Outcome::Ack, &policy(), now)
                .unwrap(),
            Conclusion::Acked
        );
    }
}
