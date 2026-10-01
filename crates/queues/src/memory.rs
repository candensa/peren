use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Batch, Conclusion, Dedup, Lease, Message, Outcome, Policy, Purge, QueueError, Send, Shard,
    Stats,
};

pub const MAX_MESSAGE_BYTES: usize = 128_000;
pub const MAX_BATCH_MESSAGES: usize = 100;
pub const MAX_BATCH_BYTES: usize = 256_000;
pub const MAX_DELAY: Duration = Duration::seconds(86_400);
pub const RETENTION: Duration = Duration::days(4);
const LEASE_TIMEOUT: Duration = Duration::seconds(30);

#[derive(Default, Deserialize, Serialize)]
pub struct MemoryBroker {
    queues: BTreeMap<String, VecDeque<Message>>,
    leases: BTreeMap<Uuid, Lease>,
    #[serde(default)]
    paused: BTreeSet<String>,
    #[serde(default)]
    purged: BTreeSet<Uuid>,
    #[serde(default)]
    dedup: BTreeMap<String, Dedup>,
}

impl MemoryBroker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn send(&mut self, command: Send, now: DateTime<Utc>) -> Result<Uuid, QueueError> {
        validate_send(&command)?;
        self.sweep(now);
        if let Some(key) = command.dedup_id.as_deref()
            && let Some(existing) = self.dedup.get(&dedup_key(&command.queue, key))
        {
            return if existing.matches(&command) {
                Ok(existing.message)
            } else {
                Err(QueueError::DedupConflict)
            };
        }
        let id = Uuid::new_v4();
        let dedup = command.dedup_id.as_deref().map(|key| {
            (
                dedup_key(&command.queue, key),
                Dedup::new(id, &command, now),
            )
        });
        let available_at = now
            .checked_add_signed(command.delay)
            .ok_or(QueueError::InvalidDelay)?;
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
        self.queues
            .entry(command.queue)
            .or_default()
            .push_back(message);
        if let Some((key, dedup)) = dedup {
            self.dedup.insert(key, dedup);
        }
        Ok(id)
    }

    pub fn lease(&mut self, queue: &str, limit: usize, now: DateTime<Utc>) -> Vec<Lease> {
        self.lease_batch(queue, limit, now).into_leases()
    }

    pub fn lease_batch(&mut self, queue: &str, limit: usize, now: DateTime<Utc>) -> Batch {
        self.lease_batch_filtered(queue, limit, now, None)
    }

    pub fn lease_shard(
        &mut self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Shard,
    ) -> Vec<Lease> {
        self.lease_batch_shard(queue, limit, now, shard)
            .into_leases()
    }

    pub fn lease_batch_shard(
        &mut self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Shard,
    ) -> Batch {
        self.lease_batch_filtered(queue, limit, now, Some(shard))
    }

    fn lease_batch_filtered(
        &mut self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
        shard: Option<Shard>,
    ) -> Batch {
        self.sweep(now);
        self.reclaim_expired(now);
        if self.paused.contains(queue) {
            return Batch {
                queue: queue.to_string(),
                leases: Vec::new(),
                metrics: self.stats(queue, now).into(),
            };
        }
        let Some(messages) = self.queues.get_mut(queue) else {
            return Batch {
                queue: queue.to_string(),
                leases: Vec::new(),
                metrics: self.stats(queue, now).into(),
            };
        };
        let mut ready = Vec::new();
        let mut deferred = VecDeque::new();
        while let Some(mut message) = messages.pop_front() {
            if ready.len() < limit
                && message.available_at <= now
                && shard.is_none_or(|shard| message.belongs_to(shard))
            {
                message.generation = message.generation.saturating_add(1);
                let lease = Lease {
                    id: Uuid::new_v4(),
                    generation: message.generation,
                    message,
                    expires_at: now + LEASE_TIMEOUT,
                };
                self.leases.insert(lease.id, lease.clone());
                ready.push(lease);
            } else {
                deferred.push_back(message);
            }
        }
        *messages = deferred;
        let metrics = self.stats(queue, now).into();
        Batch {
            queue: queue.to_string(),
            leases: ready,
            metrics,
        }
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
        self.sweep(now);
        self.reclaim_expired(now);
        let mut lease = self.leases.remove(&lease).ok_or(QueueError::UnknownLease)?;
        if self.purged.remove(&lease.message.id) {
            return Ok(Conclusion::Dropped);
        }
        match outcome {
            Outcome::Ack => Ok(Conclusion::Acked),
            Outcome::Retry if lease.message.attempts < policy.max_retries => {
                lease.message.attempts += 1;
                lease.message.available_at = now
                    .checked_add_signed(retry_delay.unwrap_or(policy.retry_delay))
                    .ok_or(QueueError::InvalidDelay)?;
                self.queues
                    .entry(lease.message.queue.clone())
                    .or_default()
                    .push_back(lease.message);
                Ok(Conclusion::Retried)
            }
            Outcome::Retry | Outcome::Fail => match &policy.dead_letter {
                Some(queue) => {
                    lease.message.queue.clone_from(queue);
                    lease.message.available_at = now;
                    self.queues
                        .entry(queue.clone())
                        .or_default()
                        .push_back(lease.message);
                    Ok(Conclusion::DeadLettered)
                }
                None => Ok(Conclusion::Dropped),
            },
        }
    }

    #[must_use]
    pub fn queued(&self, queue: &str) -> usize {
        self.queues.get(queue).map_or(0, VecDeque::len)
    }

    #[must_use]
    pub fn stats(&self, queue: &str, now: DateTime<Utc>) -> Stats {
        let (ready, delayed, oldest_ready_at) = self
            .queues
            .get(queue)
            .map(|messages| {
                messages
                    .iter()
                    .fold((0, 0, None), |(ready, delayed, oldest), message| {
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
                    })
            })
            .unwrap_or_default();
        let leased = self
            .leases
            .values()
            .filter(|lease| lease.message.queue == queue)
            .count();
        Stats {
            ready,
            delayed,
            leased,
            paused: self.paused.contains(queue),
            oldest_ready_at,
        }
    }

    pub fn pause(&mut self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        Ok(self.paused.insert(queue.to_string()))
    }

    pub fn resume(&mut self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        Ok(self.paused.remove(queue))
    }

    pub fn purge(&mut self, queue: &str) -> Purge {
        let queued = self
            .queues
            .remove(queue)
            .map_or(0, |messages| messages.len());
        let leased = self
            .leases
            .values()
            .filter(|lease| lease.message.queue == queue)
            .map(|lease| lease.message.id)
            .collect::<Vec<_>>();
        for id in &leased {
            self.purged.insert(*id);
        }
        Purge {
            queued,
            leased: leased.len(),
        }
    }

    pub fn redrive(
        &mut self,
        source: &str,
        target: &str,
        now: DateTime<Utc>,
    ) -> Result<usize, QueueError> {
        self.sweep(now);
        self.reclaim_expired(now);
        if source.is_empty() || target.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let Some(mut messages) = self.queues.remove(source) else {
            return Ok(0);
        };
        let count = messages.len();
        for message in &mut messages {
            message.queue = target.to_string();
            message.available_at = now;
        }
        self.queues
            .entry(target.to_string())
            .or_default()
            .extend(messages);
        Ok(count)
    }

    fn sweep(&mut self, now: DateTime<Utc>) {
        let retain_after = now - RETENTION;
        for messages in self.queues.values_mut() {
            messages.retain(|message| message.created_at >= retain_after);
        }
        self.leases
            .retain(|_, lease| lease.message.created_at >= retain_after);
        self.purged
            .retain(|id| self.leases.values().any(|lease| lease.message.id == *id));
        let retain_after = now - RETENTION;
        self.dedup
            .retain(|_, dedup| dedup.created_at >= retain_after);
    }

    fn reclaim_expired(&mut self, now: DateTime<Utc>) {
        let expired = self
            .leases
            .iter()
            .filter_map(|(id, lease)| (lease.expires_at <= now).then_some(*id))
            .collect::<Vec<_>>();
        for id in expired {
            if let Some(lease) = self.leases.remove(&id) {
                if self.purged.remove(&lease.message.id) {
                    continue;
                }
                self.queues
                    .entry(lease.message.queue.clone())
                    .or_default()
                    .push_front(lease.message);
            }
        }
    }

    #[must_use]
    pub fn inspect(&self, queue: &str, limit: usize) -> Vec<Message> {
        self.queues
            .get(queue)
            .into_iter()
            .flat_map(|messages| messages.iter())
            .take(limit)
            .cloned()
            .collect()
    }
}

pub fn validate_batch(messages: &[Send]) -> Result<(), QueueError> {
    if messages.len() > MAX_BATCH_MESSAGES {
        return Err(QueueError::BatchTooLarge {
            count: messages.len(),
            limit: MAX_BATCH_MESSAGES,
        });
    }
    let size = messages
        .iter()
        .map(|message| message.body.len())
        .sum::<usize>();
    if size > MAX_BATCH_BYTES {
        return Err(QueueError::BatchBytesTooLarge {
            size,
            limit: MAX_BATCH_BYTES,
        });
    }
    for message in messages {
        validate_send(message)?;
    }
    Ok(())
}

pub fn validate_send(command: &Send) -> Result<(), QueueError> {
    if command.queue.is_empty() {
        return Err(QueueError::EmptyQueue);
    }
    if command.body.len() > MAX_MESSAGE_BYTES {
        return Err(QueueError::MessageTooLarge {
            size: command.body.len(),
            limit: MAX_MESSAGE_BYTES,
        });
    }
    if command.delay < Duration::zero() || command.delay > MAX_DELAY {
        return Err(QueueError::InvalidDelay);
    }
    Ok(())
}

fn dedup_key(queue: &str, key: &str) -> String {
    format!("{queue}\0{key}")
}
