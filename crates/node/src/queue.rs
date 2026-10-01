use chrono::Utc;
use peren_config::ValidatedConfig;
use peren_primitives::CellId;
use peren_queues::{Conclusion, MemoryBroker, Outcome, Policy, Send};
use peren_runtime::{
    IsolateLimits, QueueDispatch, QueueDispositionKind, QueueEvent, QueueMessage, QueueMetrics,
    WorkerBundle,
};
use peren_storage::{CellStorage, EffectDraft, EffectRecord, StorageError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{Node, NodeError, NodeRepository, ProcessError, process};

#[derive(Debug)]
pub struct Depth {
    pub queue: String,
}

pub struct Pause {
    pub queue: String,
}

pub struct Resume {
    pub queue: String,
}

pub struct Purge {
    pub queue: String,
}

pub struct Redrive {
    pub source: String,
    pub target: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DepthReport {
    pub queue: String,
    pub ready: usize,
    pub delayed: usize,
    pub leased: usize,
    pub paused: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PauseReport {
    pub queue: String,
    pub changed: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PurgeReport {
    pub queue: String,
    pub queued: usize,
    pub leased: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RedriveReport {
    pub source: String,
    pub target: String,
    pub moved: usize,
}

pub async fn depth(config: &ValidatedConfig, request: &Depth) -> Result<DepthReport, ProcessError> {
    let broker = process::queue_broker(config, &process::default_data())?;
    let stats = broker.stats(&request.queue, Utc::now()).await?;
    Ok(DepthReport {
        queue: request.queue.clone(),
        ready: stats.ready,
        delayed: stats.delayed,
        leased: stats.leased,
        paused: stats.paused,
    })
}

pub async fn pause(config: &ValidatedConfig, request: &Pause) -> Result<PauseReport, ProcessError> {
    let broker = process::queue_broker(config, &process::default_data())?;
    let changed = broker.pause(&request.queue).await?;
    Ok(PauseReport {
        queue: request.queue.clone(),
        changed,
    })
}

pub async fn resume(
    config: &ValidatedConfig,
    request: &Resume,
) -> Result<PauseReport, ProcessError> {
    let broker = process::queue_broker(config, &process::default_data())?;
    let changed = broker.resume(&request.queue).await?;
    Ok(PauseReport {
        queue: request.queue.clone(),
        changed,
    })
}

pub async fn purge(config: &ValidatedConfig, request: &Purge) -> Result<PurgeReport, ProcessError> {
    let broker = process::queue_broker(config, &process::default_data())?;
    let report = broker.purge(&request.queue).await?;
    Ok(PurgeReport {
        queue: request.queue.clone(),
        queued: report.queued,
        leased: report.leased,
    })
}

pub async fn redrive(
    config: &ValidatedConfig,
    request: &Redrive,
) -> Result<RedriveReport, ProcessError> {
    let broker = process::queue_broker(config, &process::default_data())?;
    let moved = broker
        .redrive(&request.source, &request.target, Utc::now())
        .await?;
    Ok(RedriveReport {
        source: request.source.clone(),
        target: request.target.clone(),
        moved,
    })
}

const QUEUE_EFFECT_PREFIX: &str = "queue:";
const DEFAULT_EFFECT_LEASE_MS: i64 = 30_000;
const DEFAULT_EFFECT_RETRY_MS: i64 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueEffect {
    pub id: Uuid,
    pub queue: String,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub partition: Option<String>,
    pub delay: chrono::Duration,
    pub inbox_key: String,
}

impl QueueEffect {
    pub fn new(queue: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        let id = Uuid::new_v4();
        Self {
            id,
            queue: queue.into(),
            body: body.into(),
            content_type: None,
            partition: None,
            delay: chrono::Duration::zero(),
            inbox_key: id.to_string(),
        }
    }

    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    #[must_use]
    pub fn partition(mut self, value: impl Into<String>) -> Self {
        self.partition = Some(value.into());
        self
    }

    #[must_use]
    pub fn delay(mut self, value: chrono::Duration) -> Self {
        self.delay = value;
        self
    }

    #[must_use]
    pub fn inbox_key(mut self, value: impl Into<String>) -> Self {
        self.inbox_key = value.into();
        self
    }

    pub fn draft(self, now_ms: i64) -> Result<EffectDraft, QueueEffectError> {
        if self.queue.trim().is_empty() {
            return Err(QueueEffectError::EmptyQueue);
        }
        if self.inbox_key.trim().is_empty() {
            return Err(QueueEffectError::EmptyInbox);
        }
        let due_at_ms = now_ms
            .checked_add(self.delay.num_milliseconds())
            .ok_or(QueueEffectError::InvalidDelay)?;
        Ok(EffectDraft {
            id: self.id,
            destination: queue_destination(&self.queue),
            inbox_key: self.inbox_key,
            payload: serde_json::to_vec(&QueueEffectPayload {
                body: self.body,
                content_type: self.content_type,
                partition: self.partition,
            })?,
            due_at_ms,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct QueueEffectPayload {
    body: Vec<u8>,
    content_type: Option<String>,
    partition: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectPublish {
    pub claimed: usize,
    pub published: usize,
    pub retried: usize,
    pub skipped: usize,
}

pub fn publish_queue_effects(
    storage: &mut CellStorage,
    broker: &mut MemoryBroker,
    now_ms: i64,
    limit: usize,
) -> Result<EffectPublish, QueueEffectError> {
    publish_queue_effects_with_lease(
        storage,
        broker,
        now_ms,
        DEFAULT_EFFECT_LEASE_MS,
        DEFAULT_EFFECT_RETRY_MS,
        limit,
        &Uuid::new_v4().to_string(),
    )
}

pub fn publish_queue_effects_with_lease(
    storage: &mut CellStorage,
    broker: &mut MemoryBroker,
    now_ms: i64,
    lease_ms: i64,
    retry_ms: i64,
    limit: usize,
    lease_token: &str,
) -> Result<EffectPublish, QueueEffectError> {
    let lease_until = now_ms
        .checked_add(lease_ms)
        .ok_or(QueueEffectError::InvalidDelay)?;
    let retry_at = now_ms
        .checked_add(retry_ms)
        .ok_or(QueueEffectError::InvalidDelay)?;
    let effects = storage.claim_effects(now_ms, lease_token, lease_until, limit)?;
    let claimed = effects.len();
    let mut report = EffectPublish {
        claimed,
        published: 0,
        retried: 0,
        skipped: 0,
    };
    for effect in effects {
        match publish_queue_effect(broker, &effect, now_ms) {
            Ok(()) => {
                if storage.acknowledge_effect(effect.id, lease_token)? {
                    report.published += 1;
                } else {
                    report.skipped += 1;
                }
            }
            Err(error) if error.retryable() => {
                if storage.retry_effect(effect.id, lease_token, retry_at)? {
                    report.retried += 1;
                } else {
                    report.skipped += 1;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(report)
}

fn publish_queue_effect(
    broker: &mut MemoryBroker,
    effect: &EffectRecord,
    now_ms: i64,
) -> Result<(), QueueEffectError> {
    let Some(queue) = effect.destination.strip_prefix(QUEUE_EFFECT_PREFIX) else {
        return Ok(());
    };
    let payload: QueueEffectPayload = serde_json::from_slice(&effect.payload)?;
    broker.send(
        Send {
            queue: queue.to_string(),
            body: payload.body,
            content_type: payload.content_type,
            partition: payload.partition,
            delay: chrono::Duration::zero(),
            dedup_id: Some(effect.id.to_string()),
        },
        chrono::DateTime::<Utc>::from_timestamp_millis(now_ms)
            .ok_or(QueueEffectError::InvalidDelay)?,
    )?;
    Ok(())
}

fn queue_destination(queue: &str) -> String {
    format!("{QUEUE_EFFECT_PREFIX}{queue}")
}

#[derive(Debug, Error)]
pub enum QueueEffectError {
    #[error("queue effect has an empty queue")]
    EmptyQueue,
    #[error("queue effect has an empty inbox key")]
    EmptyInbox,
    #[error("queue effect delay is invalid")]
    InvalidDelay,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Queue(#[from] peren_queues::QueueError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl QueueEffectError {
    const fn retryable(&self) -> bool {
        matches!(self, Self::Queue(_))
    }
}

pub struct Delivery<'a, R: NodeRepository> {
    pub node: &'a Node<R>,
    pub broker: &'a mut MemoryBroker,
    pub queue: &'a str,
    pub cell: CellId,
    pub bundle: WorkerBundle,
    pub isolate: IsolateLimits,
    pub policy: Policy,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    pub leased: usize,
    pub acked: usize,
    pub retried: usize,
    pub dead: usize,
    pub dropped: usize,
}

pub async fn deliver<R: NodeRepository>(
    delivery: Delivery<'_, R>,
) -> Result<Report, DeliveryError> {
    let now = Utc::now();
    let batch = delivery
        .broker
        .lease_batch(delivery.queue, delivery.limit, now);
    if batch.is_empty() {
        return Ok(Report {
            leased: 0,
            acked: 0,
            retried: 0,
            dead: 0,
            dropped: 0,
        });
    }
    let event = QueueEvent {
        queue: delivery.queue.to_string(),
        metrics: QueueMetrics {
            ready: batch.metrics.ready,
            delayed: batch.metrics.delayed,
            leased: batch.metrics.leased,
            oldest_ready_timestamp: batch
                .metrics
                .oldest_ready_at
                .map(|value| value.timestamp_millis()),
        },
        messages: batch
            .leases
            .iter()
            .map(|lease| QueueMessage {
                id: lease.message.id.to_string(),
                body: lease.message.body.clone(),
                attempts: u32::from(lease.message.attempts),
                timestamp: lease.message.available_at.timestamp_millis(),
            })
            .collect(),
    };
    let leases = batch.into_leases();
    let dispatch = match delivery
        .node
        .dispatch_queue(delivery.cell, event, delivery.bundle, delivery.isolate)
        .await
    {
        Ok(dispatch) => dispatch,
        Err(error) => {
            let outcome = if error.is_non_retryable() {
                Outcome::Fail
            } else {
                Outcome::Retry
            };
            conclude(delivery.broker, leases, outcome, &delivery.policy)
                .map_err(DeliveryError::Queue)?;
            return Err(DeliveryError::Dispatch(error));
        }
    };
    conclude_dispatched(delivery.broker, leases, dispatch, &delivery.policy)
        .map_err(DeliveryError::Queue)
}

fn conclude_dispatched(
    broker: &mut MemoryBroker,
    leases: Vec<peren_queues::Lease>,
    dispatch: QueueDispatch,
    policy: &Policy,
) -> Result<Report, peren_queues::QueueError> {
    let mut outcomes = std::collections::BTreeMap::new();
    for disposition in dispatch.dispositions {
        outcomes.insert(
            disposition.id,
            (
                match disposition.outcome {
                    QueueDispositionKind::Ack => Outcome::Ack,
                    QueueDispositionKind::Retry => Outcome::Retry,
                },
                disposition
                    .delay_seconds
                    .map(i64::from)
                    .map(chrono::Duration::seconds),
            ),
        );
    }
    let mut report = Report {
        leased: leases.len(),
        acked: 0,
        retried: 0,
        dead: 0,
        dropped: 0,
    };
    for lease in leases {
        let (outcome, delay) = outcomes
            .remove(&lease.message.id.to_string())
            .unwrap_or((Outcome::Ack, None));
        match broker.conclude_with_delay(lease.id, outcome, policy, Utc::now(), delay)? {
            Conclusion::Acked => report.acked += 1,
            Conclusion::Retried => report.retried += 1,
            Conclusion::DeadLettered => report.dead += 1,
            Conclusion::Dropped => report.dropped += 1,
        }
    }
    Ok(report)
}

fn conclude(
    broker: &mut MemoryBroker,
    leases: Vec<peren_queues::Lease>,
    outcome: Outcome,
    policy: &Policy,
) -> Result<Report, peren_queues::QueueError> {
    let mut report = Report {
        leased: leases.len(),
        acked: 0,
        retried: 0,
        dead: 0,
        dropped: 0,
    };
    for lease in leases {
        match broker.conclude(lease.id, outcome, policy, Utc::now())? {
            Conclusion::Acked => report.acked += 1,
            Conclusion::Retried => report.retried += 1,
            Conclusion::DeadLettered => report.dead += 1,
            Conclusion::Dropped => report.dropped += 1,
        }
    }
    Ok(report)
}

#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error(transparent)]
    Queue(#[from] peren_queues::QueueError),
    #[error(transparent)]
    Dispatch(#[from] NodeError),
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use chrono::{Duration, Utc};
    use object_store::memory::InMemory;
    use peren_primitives::NodeId;
    use peren_provider_object_store::BucketStore;
    use peren_queues::{MemoryBroker, Policy};
    use peren_runtime::{IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle};
    use peren_storage::CellStorage;
    use uuid::Uuid;

    use super::*;

    fn bundle(source: &str) -> WorkerBundle {
        WorkerBundle::new(
            ModuleName::parse("worker.js").unwrap(),
            [(
                ModuleName::parse("worker.js").unwrap(),
                Module::new(ModuleKind::JavaScript, source.as_bytes().to_vec()).unwrap(),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap()
    }

    fn queue_bundle() -> WorkerBundle {
        bundle(
            "export default {
              async queue(event) {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('queue', new Uint8Array([event.messages.length]));
                });
              },
              async fetch() { return new Response('ok'); }
            };",
        )
    }

    fn failing_bundle() -> WorkerBundle {
        bundle(
            "export default { async queue() { throw new Error('boom'); }, async fetch() { return new Response('ok'); } };",
        )
    }

    fn mixed_disposition_bundle() -> WorkerBundle {
        bundle(
            "export default {
              async queue(event) {
                event.messages[0].retry({ delaySeconds: 30 });
                event.messages[1].ack();
              },
              async fetch() { return new Response('ok'); }
            };",
        )
    }

    fn policy() -> Policy {
        Policy {
            max_retries: 1,
            retry_delay: Duration::zero(),
            dead_letter: Some("dead".into()),
        }
    }

    fn send(queue: &str, body: &[u8]) -> peren_queues::Send {
        peren_queues::Send {
            queue: queue.into(),
            body: body.to_vec(),
            content_type: None,
            partition: None,
            delay: Duration::zero(),
            dedup_id: None,
        }
    }

    #[tokio::test]
    async fn delivery_acks_successful_queue_batches() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let node = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.clone(), store);
        let mut broker = MemoryBroker::new();
        broker.send(send("jobs", b"a"), Utc::now()).unwrap();

        let report = deliver(Delivery {
            node: &node,
            broker: &mut broker,
            queue: "jobs",
            cell: CellId::from_bytes([33; 32]),
            bundle: queue_bundle(),
            isolate: IsolateLimits::new(16 * 1024 * 1024, std::time::Duration::from_secs(5)),
            policy: policy(),
            limit: 10,
        })
        .await
        .unwrap();

        assert_eq!(report.acked, 1);
        assert_eq!(broker.queued("jobs"), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn delivery_honors_per_message_queue_dispositions() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let node = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.clone(), store);
        let mut broker = MemoryBroker::new();
        broker.send(send("jobs", b"retry"), Utc::now()).unwrap();
        broker.send(send("jobs", b"ack"), Utc::now()).unwrap();

        let report = deliver(Delivery {
            node: &node,
            broker: &mut broker,
            queue: "jobs",
            cell: CellId::from_bytes([36; 32]),
            bundle: mixed_disposition_bundle(),
            isolate: IsolateLimits::new(16 * 1024 * 1024, std::time::Duration::from_secs(5)),
            policy: policy(),
            limit: 10,
        })
        .await
        .unwrap();

        assert_eq!(report.leased, 2);
        assert_eq!(report.acked, 1);
        assert_eq!(report.retried, 1);
        assert!(broker.lease("jobs", 1, Utc::now()).is_empty());
        assert_eq!(
            broker
                .lease("jobs", 1, Utc::now() + Duration::seconds(31))
                .len(),
            1
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn delivery_retries_failed_batches_and_dead_letters_after_limit() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let node = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.clone(), store);
        let mut broker = MemoryBroker::new();
        broker.send(send("jobs", b"a"), Utc::now()).unwrap();

        for _ in 0..2 {
            assert!(
                deliver(Delivery {
                    node: &node,
                    broker: &mut broker,
                    queue: "jobs",
                    cell: CellId::from_bytes([34; 32]),
                    bundle: failing_bundle(),
                    isolate: IsolateLimits::new(
                        16 * 1024 * 1024,
                        std::time::Duration::from_secs(5)
                    ),
                    policy: policy(),
                    limit: 10,
                })
                .await
                .is_err()
            );
        }

        assert_eq!(broker.queued("jobs"), 0);
        assert_eq!(broker.queued("dead"), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn queue_effects_are_published_once_and_acknowledged() {
        let mut storage = CellStorage::open(std::path::Path::new(":memory:")).unwrap();
        let effect = QueueEffect::new("jobs", b"payload".to_vec())
            .content_type("text/plain")
            .partition("user-1")
            .inbox_key("same")
            .draft(100)
            .unwrap();
        let id = effect.id;
        storage
            .transaction_with_effects(
                |transaction| transaction.put("state", b"k", b"v"),
                vec![effect],
            )
            .unwrap();
        storage
            .transaction_with_effects(
                |transaction| transaction.put("state", b"k2", b"v2"),
                vec![
                    QueueEffect::new("jobs", b"duplicate".to_vec())
                        .inbox_key("same")
                        .draft(101)
                        .unwrap(),
                ],
            )
            .unwrap();
        let mut broker = MemoryBroker::new();

        let report = publish_queue_effects_with_lease(
            &mut storage,
            &mut broker,
            100,
            1_000,
            100,
            10,
            "lease-a",
        )
        .unwrap();

        assert_eq!(report.claimed, 1);
        assert_eq!(report.published, 1);
        assert_eq!(broker.queued("jobs"), 1);
        assert_eq!(
            storage.effect(id).unwrap().unwrap().status,
            peren_storage::EffectStatus::Acknowledged
        );
        let lease = broker
            .lease(
                "jobs",
                1,
                chrono::DateTime::<Utc>::from_timestamp_millis(100).unwrap(),
            )
            .pop()
            .unwrap();
        assert_eq!(lease.message.body, b"payload");
        assert_eq!(lease.message.content_type.as_deref(), Some("text/plain"));
        assert_eq!(lease.message.partition.as_deref(), Some("user-1"));
    }
}
