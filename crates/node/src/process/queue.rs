use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use peren_config::{QueueBroker, ValidatedConfig};
use peren_queues::{
    Adapter, AdapterSpec, AdapterStatus, Batch as QueueBatch, CellBroker, Conclusion, FileBroker,
    MemoryBroker, Outcome, Policy, Purge, Send, Shard, Stats, kafka::KafkaBroker, nats::NatsBroker,
    rabbit::RabbitMqBroker,
};
use tokio::sync::Mutex;

use super::ProcessError;

#[derive(Clone)]
pub(crate) enum QueueBrokerState {
    Memory(Arc<Mutex<MemoryBroker>>),
    File(Arc<Mutex<FileBroker>>),
    Cell(Arc<CellBroker>),
    Nats(NatsBroker),
    RabbitMq(RabbitMqBroker),
    Kafka(KafkaBroker),
}

impl QueueBrokerState {
    #[cfg(test)]
    pub(crate) fn memory() -> Self {
        Self::Memory(Arc::new(Mutex::new(MemoryBroker::new())))
    }

    pub(crate) async fn send(
        &self,
        command: Send,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<uuid::Uuid, peren_queues::QueueError> {
        peren_queues::validate_send(&command)?;
        match self {
            Self::Memory(broker) => broker.lock().await.send(command, now),
            Self::File(broker) => broker.lock().await.send(command, now),
            Self::Cell(broker) => broker.send(command, now),
            Self::Nats(broker) => broker.send(command, now).await,
            Self::RabbitMq(broker) => broker.send(command, now).await,
            Self::Kafka(broker) => broker.send(command, now).await,
        }
    }

    pub(crate) async fn lease_batch(
        &self,
        queue: &str,
        limit: usize,
        now: chrono::DateTime<chrono::Utc>,
        shard: Option<Shard>,
    ) -> Result<QueueBatch, peren_queues::QueueError> {
        match (self, shard) {
            (Self::Memory(broker), Some(shard)) => Ok(broker
                .lock()
                .await
                .lease_batch_shard(queue, limit, now, shard)),
            (Self::File(broker), Some(shard)) => {
                let leases = broker.lock().await.lease_shard(queue, limit, now, shard)?;
                let metrics = self.stats(queue, now).await?.into();
                Ok(QueueBatch {
                    queue: queue.to_string(),
                    leases,
                    metrics,
                })
            }
            (Self::Cell(broker), Some(shard)) => broker.lease_batch_shard(queue, limit, now, shard),
            (Self::Memory(broker), None) => Ok(broker.lock().await.lease_batch(queue, limit, now)),
            (Self::File(broker), None) => {
                let leases = broker.lock().await.lease(queue, limit, now)?;
                let metrics = self.stats(queue, now).await?.into();
                Ok(QueueBatch {
                    queue: queue.to_string(),
                    leases,
                    metrics,
                })
            }
            (Self::Cell(broker), None) => broker.lease_batch(queue, limit, now),
            (Self::Nats(broker), _) => {
                let leases = broker.lease(queue, limit, now).await?;
                let metrics = broker.stats(queue).await?.into();
                Ok(QueueBatch {
                    queue: queue.to_string(),
                    leases,
                    metrics,
                })
            }
            (Self::RabbitMq(broker), _) => {
                let leases = broker.lease(queue, limit, now).await?;
                let metrics = broker.stats(queue).await?.into();
                Ok(QueueBatch {
                    queue: queue.to_string(),
                    leases,
                    metrics,
                })
            }
            (Self::Kafka(broker), _) => {
                let leases = broker.lease(queue, limit, now).await?;
                let metrics = broker.stats(queue).await?.into();
                Ok(QueueBatch {
                    queue: queue.to_string(),
                    leases,
                    metrics,
                })
            }
        }
    }

    pub(crate) async fn stats(
        &self,
        queue: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Stats, peren_queues::QueueError> {
        match self {
            Self::Memory(broker) => Ok(broker.lock().await.stats(queue, now)),
            Self::File(broker) => Ok(broker.lock().await.stats(queue, now)),
            Self::Cell(broker) => broker.stats(queue, now),
            Self::Nats(broker) => broker.stats(queue).await,
            Self::RabbitMq(broker) => broker.stats(queue).await,
            Self::Kafka(broker) => broker.stats(queue).await,
        }
    }

    pub(crate) async fn pause(&self, queue: &str) -> Result<bool, peren_queues::QueueError> {
        match self {
            Self::Memory(broker) => broker.lock().await.pause(queue),
            Self::File(broker) => broker.lock().await.pause(queue),
            Self::Cell(broker) => broker.pause(queue),
            Self::Nats(broker) => broker.pause(queue).await,
            Self::RabbitMq(broker) => broker.pause(queue).await,
            Self::Kafka(broker) => broker.pause(queue).await,
        }
    }

    pub(crate) async fn resume(&self, queue: &str) -> Result<bool, peren_queues::QueueError> {
        match self {
            Self::Memory(broker) => broker.lock().await.resume(queue),
            Self::File(broker) => broker.lock().await.resume(queue),
            Self::Cell(broker) => broker.resume(queue),
            Self::Nats(broker) => broker.resume(queue).await,
            Self::RabbitMq(broker) => broker.resume(queue).await,
            Self::Kafka(broker) => broker.resume(queue).await,
        }
    }

    pub(crate) async fn purge(&self, queue: &str) -> Result<Purge, peren_queues::QueueError> {
        match self {
            Self::Memory(broker) => Ok(broker.lock().await.purge(queue)),
            Self::File(broker) => broker.lock().await.purge(queue),
            Self::Cell(broker) => broker.purge(queue),
            Self::Nats(broker) => broker.purge(queue).await,
            Self::RabbitMq(broker) => broker.purge(queue).await,
            Self::Kafka(broker) => broker.purge(queue).await,
        }
    }

    pub(crate) async fn redrive(
        &self,
        source: &str,
        target: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, peren_queues::QueueError> {
        match self {
            Self::Memory(broker) => broker.lock().await.redrive(source, target, now),
            Self::File(broker) => broker.lock().await.redrive(source, target, now),
            Self::Cell(broker) => broker.redrive(source, target, now),
            Self::Nats(broker) => broker.redrive(source, target, now).await,
            Self::RabbitMq(broker) => broker.redrive(source, target).await,
            Self::Kafka(broker) => broker.redrive(source, target, now).await,
        }
    }

    pub(super) async fn conclude_with_delay(
        &self,
        lease: uuid::Uuid,
        outcome: Outcome,
        policy: &Policy,
        now: chrono::DateTime<chrono::Utc>,
        delay: Option<chrono::Duration>,
    ) -> Result<Conclusion, peren_queues::QueueError> {
        match self {
            Self::Memory(broker) => broker
                .lock()
                .await
                .conclude_with_delay(lease, outcome, policy, now, delay),
            Self::File(broker) => broker
                .lock()
                .await
                .conclude_with_delay(lease, outcome, policy, now, delay),
            Self::Cell(broker) => broker.conclude_with_delay(lease, outcome, policy, now, delay),
            Self::Nats(broker) => {
                broker
                    .conclude_with_delay(lease, outcome, policy, now, delay)
                    .await
            }
            Self::RabbitMq(broker) => {
                broker
                    .conclude_with_delay(lease, outcome, policy, now, delay)
                    .await
            }
            Self::Kafka(broker) => {
                broker
                    .conclude_with_delay(lease, outcome, policy, now, delay)
                    .await
            }
        }
    }
}

pub(super) fn queue_provider_metadata(config: &ValidatedConfig) -> serde_json::Value {
    let Some(queues) = &config.raw.queues else {
        return serde_json::json!({ "kind": "memory" });
    };
    match queues.broker {
        QueueBroker::Memory => serde_json::json!({ "kind": "memory" }),
        QueueBroker::File => serde_json::json!({ "kind": "file" }),
        QueueBroker::Cell => serde_json::json!({ "kind": "cell" }),
        QueueBroker::Nats => serde_json::json!({ "kind": "nats" }),
        QueueBroker::RabbitMq => serde_json::json!({ "kind": "rabbitmq" }),
        QueueBroker::Kafka => serde_json::json!({ "kind": "kafka" }),
    }
}

pub(crate) fn queue_broker(
    config: &ValidatedConfig,
    data: &Path,
) -> Result<QueueBrokerState, ProcessError> {
    let Some(queues) = &config.raw.queues else {
        return Ok(QueueBrokerState::Memory(Arc::new(Mutex::new(
            MemoryBroker::new(),
        ))));
    };
    if matches!(queues.broker, QueueBroker::File) {
        let path = queues
            .file_path
            .as_deref()
            .ok_or(ProcessError::QueueBroker(queues.broker))?;
        return Ok(QueueBrokerState::File(Arc::new(Mutex::new(
            FileBroker::open(path)?,
        ))));
    }
    if matches!(queues.broker, QueueBroker::Cell) {
        return Ok(QueueBrokerState::Cell(Arc::new(CellBroker::open(
            cell_path(queues.cell_path.as_deref(), data),
        )?)));
    }
    let spec = queue_adapter(queues);
    match spec.validate()? {
        AdapterStatus::Supported(_) => match queues.broker {
            QueueBroker::Memory => Ok(QueueBrokerState::Memory(Arc::new(Mutex::new(
                MemoryBroker::new(),
            )))),
            QueueBroker::Nats => Ok(QueueBrokerState::Nats(NatsBroker::new(
                queues
                    .nats_url
                    .clone()
                    .ok_or(ProcessError::QueueBroker(queues.broker))?,
            ))),
            QueueBroker::RabbitMq => Ok(QueueBrokerState::RabbitMq(RabbitMqBroker::new(
                queues
                    .amqp_url
                    .clone()
                    .ok_or(ProcessError::QueueBroker(queues.broker))?,
            ))),
            QueueBroker::Kafka => Ok(QueueBrokerState::Kafka(KafkaBroker::new(
                queues
                    .kafka_bootstrap_servers
                    .clone()
                    .ok_or(ProcessError::QueueBroker(queues.broker))?,
            ))),
            QueueBroker::Cell | QueueBroker::File => Err(ProcessError::QueueBroker(queues.broker)),
        },
        AdapterStatus::Missing(_) => Err(ProcessError::QueueBroker(queues.broker)),
    }
}

fn queue_adapter(queues: &peren_config::Queues) -> AdapterSpec {
    match queues.broker {
        QueueBroker::Memory | QueueBroker::File => AdapterSpec::memory(),
        QueueBroker::Cell => AdapterSpec {
            adapter: Adapter::Cell,
            endpoint: None,
        },
        QueueBroker::Nats => AdapterSpec {
            adapter: Adapter::Nats,
            endpoint: queues.nats_url.clone(),
        },
        QueueBroker::RabbitMq => AdapterSpec {
            adapter: Adapter::RabbitMq,
            endpoint: queues.amqp_url.clone(),
        },
        QueueBroker::Kafka => AdapterSpec {
            adapter: Adapter::Kafka,
            endpoint: queues.kafka_bootstrap_servers.clone(),
        },
    }
}

fn cell_path(configured: Option<&str>, data: &Path) -> PathBuf {
    configured.map_or_else(|| data.join("queues").join("cell.sqlite"), PathBuf::from)
}
