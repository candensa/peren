use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions, QueuePurgeOptions,
    },
    types::{AMQPValue, FieldTable, LongString},
};
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

use crate::{Conclusion, Lease, Message, Outcome, Policy, Purge, QueueError, Send, Stats};

const ID: &str = "peren-message-id";
const CONTENT: &str = "peren-content-type";
const PARTITION: &str = "peren-partition";
const ATTEMPTS: &str = "peren-attempts";
const DEDUP: &str = "peren-dedup-id";

#[derive(Clone)]
pub struct RabbitMqBroker {
    url: Arc<str>,
    channel: Arc<OnceCell<Channel>>,
    leases: Arc<Mutex<BTreeMap<Uuid, Delivery>>>,
}

struct Delivery {
    tag: lapin::types::DeliveryTag,
    queue: String,
    body: Vec<u8>,
    headers: FieldTable,
    attempts: u16,
}

impl RabbitMqBroker {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: Arc::from(url.into()),
            channel: Arc::default(),
            leases: Arc::default(),
        }
    }

    pub async fn send(&self, command: Send, now: DateTime<Utc>) -> Result<Uuid, QueueError> {
        if command.queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let id = Uuid::new_v4();
        let channel = self.channel().await?;
        ensure(&channel, &command.queue, None).await?;
        let attempts = 0;
        let headers = headers(
            id,
            command.content_type.as_deref(),
            command.partition.as_deref(),
            command.dedup_id.as_deref(),
            attempts,
        );
        let mut properties = BasicProperties::default()
            .with_headers(headers)
            .with_delivery_mode(2);
        let exchange = if command.delay > chrono::Duration::zero() {
            let delay = command
                .delay
                .to_std()
                .map_err(|_| QueueError::InvalidDelay)?;
            properties = properties.with_expiration(delay.as_millis().to_string().into());
            delay_exchange(&command.queue)
        } else {
            exchange(&command.queue)
        };
        publish(
            &channel,
            &exchange,
            &command.queue,
            command.body,
            properties,
        )
        .await?;
        let _ = now;
        Ok(id)
    }

    pub async fn lease(
        &self,
        queue: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<Lease>, QueueError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let channel = self.channel().await?;
        ensure(&channel, queue, None).await?;
        if paused(&channel, queue).await? {
            return Ok(Vec::new());
        }
        let mut leases = Vec::new();
        for _ in 0..limit {
            let Some(delivery) = channel
                .basic_get(queue_name(queue).into(), BasicGetOptions::default())
                .await
                .map_err(|error| QueueError::Broker(error.to_string()))?
            else {
                break;
            };
            let headers = delivery.properties.headers().clone().unwrap_or_default();
            let id = header_uuid(&headers, ID).unwrap_or_else(Uuid::new_v4);
            let attempts = header_u16(&headers, ATTEMPTS).unwrap_or(0);
            let lease_id = Uuid::new_v4();
            let message = Message {
                id,
                created_at: now,
                queue: queue.to_string(),
                body: delivery.data.clone(),
                content_type: header_string(&headers, CONTENT),
                partition: header_string(&headers, PARTITION),
                attempts,
                generation: u64::from(attempts).saturating_add(1),
                available_at: now,
            };
            self.leases.lock().await.insert(
                lease_id,
                Delivery {
                    tag: delivery.delivery_tag,
                    queue: queue.to_string(),
                    body: delivery.data.clone(),
                    headers,
                    attempts,
                },
            );
            leases.push(Lease {
                id: lease_id,
                generation: message.generation,
                message,
                expires_at: now + chrono::Duration::seconds(30),
            });
        }
        Ok(leases)
    }

    pub async fn conclude_with_delay(
        &self,
        lease: Uuid,
        outcome: Outcome,
        policy: &Policy,
        _now: DateTime<Utc>,
        retry_delay: Option<chrono::Duration>,
    ) -> Result<Conclusion, QueueError> {
        let delivery = self
            .leases
            .lock()
            .await
            .remove(&lease)
            .ok_or(QueueError::UnknownLease)?;
        let channel = self.channel().await?;
        match outcome {
            Outcome::Ack => {
                channel
                    .basic_ack(delivery.tag, BasicAckOptions::default())
                    .await
                    .map_err(|error| QueueError::Broker(error.to_string()))?;
                Ok(Conclusion::Acked)
            }
            Outcome::Retry if delivery.attempts < policy.max_retries => {
                let mut headers = delivery.headers.clone();
                set_attempts(&mut headers, delivery.attempts.saturating_add(1));
                let mut properties = BasicProperties::default()
                    .with_headers(headers)
                    .with_delivery_mode(2);
                let delay = retry_delay
                    .unwrap_or(policy.retry_delay)
                    .to_std()
                    .map_err(|_| QueueError::InvalidDelay)?;
                properties = properties.with_expiration(delay.as_millis().to_string().into());
                publish(
                    &channel,
                    &delay_exchange(&delivery.queue),
                    &delivery.queue,
                    delivery.body,
                    properties,
                )
                .await?;
                channel
                    .basic_ack(delivery.tag, BasicAckOptions::default())
                    .await
                    .map_err(|error| QueueError::Broker(error.to_string()))?;
                Ok(Conclusion::Retried)
            }
            Outcome::Retry | Outcome::Fail => {
                if let Some(target) = &policy.dead_letter {
                    ensure(&channel, target, None).await?;
                    publish(
                        &channel,
                        &exchange(target),
                        target,
                        delivery.body,
                        BasicProperties::default()
                            .with_headers(delivery.headers)
                            .with_delivery_mode(2),
                    )
                    .await?;
                    channel
                        .basic_ack(delivery.tag, BasicAckOptions::default())
                        .await
                        .map_err(|error| QueueError::Broker(error.to_string()))?;
                    Ok(Conclusion::DeadLettered)
                } else {
                    channel
                        .basic_ack(delivery.tag, BasicAckOptions::default())
                        .await
                        .map_err(|error| QueueError::Broker(error.to_string()))?;
                    Ok(Conclusion::Dropped)
                }
            }
        }
    }

    pub async fn stats(&self, queue: &str) -> Result<Stats, QueueError> {
        let channel = self.channel().await?;
        ensure(&channel, queue, None).await?;
        let ready = channel
            .queue_declare(
                queue_name(queue).into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?
            .message_count() as usize;
        let delayed = channel
            .queue_declare(
                delay_queue(queue).into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                delay_args(queue),
            )
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?
            .message_count() as usize;
        Ok(Stats {
            ready,
            delayed,
            leased: self
                .leases
                .lock()
                .await
                .values()
                .filter(|delivery| delivery.queue == queue)
                .count(),
            oldest_ready_at: None,
            paused: paused(&channel, queue).await?,
        })
    }

    pub async fn pause(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let channel = self.channel().await?;
        ensure(&channel, queue, None).await?;
        ensure_pause(&channel, queue).await?;
        if paused(&channel, queue).await? {
            return Ok(false);
        }
        publish(
            &channel,
            "",
            &pause_queue(queue),
            b"paused".to_vec(),
            BasicProperties::default().with_delivery_mode(2),
        )
        .await?;
        Ok(true)
    }

    pub async fn resume(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let channel = self.channel().await?;
        ensure(&channel, queue, None).await?;
        ensure_pause(&channel, queue).await?;
        let count = channel
            .queue_purge(pause_queue(queue).into(), QueuePurgeOptions::default())
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        Ok(count > 0)
    }

    pub async fn purge(&self, queue: &str) -> Result<Purge, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let channel = self.channel().await?;
        ensure(&channel, queue, None).await?;
        let ready = channel
            .queue_purge(queue_name(queue).into(), QueuePurgeOptions::default())
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))? as usize;
        let delayed = channel
            .queue_purge(delay_queue(queue).into(), QueuePurgeOptions::default())
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))? as usize;
        let before = self.leases.lock().await.len();
        self.leases
            .lock()
            .await
            .retain(|_, delivery| delivery.queue != queue);
        let leased = before - self.leases.lock().await.len();
        Ok(Purge {
            queued: ready + delayed,
            leased,
        })
    }

    pub async fn redrive(&self, source: &str, target: &str) -> Result<usize, QueueError> {
        if source.is_empty() || target.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let channel = self.channel().await?;
        ensure(&channel, source, None).await?;
        ensure(&channel, target, None).await?;
        let mut moved = 0;
        while let Some(delivery) = channel
            .basic_get(queue_name(source).into(), BasicGetOptions::default())
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?
        {
            publish(
                &channel,
                &exchange(target),
                target,
                delivery.data.clone(),
                BasicProperties::default()
                    .with_headers(delivery.properties.headers().clone().unwrap_or_default())
                    .with_delivery_mode(2),
            )
            .await?;
            channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await
                .map_err(|error| QueueError::Broker(error.to_string()))?;
            moved += 1;
        }
        Ok(moved)
    }

    async fn channel(&self) -> Result<Channel, QueueError> {
        self.channel
            .get_or_try_init(|| async {
                let connection = Connection::connect_with_runtime(
                    self.url.as_ref(),
                    ConnectionProperties::default(),
                    lapin::runtime::default_runtime()
                        .map_err(|error| QueueError::Connect(error.to_string()))?,
                )
                .await
                .map_err(|error| QueueError::Connect(error.to_string()))?;
                let channel = connection
                    .create_channel()
                    .await
                    .map_err(|error| QueueError::Connect(error.to_string()))?;
                channel
                    .confirm_select(ConfirmSelectOptions::default())
                    .await
                    .map_err(|error| QueueError::Connect(error.to_string()))?;
                Ok(channel)
            })
            .await
            .cloned()
    }
}

async fn ensure_pause(channel: &Channel, queue: &str) -> Result<(), QueueError> {
    channel
        .queue_declare(
            pause_queue(queue).into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    Ok(())
}

async fn paused(channel: &Channel, queue: &str) -> Result<bool, QueueError> {
    ensure_pause(channel, queue).await?;
    let declared = channel
        .queue_declare(
            pause_queue(queue).into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    Ok(declared.message_count() > 0)
}

async fn ensure(
    channel: &Channel,
    queue: &str,
    _dead_letter: Option<&str>,
) -> Result<(), QueueError> {
    channel
        .exchange_declare(
            exchange(queue).into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    channel
        .queue_declare(
            queue_name(queue).into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    channel
        .queue_bind(
            queue_name(queue).into(),
            exchange(queue).into(),
            queue.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;

    channel
        .exchange_declare(
            delay_exchange(queue).into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    channel
        .queue_declare(
            delay_queue(queue).into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            delay_args(queue),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    channel
        .queue_bind(
            delay_queue(queue).into(),
            delay_exchange(queue).into(),
            queue.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|error| QueueError::Broker(error.to_string()))?;

    Ok(())
}

async fn publish(
    channel: &Channel,
    exchange: &str,
    routing: &str,
    body: Vec<u8>,
    properties: BasicProperties,
) -> Result<(), QueueError> {
    let confirm = channel
        .basic_publish(
            exchange.into(),
            routing.into(),
            BasicPublishOptions::default(),
            &body,
            properties,
        )
        .await
        .map_err(|error| QueueError::Publish {
            queue: routing.to_string(),
            reason: error.to_string(),
        })?;
    confirm.await.map_err(|error| QueueError::Publish {
        queue: routing.to_string(),
        reason: error.to_string(),
    })?;
    Ok(())
}

fn headers(
    id: Uuid,
    content: Option<&str>,
    partition: Option<&str>,
    dedup: Option<&str>,
    attempts: u16,
) -> FieldTable {
    let mut headers = FieldTable::default();
    headers.insert(
        ID.into(),
        AMQPValue::LongString(LongString::from(id.to_string())),
    );
    headers.insert(ATTEMPTS.into(), AMQPValue::LongUInt(u32::from(attempts)));
    if let Some(value) = content {
        headers.insert(
            CONTENT.into(),
            AMQPValue::LongString(LongString::from(value.to_string())),
        );
    }
    if let Some(value) = partition {
        headers.insert(
            PARTITION.into(),
            AMQPValue::LongString(LongString::from(value.to_string())),
        );
    }
    if let Some(value) = dedup {
        headers.insert(
            DEDUP.into(),
            AMQPValue::LongString(LongString::from(value.to_string())),
        );
    }
    headers
}

fn set_attempts(headers: &mut FieldTable, attempts: u16) {
    headers.insert(ATTEMPTS.into(), AMQPValue::LongUInt(u32::from(attempts)));
}

fn header_string(headers: &FieldTable, name: &str) -> Option<String> {
    headers
        .inner()
        .get(name)
        .and_then(|value| value.as_long_string())
        .map(ToString::to_string)
}

fn header_uuid(headers: &FieldTable, name: &str) -> Option<Uuid> {
    header_string(headers, name).and_then(|value| Uuid::parse_str(&value).ok())
}
fn header_u16(headers: &FieldTable, name: &str) -> Option<u16> {
    headers
        .inner()
        .get(name)
        .and_then(AMQPValue::as_long_uint)
        .and_then(|value| u16::try_from(value).ok())
}

fn delay_args(queue: &str) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(LongString::from(exchange(queue))),
    );
    args.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString(LongString::from(queue.to_string())),
    );
    args
}

fn exchange(queue: &str) -> String {
    format!("peren.queue.{queue}")
}
fn delay_exchange(queue: &str) -> String {
    format!("peren.queue.{queue}.delay")
}
fn queue_name(queue: &str) -> String {
    format!("peren.queue.{queue}")
}
fn delay_queue(queue: &str) -> String {
    format!("peren.queue.{queue}.delay")
}
fn pause_queue(queue: &str) -> String {
    format!("peren.queue.{queue}.pause")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn send(queue: &str, body: &[u8]) -> Send {
        Send {
            queue: queue.to_string(),
            body: body.to_vec(),
            content_type: Some("application/octet-stream".to_string()),
            partition: None,
            delay: Duration::zero(),
            dedup_id: None,
        }
    }

    async fn wait_lease(broker: &RabbitMqBroker, queue: &str) -> Lease {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let mut leases = broker.lease(queue, 1, Utc::now()).await.unwrap();
            if let Some(lease) = leases.pop() {
                return lease;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for rabbitmq lease"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn prove_ack_retry_and_dead_letter(
        broker: &RabbitMqBroker,
        queue: &str,
        dead: &str,
        policy: &Policy,
    ) {
        broker.send(send(queue, b"ack"), Utc::now()).await.unwrap();
        let acked = wait_lease(broker, queue).await;
        assert_eq!(acked.message.body, b"ack");
        assert_eq!(
            broker
                .conclude_with_delay(acked.id, Outcome::Ack, policy, Utc::now(), None)
                .await
                .unwrap(),
            Conclusion::Acked
        );
        assert!(broker.lease(queue, 1, Utc::now()).await.unwrap().is_empty());

        broker
            .send(send(queue, b"retry"), Utc::now())
            .await
            .unwrap();
        let retry = wait_lease(broker, queue).await;
        assert_eq!(
            broker
                .conclude_with_delay(
                    retry.id,
                    Outcome::Retry,
                    policy,
                    Utc::now(),
                    Some(Duration::milliseconds(50)),
                )
                .await
                .unwrap(),
            Conclusion::Retried
        );
        let redelivered = wait_lease(broker, queue).await;
        assert_eq!(redelivered.message.body, b"retry");
        assert_eq!(
            broker
                .conclude_with_delay(redelivered.id, Outcome::Retry, policy, Utc::now(), None)
                .await
                .unwrap(),
            Conclusion::DeadLettered
        );
        let dead_letter = wait_lease(broker, dead).await;
        assert_eq!(dead_letter.message.body, b"retry");
        assert_eq!(
            broker
                .conclude_with_delay(dead_letter.id, Outcome::Ack, policy, Utc::now(), None)
                .await
                .unwrap(),
            Conclusion::Acked
        );
    }

    async fn prove_pause_resume(broker: &RabbitMqBroker, queue: &str, dead: &str, policy: &Policy) {
        assert!(broker.pause(queue).await.unwrap());
        assert!(!broker.pause(queue).await.unwrap());
        broker
            .send(send(queue, b"paused"), Utc::now())
            .await
            .unwrap();
        assert!(broker.lease(queue, 1, Utc::now()).await.unwrap().is_empty());
        let stats = broker.stats(queue).await.unwrap();
        assert!(stats.paused);
        assert_eq!(stats.ready, 1);
        assert!(broker.resume(queue).await.unwrap());
        let resumed = wait_lease(broker, queue).await;
        assert_eq!(resumed.message.body, b"paused");
        assert_eq!(
            broker
                .conclude_with_delay(resumed.id, Outcome::Fail, policy, Utc::now(), None)
                .await
                .unwrap(),
            Conclusion::DeadLettered
        );
        assert_eq!(broker.redrive(dead, queue).await.unwrap(), 1);
        let redriven = wait_lease(broker, queue).await;
        assert_eq!(redriven.message.body, b"paused");
        assert_eq!(
            broker
                .conclude_with_delay(redriven.id, Outcome::Ack, policy, Utc::now(), None)
                .await
                .unwrap(),
            Conclusion::Acked
        );
    }

    async fn prove_purge(broker: &RabbitMqBroker, queue: &str) {
        broker
            .send(send(queue, b"purge-a"), Utc::now())
            .await
            .unwrap();
        broker
            .send(send(queue, b"purge-b"), Utc::now())
            .await
            .unwrap();
        let purged = broker.purge(queue).await.unwrap();
        assert_eq!(purged.queued, 2);
        assert!(broker.lease(queue, 1, Utc::now()).await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires PEREN_AMQP_URL and a live RabbitMQ server"]
    async fn live_rabbitmq_owns_queue_lifecycle() {
        let url = std::env::var("PEREN_AMQP_URL").expect("PEREN_AMQP_URL is required");
        let broker = RabbitMqBroker::new(url);
        let queue = format!("jobs_{}", Uuid::new_v4().simple());
        let dead = format!("dead_{}", Uuid::new_v4().simple());
        let policy = Policy {
            max_retries: 1,
            retry_delay: Duration::milliseconds(100),
            dead_letter: Some(dead.clone()),
        };

        prove_ack_retry_and_dead_letter(&broker, &queue, &dead, &policy).await;
        prove_pause_resume(&broker, &queue, &dead, &policy).await;
        prove_purge(&broker, &queue).await;
    }
}
