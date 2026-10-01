use std::{collections::BTreeMap, sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Utc};
use rdkafka::{
    Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Header, Headers, Message as KafkaRecord, OwnedHeaders, OwnedMessage},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

use crate::{Conclusion, Lease, Message, Outcome, Policy, Purge, QueueError, Send, Stats};

const ID: &str = "peren-message-id";
const CONTENT: &str = "peren-content-type";
const PARTITION: &str = "peren-partition";
const ATTEMPTS: &str = "peren-attempts";
const DELIVER: &str = "peren-deliver-at-ms";
const DEDUP: &str = "peren-dedup-id";

#[derive(Clone)]
pub struct KafkaBroker {
    servers: Arc<str>,
    producer: Arc<OnceCell<FutureProducer>>,
    main: Arc<Mutex<BTreeMap<String, Arc<StreamConsumer>>>>,
    delay: Arc<Mutex<BTreeMap<String, Arc<StreamConsumer>>>>,
    leases: Arc<Mutex<BTreeMap<Uuid, Delivery>>>,
}

struct Delivery {
    record: OwnedMessage,
    queue: String,
    id: Uuid,
    attempts: u16,
    content: Option<String>,
    partition: Option<String>,
    dedup: Option<String>,
}

impl KafkaBroker {
    #[must_use]
    pub fn new(servers: impl Into<String>) -> Self {
        Self {
            servers: Arc::from(servers.into()),
            producer: Arc::default(),
            main: Arc::default(),
            delay: Arc::default(),
            leases: Arc::default(),
        }
    }

    pub async fn send(&self, command: Send, now: DateTime<Utc>) -> Result<Uuid, QueueError> {
        if command.queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        self.ensure(&command.queue).await?;
        let id = Uuid::new_v4();
        let mut headers = headers(
            id,
            0,
            command.content_type.as_deref(),
            command.partition.as_deref(),
            command.dedup_id.as_deref(),
        );
        let topic = if command.delay > chrono::Duration::zero() {
            let deliver_at = now
                .checked_add_signed(command.delay)
                .ok_or(QueueError::InvalidDelay)?
                .timestamp_millis()
                .to_string();
            headers = headers.insert(Header {
                key: DELIVER,
                value: Some(deliver_at.as_bytes()),
            });
            delay_topic(&command.queue)
        } else {
            topic(&command.queue)
        };
        self.publish(&topic, command.body, headers).await?;
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
        self.ensure(queue).await?;
        if self.paused(queue).await? {
            return Ok(Vec::new());
        }
        self.pump_delay(queue, now).await?;
        let consumer = self.main(queue).await?;
        let mut leases = Vec::new();
        let deadline = tokio::time::Instant::now() + StdDuration::from_millis(500);
        while leases.len() < limit && tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let received = tokio::time::timeout(remaining, consumer.recv()).await;
            let Ok(result) = received else {
                break;
            };
            let borrowed = match result {
                Ok(message) => message,
                Err(error) if transient(&error.to_string()) => break,
                Err(error) => return Err(QueueError::Broker(error.to_string())),
            };
            let record = borrowed.detach();
            let parsed = parse(queue, record, now);
            let lease_id = Uuid::new_v4();
            self.leases.lock().await.insert(lease_id, parsed.delivery);
            leases.push(Lease {
                id: lease_id,
                generation: parsed.message.generation,
                message: parsed.message,
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
        now: DateTime<Utc>,
        retry_delay: Option<chrono::Duration>,
    ) -> Result<Conclusion, QueueError> {
        let delivery = self
            .leases
            .lock()
            .await
            .remove(&lease)
            .ok_or(QueueError::UnknownLease)?;
        match outcome {
            Outcome::Ack => {
                self.commit(&delivery.record).await?;
                Ok(Conclusion::Acked)
            }
            Outcome::Retry if delivery.attempts < policy.max_retries => {
                let attempts = delivery.attempts.saturating_add(1);
                let mut headers = headers(
                    delivery.id,
                    attempts,
                    delivery.content.as_deref(),
                    delivery.partition.as_deref(),
                    delivery.dedup.as_deref(),
                );
                let delay = retry_delay.unwrap_or(policy.retry_delay);
                let topic = if delay > chrono::Duration::zero() {
                    let deliver_at = now
                        .checked_add_signed(delay)
                        .ok_or(QueueError::InvalidDelay)?
                        .timestamp_millis()
                        .to_string();
                    headers = headers.insert(Header {
                        key: DELIVER,
                        value: Some(deliver_at.as_bytes()),
                    });
                    delay_topic(&delivery.queue)
                } else {
                    topic(&delivery.queue)
                };
                self.publish(&topic, payload(&delivery.record), headers)
                    .await?;
                self.commit(&delivery.record).await?;
                Ok(Conclusion::Retried)
            }
            Outcome::Retry | Outcome::Fail => {
                if let Some(target) = &policy.dead_letter {
                    self.ensure(target).await?;
                    let headers = headers(
                        delivery.id,
                        delivery.attempts,
                        delivery.content.as_deref(),
                        delivery.partition.as_deref(),
                        delivery.dedup.as_deref(),
                    );
                    self.publish(&topic(target), payload(&delivery.record), headers)
                        .await?;
                    self.commit(&delivery.record).await?;
                    Ok(Conclusion::DeadLettered)
                } else {
                    self.commit(&delivery.record).await?;
                    Ok(Conclusion::Dropped)
                }
            }
        }
    }

    pub async fn stats(&self, queue: &str) -> Result<Stats, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        self.ensure(queue).await?;
        let ready = self.depth(&topic(queue)).await?;
        let delayed = self.depth(&delay_topic(queue)).await?;
        let leased = self
            .leases
            .lock()
            .await
            .values()
            .filter(|delivery| delivery.queue == queue)
            .count();
        Ok(Stats {
            ready,
            delayed,
            leased,
            oldest_ready_at: None,
            paused: self.paused(queue).await?,
        })
    }

    pub async fn pause(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        if self.paused(queue).await? {
            return Ok(false);
        }
        self.create_topics(&[pause_topic(queue)]).await?;
        Ok(true)
    }

    pub async fn resume(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        if !self.paused(queue).await? {
            return Ok(false);
        }
        self.delete_topics(&[pause_topic(queue)]).await?;
        Ok(true)
    }

    pub async fn purge(&self, queue: &str) -> Result<Purge, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let queued = self
            .depth(&topic(queue))
            .await?
            .saturating_add(self.depth(&delay_topic(queue)).await?);
        self.delete_topics(&[topic(queue), delay_topic(queue)])
            .await?;
        self.create_topics(&[topic(queue), delay_topic(queue)])
            .await?;
        self.main.lock().await.remove(&clean(queue));
        self.delay.lock().await.remove(&clean(queue));
        let before = self.leases.lock().await.len();
        self.leases
            .lock()
            .await
            .retain(|_, delivery| delivery.queue != queue);
        let leased = before - self.leases.lock().await.len();
        Ok(Purge { queued, leased })
    }

    pub async fn redrive(
        &self,
        source: &str,
        target: &str,
        now: DateTime<Utc>,
    ) -> Result<usize, QueueError> {
        if source.is_empty() || target.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        self.ensure(target).await?;
        let mut moved = 0;
        loop {
            let mut leases = self.lease(source, 100, now).await?;
            if leases.is_empty() {
                break;
            }
            for lease in leases.drain(..) {
                let delivery = self
                    .leases
                    .lock()
                    .await
                    .remove(&lease.id)
                    .ok_or(QueueError::UnknownLease)?;
                self.publish(
                    &topic(target),
                    payload(&delivery.record),
                    headers(
                        delivery.id,
                        delivery.attempts,
                        delivery.content.as_deref(),
                        delivery.partition.as_deref(),
                        delivery.dedup.as_deref(),
                    ),
                )
                .await?;
                self.commit(&delivery.record).await?;
                moved += 1;
            }
        }
        Ok(moved)
    }

    async fn pump_delay(&self, queue: &str, now: DateTime<Utc>) -> Result<(), QueueError> {
        let consumer = self.delay(queue).await?;
        loop {
            let received =
                tokio::time::timeout(StdDuration::from_millis(50), consumer.recv()).await;
            let Ok(result) = received else {
                break;
            };
            let borrowed = match result {
                Ok(message) => message,
                Err(error) if transient(&error.to_string()) => break,
                Err(error) => return Err(QueueError::Broker(error.to_string())),
            };
            let record = borrowed.detach();
            let Some(deliver_at) = header_i64(record.headers(), DELIVER) else {
                self.commit(&record).await?;
                continue;
            };
            if deliver_at > now.timestamp_millis() {
                break;
            }
            self.publish(
                &topic(queue),
                payload(&record),
                clone_headers(record.headers(), None),
            )
            .await?;
            self.commit(&record).await?;
        }
        Ok(())
    }

    async fn ensure(&self, queue: &str) -> Result<(), QueueError> {
        self.create_topics(&[topic(queue), delay_topic(queue)])
            .await
    }

    async fn create_topics(&self, names: &[String]) -> Result<(), QueueError> {
        let admin: AdminClient<DefaultClientContext> = config(&self.servers)
            .create()
            .map_err(|error| QueueError::Connect(error.to_string()))?;
        let topics = names
            .iter()
            .map(|name| NewTopic::new(name, 1, TopicReplication::Fixed(1)))
            .collect::<Vec<_>>();
        let results = admin
            .create_topics(&topics, &AdminOptions::new())
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        for result in results {
            if let Err((name, code)) = result
                && code != rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists
            {
                return Err(QueueError::Broker(format!(
                    "failed to create topic {name}: {code}"
                )));
            }
        }
        Ok(())
    }

    async fn delete_topics(&self, names: &[String]) -> Result<(), QueueError> {
        let admin: AdminClient<DefaultClientContext> = config(&self.servers)
            .create()
            .map_err(|error| QueueError::Connect(error.to_string()))?;
        let refs = names.iter().map(String::as_str).collect::<Vec<_>>();
        let results = admin
            .delete_topics(&refs, &AdminOptions::new())
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        for result in results {
            if let Err((name, code)) = result
                && code != rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition
            {
                return Err(QueueError::Broker(format!(
                    "failed to delete topic {name}: {code}"
                )));
            }
        }
        Ok(())
    }

    async fn paused(&self, queue: &str) -> Result<bool, QueueError> {
        let producer = self.producer().await?;
        let metadata = producer
            .client()
            .fetch_metadata(
                Some(&pause_topic(queue)),
                Timeout::After(StdDuration::from_secs(2)),
            )
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        Ok(metadata
            .topics()
            .iter()
            .any(|topic| topic.name() == pause_topic(queue) && topic.error().is_none()))
    }

    async fn depth(&self, name: &str) -> Result<usize, QueueError> {
        let producer = self.producer().await?;
        match producer
            .client()
            .fetch_watermarks(name, 0, Timeout::After(StdDuration::from_secs(2)))
        {
            Ok((low, high)) => Ok(usize::try_from(high.saturating_sub(low)).unwrap_or(usize::MAX)),
            Err(error) if error.to_string().contains("UnknownTopicOrPartition") => Ok(0),
            Err(error) => Err(QueueError::Broker(error.to_string())),
        }
    }

    async fn producer(&self) -> Result<FutureProducer, QueueError> {
        self.producer
            .get_or_try_init(|| async {
                config(&self.servers)
                    .create()
                    .map_err(|error| QueueError::Connect(error.to_string()))
            })
            .await
            .cloned()
    }

    async fn main(&self, queue: &str) -> Result<Arc<StreamConsumer>, QueueError> {
        self.consumer(queue, false).await
    }

    async fn delay(&self, queue: &str) -> Result<Arc<StreamConsumer>, QueueError> {
        self.consumer(queue, true).await
    }

    async fn consumer(&self, queue: &str, delay: bool) -> Result<Arc<StreamConsumer>, QueueError> {
        let key = clean(queue);
        let cache = if delay { &self.delay } else { &self.main };
        if let Some(consumer) = cache.lock().await.get(&key).cloned() {
            return Ok(consumer);
        }

        let name = if delay {
            delay_topic(queue)
        } else {
            topic(queue)
        };
        let group = if delay {
            format!("peren-delay-{key}")
        } else {
            format!("peren-{key}")
        };
        let consumer = Arc::new(consumer(&self.servers, &group, &name)?);
        cache.lock().await.insert(key, Arc::clone(&consumer));
        Ok(consumer)
    }

    async fn publish(
        &self,
        topic: &str,
        body: Vec<u8>,
        headers: OwnedHeaders,
    ) -> Result<(), QueueError> {
        let producer = self.producer().await?;
        let record: FutureRecord<'_, (), _> = FutureRecord::to(topic)
            .payload(body.as_slice())
            .headers(headers);
        producer
            .flush(Timeout::After(StdDuration::from_secs(1)))
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        producer
            .send(record, Timeout::After(StdDuration::from_secs(10)))
            .await
            .map_err(|(error, _)| QueueError::Publish {
                queue: topic.to_string(),
                reason: error.to_string(),
            })?;
        Ok(())
    }

    async fn commit(&self, record: &OwnedMessage) -> Result<(), QueueError> {
        let delayed = std::path::Path::new(record.topic())
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("delay"));
        let consumer = if delayed {
            self.delay(&queue_from_topic(record.topic(), true)).await?
        } else {
            self.main(&queue_from_topic(record.topic(), false)).await?
        };
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(
                record.topic(),
                record.partition(),
                Offset::Offset(record.offset() + 1),
            )
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(|error| QueueError::Broker(error.to_string()))
    }
}

struct Parsed {
    message: Message,
    delivery: Delivery,
}

fn parse(queue: &str, record: OwnedMessage, now: DateTime<Utc>) -> Parsed {
    let id = header_uuid(record.headers(), ID).unwrap_or_else(Uuid::new_v4);
    let attempts = header_u16(record.headers(), ATTEMPTS).unwrap_or(0);
    let content = header_string(record.headers(), CONTENT);
    let partition = header_string(record.headers(), PARTITION);
    let dedup = header_string(record.headers(), DEDUP);
    let body = payload(&record);
    let message = Message {
        id,
        created_at: now,
        queue: queue.to_string(),
        body: body.clone(),
        content_type: content.clone(),
        partition: partition.clone(),
        attempts,
        generation: u64::from(attempts).saturating_add(1),
        available_at: now,
    };
    Parsed {
        message,
        delivery: Delivery {
            record,
            queue: queue.to_string(),
            id,
            attempts,
            content,
            partition,
            dedup,
        },
    }
}

fn consumer(servers: &str, group: &str, topic: &str) -> Result<StreamConsumer, QueueError> {
    let consumer: StreamConsumer = config(servers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .create()
        .map_err(|error| QueueError::Connect(error.to_string()))?;
    consumer
        .fetch_metadata(Some(topic), Timeout::After(StdDuration::from_secs(10)))
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    consumer
        .assign(&assignment)
        .map_err(|error| QueueError::Broker(error.to_string()))?;
    Ok(consumer)
}

fn transient(error: &str) -> bool {
    error.contains("NotCoordinator")
        || error.contains("Broker: Not coordinator")
        || error.contains("Coordinator load in progress")
}

fn config(servers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", servers);
    config.set("allow.auto.create.topics", "false");
    config
}

fn headers(
    id: Uuid,
    attempts: u16,
    content: Option<&str>,
    partition: Option<&str>,
    dedup: Option<&str>,
) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new()
        .insert(Header {
            key: ID,
            value: Some(id.to_string().as_bytes()),
        })
        .insert(Header {
            key: ATTEMPTS,
            value: Some(attempts.to_string().as_bytes()),
        });
    if let Some(value) = content {
        headers = headers.insert(Header {
            key: CONTENT,
            value: Some(value.as_bytes()),
        });
    }
    if let Some(value) = partition {
        headers = headers.insert(Header {
            key: PARTITION,
            value: Some(value.as_bytes()),
        });
    }
    if let Some(value) = dedup {
        headers = headers.insert(Header {
            key: DEDUP,
            value: Some(value.as_bytes()),
        });
    }
    headers
}

fn clone_headers(source: Option<&OwnedHeaders>, skip: Option<&str>) -> OwnedHeaders {
    let mut target = OwnedHeaders::new();
    if let Some(source) = source {
        for index in 0..source.count() {
            let header = source.get(index);
            if Some(header.key) != skip {
                target = target.insert(Header {
                    key: header.key,
                    value: header.value,
                });
            }
        }
    }
    target
}

fn payload(record: &OwnedMessage) -> Vec<u8> {
    record.payload().unwrap_or_default().to_vec()
}

fn header_string(headers: Option<&OwnedHeaders>, name: &str) -> Option<String> {
    headers.and_then(|headers| {
        (0..headers.count()).find_map(|index| {
            let header = headers.get(index);
            (header.key == name)
                .then_some(header.value)
                .flatten()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .map(ToString::to_string)
        })
    })
}

fn header_uuid(headers: Option<&OwnedHeaders>, name: &str) -> Option<Uuid> {
    header_string(headers, name).and_then(|value| Uuid::parse_str(&value).ok())
}
fn header_u16(headers: Option<&OwnedHeaders>, name: &str) -> Option<u16> {
    header_string(headers, name).and_then(|value| value.parse().ok())
}
fn header_i64(headers: Option<&OwnedHeaders>, name: &str) -> Option<i64> {
    header_string(headers, name).and_then(|value| value.parse().ok())
}

fn topic(queue: &str) -> String {
    format!("peren.queue.{}", clean(queue))
}
fn delay_topic(queue: &str) -> String {
    format!("peren.queue.{}.delay", clean(queue))
}
fn pause_topic(queue: &str) -> String {
    format!("peren.queue.{}.pause", clean(queue))
}
fn clean(queue: &str) -> String {
    queue
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
fn queue_from_topic(topic: &str, delay: bool) -> String {
    topic
        .strip_prefix("peren.queue.")
        .unwrap_or(topic)
        .strip_suffix(if delay { ".delay" } else { "" })
        .unwrap_or(topic)
        .to_string()
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

    async fn wait_lease(broker: &KafkaBroker, queue: &str) -> Lease {
        let deadline = tokio::time::Instant::now() + StdDuration::from_mins(1);
        loop {
            let mut leases = broker.lease(queue, 1, Utc::now()).await.unwrap();
            if let Some(lease) = leases.pop() {
                return lease;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for kafka lease"
            );
            tokio::time::sleep(StdDuration::from_millis(100)).await;
        }
    }

    async fn prove_ack_retry_and_dead_letter(
        broker: &KafkaBroker,
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
        tokio::time::sleep(StdDuration::from_millis(150)).await;
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

    async fn prove_pause_resume_redrive(
        broker: &KafkaBroker,
        queue: &str,
        dead: &str,
        policy: &Policy,
    ) {
        assert!(broker.pause(queue).await.unwrap());
        assert!(!broker.pause(queue).await.unwrap());
        broker
            .send(send(queue, b"paused"), Utc::now())
            .await
            .unwrap();
        assert!(broker.lease(queue, 1, Utc::now()).await.unwrap().is_empty());
        assert!(broker.stats(queue).await.unwrap().paused);
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
        assert!(broker.redrive(dead, queue, Utc::now()).await.unwrap() >= 1);
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

    async fn prove_purge(broker: &KafkaBroker, queue: &str) {
        broker
            .send(send(queue, b"purge-a"), Utc::now())
            .await
            .unwrap();
        broker
            .send(send(queue, b"purge-b"), Utc::now())
            .await
            .unwrap();
        tokio::time::sleep(StdDuration::from_millis(150)).await;
        let purged = broker.purge(queue).await.unwrap();
        assert!(purged.queued >= 2);
    }

    #[tokio::test]
    #[ignore = "requires PEREN_KAFKA_BOOTSTRAP and a live Kafka broker"]
    async fn live_kafka_owns_queue_lifecycle() {
        let servers =
            std::env::var("PEREN_KAFKA_BOOTSTRAP").expect("PEREN_KAFKA_BOOTSTRAP is required");
        let broker = KafkaBroker::new(servers);
        let suffix = Uuid::new_v4().simple().to_string();
        let queue = format!("jobs_kafka_live_{suffix}");
        let dead = format!("dead_kafka_live_{suffix}");
        let policy = Policy {
            max_retries: 1,
            retry_delay: Duration::milliseconds(100),
            dead_letter: Some(dead.clone()),
        };

        prove_ack_retry_and_dead_letter(&broker, &queue, &dead, &policy).await;
        prove_pause_resume_redrive(&broker, &queue, &dead, &policy).await;
        prove_purge(&broker, &queue).await;
    }
}
