use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration as StdDuration};

use async_nats::{
    HeaderMap, HeaderValue,
    jetstream::{self, AckKind, consumer::PullConsumer, stream},
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::{Mutex, OnceCell};
use uuid::Uuid;

use crate::{Conclusion, Lease, Message, Outcome, Policy, Purge, QueueError, Send, Stats};

const ID: &str = "Peren-Message-Id";
const CONTENT: &str = "Peren-Content-Type";
const PARTITION: &str = "Peren-Partition";
const DEDUP: &str = "Peren-Dedup-Id";

#[derive(Clone)]
pub struct NatsBroker {
    url: Arc<str>,
    leases: Arc<Mutex<BTreeMap<Uuid, async_nats::jetstream::Message>>>,
    context: Arc<OnceCell<jetstream::Context>>,
}

impl NatsBroker {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: Arc::from(url.into()),
            leases: Arc::default(),
            context: Arc::default(),
        }
    }

    pub async fn send(&self, command: Send, now: DateTime<Utc>) -> Result<Uuid, QueueError> {
        if command.queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let id = Uuid::new_v4();
        let context = self.context().await?;
        self.ensure(&context, &command.queue)
            .await
            .map_err(|error| QueueError::Broker(format!("nats ensure queue: {error}")))?;
        let mut headers = HeaderMap::new();
        headers.insert(
            ID,
            HeaderValue::from_str(&id.to_string())
                .map_err(|error| QueueError::Broker(error.to_string()))?,
        );
        if let Some(content) = command.content_type.as_deref() {
            headers.insert(
                CONTENT,
                HeaderValue::from_str(content)
                    .map_err(|error| QueueError::Broker(error.to_string()))?,
            );
        }
        if let Some(partition) = command.partition.as_deref() {
            headers.insert(
                PARTITION,
                HeaderValue::from_str(partition)
                    .map_err(|error| QueueError::Broker(error.to_string()))?,
            );
        }
        if let Some(dedup) = command.dedup_id.as_deref() {
            headers.insert(
                DEDUP,
                HeaderValue::from_str(dedup)
                    .map_err(|error| QueueError::Broker(error.to_string()))?,
            );
        }
        if command.delay > chrono::Duration::zero() {
            let available_at = now
                .checked_add_signed(command.delay)
                .ok_or(QueueError::InvalidDelay)?;
            headers.insert(
                "Nats-Delay",
                HeaderValue::from_str(&available_at.timestamp_millis().to_string())
                    .map_err(|error| QueueError::Broker(error.to_string()))?,
            );
        }
        let subject = subject(&command.queue);
        let ack = context
            .publish_with_headers(subject.clone(), headers, command.body.into())
            .await
            .map_err(|error| QueueError::Publish {
                queue: command.queue.clone(),
                reason: format!("nats publish request: {error}"),
            })?;
        ack.await.map_err(|error| QueueError::Publish {
            queue: command.queue,
            reason: format!("nats publish ack: {error}"),
        })?;
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
        let context = self.context().await?;
        if self.paused(&context, queue).await? {
            return Ok(Vec::new());
        }
        let consumer = self.ensure(&context, queue).await?;
        let batch = consumer
            .fetch()
            .max_messages(limit)
            .messages()
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        tokio::pin!(batch);
        let mut leases = Vec::new();
        while let Some(next) = batch.next().await {
            let msg = next.map_err(|error| QueueError::Broker(error.to_string()))?;
            let Some(lease) = self.convert(queue, now, msg).await? else {
                continue;
            };
            leases.push(lease);
            if leases.len() == limit {
                break;
            }
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
        let msg = self
            .leases
            .lock()
            .await
            .remove(&lease)
            .ok_or(QueueError::UnknownLease)?;
        match outcome {
            Outcome::Ack => {
                msg.ack()
                    .await
                    .map_err(|error| QueueError::Broker(error.to_string()))?;
                Ok(Conclusion::Acked)
            }
            Outcome::Retry => {
                let attempts = delivered(&msg);
                if attempts <= policy.max_retries {
                    let delay = retry_delay
                        .unwrap_or(policy.retry_delay)
                        .to_std()
                        .map_err(|_| QueueError::InvalidDelay)?;
                    msg.ack_with(AckKind::Nak(Some(delay)))
                        .await
                        .map_err(|error| QueueError::Broker(error.to_string()))?;
                    Ok(Conclusion::Retried)
                } else {
                    self.dead_letter_or_drop(msg, policy, now).await
                }
            }
            Outcome::Fail => self.dead_letter_or_drop(msg, policy, now).await,
        }
    }

    pub async fn stats(&self, queue: &str) -> Result<Stats, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let context = self.context().await?;
        let mut stream = self.ensure_stream(&context, queue).await?;
        let mut ready = 0;
        let mut delayed = 0;
        let now = Utc::now().timestamp_millis();
        if let Ok(mut subjects) = stream.info_with_subjects(subject(queue)).await {
            while let Some(next) = subjects.next().await {
                let (_, count) = next.map_err(|error| QueueError::Broker(error.to_string()))?;
                ready += count;
            }
        } else {
            ready = usize::try_from(
                stream
                    .info()
                    .await
                    .map_err(|error| QueueError::Broker(error.to_string()))?
                    .state
                    .messages,
            )
            .unwrap_or(usize::MAX);
        }
        // JetStream stores delayed messages in the same stream; classify known future-delay
        // messages conservatively through delivery-time headers only when they are leased.
        let leased = self
            .leases
            .lock()
            .await
            .values()
            .filter(|msg| {
                msg.subject.as_ref() == subject(queue) && !future_delayed(msg, now).unwrap_or(false)
            })
            .count();
        delayed += self
            .leases
            .lock()
            .await
            .values()
            .filter(|msg| {
                msg.subject.as_ref() == subject(queue) && future_delayed(msg, now).unwrap_or(false)
            })
            .count();
        Ok(Stats {
            ready,
            delayed,
            leased,
            oldest_ready_at: None,
            paused: self.paused(&context, queue).await?,
        })
    }

    pub async fn pause(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let context = self.context().await?;
        self.ensure_control(&context).await?;
        if self.paused(&context, queue).await? {
            return Ok(false);
        }
        let ack = context
            .publish(pause_subject(queue), b"paused".as_slice().into())
            .await
            .map_err(|error| QueueError::Publish {
                queue: queue.to_string(),
                reason: error.to_string(),
            })?;
        ack.await.map_err(|error| QueueError::Publish {
            queue: queue.to_string(),
            reason: error.to_string(),
        })?;
        Ok(true)
    }

    pub async fn resume(&self, queue: &str) -> Result<bool, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let context = self.context().await?;
        let stream = self.ensure_control(&context).await?;
        let response = stream
            .purge()
            .filter(pause_subject(queue))
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        Ok(response.purged > 0)
    }

    pub async fn purge(&self, queue: &str) -> Result<Purge, QueueError> {
        if queue.is_empty() {
            return Err(QueueError::EmptyQueue);
        }
        let context = self.context().await?;
        let stream = self.ensure_stream(&context, queue).await?;
        let response = stream
            .purge()
            .filter(subject(queue))
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        let before = self.leases.lock().await.len();
        self.leases
            .lock()
            .await
            .retain(|_, msg| msg.subject.as_ref() != subject(queue));
        let leased = before - self.leases.lock().await.len();
        Ok(Purge {
            queued: usize::try_from(response.purged).unwrap_or(usize::MAX),
            leased,
        })
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
        let mut moved = 0;
        loop {
            let mut leases = self.lease(source, 100, now).await?;
            if leases.is_empty() {
                break;
            }
            for lease in leases.drain(..) {
                let msg = self
                    .leases
                    .lock()
                    .await
                    .remove(&lease.id)
                    .ok_or(QueueError::UnknownLease)?;
                let context = self.context().await?;
                self.ensure(&context, target).await?;
                let ack = context
                    .publish_with_headers(
                        subject(target),
                        msg.headers.clone().unwrap_or_default(),
                        msg.payload.clone(),
                    )
                    .await
                    .map_err(|error| QueueError::Publish {
                        queue: target.to_string(),
                        reason: error.to_string(),
                    })?;
                ack.await.map_err(|error| QueueError::Publish {
                    queue: target.to_string(),
                    reason: error.to_string(),
                })?;
                msg.ack()
                    .await
                    .map_err(|error| QueueError::Broker(error.to_string()))?;
                moved += 1;
            }
        }
        Ok(moved)
    }

    async fn dead_letter_or_drop(
        &self,
        msg: async_nats::jetstream::Message,
        policy: &Policy,
        _now: DateTime<Utc>,
    ) -> Result<Conclusion, QueueError> {
        let Some(queue) = &policy.dead_letter else {
            msg.ack()
                .await
                .map_err(|error| QueueError::Broker(error.to_string()))?;
            return Ok(Conclusion::Dropped);
        };
        let context = self.context().await?;
        self.ensure(&context, queue).await?;
        let headers = msg.headers.clone().unwrap_or_default();
        let ack = context
            .publish_with_headers(subject(queue), headers, msg.payload.clone())
            .await
            .map_err(|error| QueueError::Publish {
                queue: queue.clone(),
                reason: error.to_string(),
            })?;
        ack.await.map_err(|error| QueueError::Publish {
            queue: queue.clone(),
            reason: error.to_string(),
        })?;
        msg.ack()
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        Ok(Conclusion::DeadLettered)
    }

    async fn convert(
        &self,
        queue: &str,
        now: DateTime<Utc>,
        msg: async_nats::jetstream::Message,
    ) -> Result<Option<Lease>, QueueError> {
        if let Some(deliver_at) = msg
            .headers
            .as_ref()
            .and_then(|headers| headers.get("Nats-Delay"))
            .and_then(|value| value.as_str().parse::<i64>().ok())
            && deliver_at > now.timestamp_millis()
        {
            let wait =
                StdDuration::from_millis((deliver_at - now.timestamp_millis()).cast_unsigned());
            msg.ack_with(AckKind::Nak(Some(wait.min(StdDuration::from_mins(1)))))
                .await
                .map_err(|error| QueueError::Broker(error.to_string()))?;
            return Ok(None);
        }
        let id = msg
            .headers
            .as_ref()
            .and_then(|h| h.get(ID))
            .and_then(|v| Uuid::parse_str(v.as_str()).ok())
            .unwrap_or_else(Uuid::new_v4);
        let lease_id = Uuid::new_v4();
        let message = Message {
            id,
            created_at: now,
            queue: queue.to_string(),
            body: msg.payload.to_vec(),
            content_type: msg
                .headers
                .as_ref()
                .and_then(|h| h.get(CONTENT))
                .map(|v| v.as_str().to_string()),
            partition: msg
                .headers
                .as_ref()
                .and_then(|h| h.get(PARTITION))
                .map(|v| v.as_str().to_string()),
            attempts: delivered(&msg),
            generation: u64::from(delivered(&msg)),
            available_at: now,
        };
        self.leases.lock().await.insert(lease_id, msg);
        Ok(Some(Lease {
            id: lease_id,
            generation: message.generation,
            message,
            expires_at: now + chrono::Duration::seconds(30),
        }))
    }

    async fn context(&self) -> Result<jetstream::Context, QueueError> {
        self.context
            .get_or_try_init(|| async {
                let client = async_nats::connect(self.url.as_ref())
                    .await
                    .map_err(|error| QueueError::Connect(error.to_string()))?;
                Ok(jetstream::new(client))
            })
            .await
            .cloned()
    }

    async fn ensure_stream(
        &self,
        context: &jetstream::Context,
        queue: &str,
    ) -> Result<async_nats::jetstream::stream::Stream, QueueError> {
        context
            .get_or_create_stream(stream::Config {
                name: stream_name(queue),
                subjects: vec![subject(queue)],
                max_messages: 1_000_000,
                ..Default::default()
            })
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))
    }

    async fn ensure_control(
        &self,
        context: &jetstream::Context,
    ) -> Result<async_nats::jetstream::stream::Stream, QueueError> {
        context
            .get_or_create_stream(stream::Config {
                name: "PEREN_QUEUE_CONTROL".to_string(),
                subjects: vec!["peren.queue.control.>".to_string()],
                max_messages: 1_000_000,
                allow_direct: true,
                ..Default::default()
            })
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))
    }

    async fn paused(&self, context: &jetstream::Context, queue: &str) -> Result<bool, QueueError> {
        let stream = self.ensure_control(context).await?;
        match stream
            .direct_get_last_for_subject(pause_subject(queue))
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn ensure(
        &self,
        context: &jetstream::Context,
        queue: &str,
    ) -> Result<PullConsumer, QueueError> {
        let stream = self.ensure_stream(context, queue).await?;
        stream
            .get_or_create_consumer(
                "peren",
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some("peren".to_string()),
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    filter_subject: subject(queue),
                    max_deliver: 1_000,
                    max_waiting: 512,
                    max_ack_pending: 4096,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))?;
        stream
            .get_consumer("peren")
            .await
            .map_err(|error| QueueError::Broker(error.to_string()))
    }
}

fn future_delayed(msg: &async_nats::jetstream::Message, now_ms: i64) -> Option<bool> {
    msg.headers
        .as_ref()
        .and_then(|headers| headers.get("Nats-Delay"))
        .and_then(|value| value.as_str().parse::<i64>().ok())
        .map(|deliver_at| deliver_at > now_ms)
}

fn delivered(msg: &async_nats::jetstream::Message) -> u16 {
    msg.info()
        .ok()
        .and_then(|info| u16::try_from(info.delivered).ok())
        .unwrap_or(1)
}

fn subject(queue: &str) -> String {
    format!("peren.queue.{}", sanitize(queue))
}
fn pause_subject(queue: &str) -> String {
    format!("peren.queue.control.{}", sanitize(queue))
}
fn stream_name(queue: &str) -> String {
    format!("PEREN_QUEUE_{}", sanitize(queue).to_uppercase())
}
fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
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

    fn policy() -> Policy {
        Policy {
            max_retries: 1,
            retry_delay: Duration::milliseconds(100),
            dead_letter: Some("dead".to_string()),
        }
    }

    async fn wait_lease(broker: &NatsBroker, queue: &str) -> Lease {
        let deadline = tokio::time::Instant::now() + StdDuration::from_secs(10);
        loop {
            let mut leases = broker.lease(queue, 1, Utc::now()).await.unwrap();
            if let Some(lease) = leases.pop() {
                return lease;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for queue lease"
            );
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }
    }

    async fn prove_ack_retry_and_dead_letter(
        broker: &NatsBroker,
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
        broker: &NatsBroker,
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

    async fn prove_purge(broker: &NatsBroker, queue: &str) {
        broker
            .send(send(queue, b"purge-a"), Utc::now())
            .await
            .unwrap();
        broker
            .send(send(queue, b"purge-b"), Utc::now())
            .await
            .unwrap();
        let purged = broker.purge(queue).await.unwrap();
        assert!(purged.queued >= 2);
    }

    #[tokio::test]
    #[ignore = "requires PEREN_NATS_URL and a live JetStream-enabled NATS server"]
    async fn live_nats_owns_queue_lifecycle() {
        let url = std::env::var("PEREN_NATS_URL").expect("PEREN_NATS_URL is required");
        let broker = NatsBroker::new(url);
        let queue = format!("jobs_{}", Uuid::new_v4().simple());
        let dead = format!("dead_{}", Uuid::new_v4().simple());
        let policy = Policy {
            dead_letter: Some(dead.clone()),
            ..policy()
        };

        prove_ack_retry_and_dead_letter(&broker, &queue, &dead, &policy).await;
        prove_pause_resume_redrive(&broker, &queue, &dead, &policy).await;
        prove_purge(&broker, &queue).await;
    }
}
