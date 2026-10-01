use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Queues {
    #[serde(default)]
    pub broker: QueueBroker,
    #[serde(default)]
    pub nats_url: Option<String>,
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default)]
    pub cell_path: Option<String>,
    #[serde(default)]
    pub amqp_url: Option<String>,
    #[serde(default)]
    pub kafka_bootstrap_servers: Option<String>,
    #[serde(default)]
    pub consumer_defaults: QueueDefaults,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueBroker {
    #[default]
    Memory,
    File,
    Cell,
    Nats,
    #[serde(rename = "rabbitmq")]
    RabbitMq,
    Kafka,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum QueueConsumer {
    Name(String),
    Settings(QueueSettings),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueueSettings {
    pub queue: String,
    #[serde(flatten)]
    pub overrides: QueueOverrides,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct QueueOverrides {
    #[serde(default)]
    pub max_batch_size: Option<u16>,
    #[serde(default)]
    pub max_batch_timeout_secs: Option<u64>,
    #[serde(default)]
    pub max_retries: Option<u16>,
    #[serde(default)]
    pub max_concurrency: Option<u16>,
    #[serde(default)]
    pub dead_letter_queue: Option<String>,
    #[serde(default)]
    pub retry_delay_secs: Option<u64>,
}

impl QueueSettings {
    #[must_use]
    pub fn resolve(&self, defaults: &QueueDefaults) -> QueueDefaults {
        QueueDefaults {
            max_batch_size: self
                .overrides
                .max_batch_size
                .unwrap_or(defaults.max_batch_size),
            max_batch_timeout_secs: self
                .overrides
                .max_batch_timeout_secs
                .unwrap_or(defaults.max_batch_timeout_secs),
            max_retries: self.overrides.max_retries.unwrap_or(defaults.max_retries),
            max_concurrency: self
                .overrides
                .max_concurrency
                .unwrap_or(defaults.max_concurrency),
            dead_letter_queue: self
                .overrides
                .dead_letter_queue
                .clone()
                .or_else(|| defaults.dead_letter_queue.clone()),
            retry_delay_secs: self
                .overrides
                .retry_delay_secs
                .unwrap_or(defaults.retry_delay_secs),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QueueDefaults {
    #[serde(default = "batch_size")]
    pub max_batch_size: u16,
    #[serde(default = "batch_timeout")]
    pub max_batch_timeout_secs: u64,
    #[serde(default = "retries")]
    pub max_retries: u16,
    #[serde(default = "one")]
    pub max_concurrency: u16,
    #[serde(default)]
    pub dead_letter_queue: Option<String>,
    #[serde(default)]
    pub retry_delay_secs: u64,
}

impl Default for QueueDefaults {
    fn default() -> Self {
        Self {
            max_batch_size: batch_size(),
            max_batch_timeout_secs: batch_timeout(),
            max_retries: retries(),
            max_concurrency: one(),
            dead_letter_queue: None,
            retry_delay_secs: 0,
        }
    }
}

const fn batch_size() -> u16 {
    10
}
const fn batch_timeout() -> u64 {
    5
}
const fn retries() -> u16 {
    3
}
const fn one() -> u16 {
    1
}
