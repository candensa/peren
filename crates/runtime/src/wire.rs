use std::collections::BTreeMap;

use crate::HttpRequest;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListOptions {
    pub prefix: Option<Vec<u8>>,
    pub cursor: Option<Vec<u8>>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListEntry {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPage {
    pub keys: Vec<ListEntry>,
    pub cursor: Option<Vec<u8>>,
    pub list_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KvGet {
    pub namespace: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KvPut {
    pub namespace: String,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KvList {
    pub namespace: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SqlQuery {
    pub database: Option<String>,
    pub sql: String,
    pub parameters: Vec<SqlValue>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub changes: u64,
    pub last_insert_rowid: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ScheduledEvent {
    #[serde(rename = "scheduledTime")]
    pub scheduled_time_ms: i64,
    pub cron: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AwsSigv4Fetch {
    pub binding: String,
    pub region: String,
    pub service: String,
    pub allowed_hosts: Vec<String>,
    pub request: HttpRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2Put {
    pub bucket: String,
    pub key: String,
    pub body: Vec<u8>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub custom_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2Get {
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2Delete {
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2List {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2Object {
    pub key: String,
    pub body: Vec<u8>,
    pub size: usize,
    pub content_type: Option<String>,
    pub custom_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheGet {
    pub cache: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CachePut {
    pub cache: String,
    pub key: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheEntry {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiRun {
    pub command: String,
    pub model: String,
    pub input: serde_json::Value,
    #[serde(default)]
    pub options: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2ListPage {
    pub objects: Vec<R2ObjectEntry>,
    pub cursor: Option<String>,
    pub list_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct R2ObjectEntry {
    pub key: String,
    pub size: usize,
    #[serde(default)]
    pub custom_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceFetch {
    pub service: String,
    pub request: HttpRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableObjectFetch {
    pub namespace: String,
    pub id: String,
    pub name: Option<String>,
    pub class_name: String,
    pub request: HttpRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSend {
    pub queue: String,
    #[serde(serialize_with = "bytes_body", deserialize_with = "bytes_input")]
    pub body: Vec<u8>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub partition: Option<String>,
    #[serde(default)]
    pub delay_seconds: Option<u32>,
    #[serde(default)]
    pub dedup_id: Option<String>,
}

fn bytes_body<S>(body: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    body.serialize(serializer)
}

fn bytes_input<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<u8>::deserialize(deserializer)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueueEvent {
    pub queue: String,
    pub messages: Vec<QueueMessage>,
    pub metrics: QueueMetrics,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueMetrics {
    pub ready: usize,
    pub delayed: usize,
    pub leased: usize,
    pub oldest_ready_timestamp: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueueMessage {
    pub id: String,
    #[serde(serialize_with = "string_body")]
    pub body: Vec<u8>,
    pub attempts: u32,
    pub timestamp: i64,
}

fn string_body<S>(body: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&String::from_utf8_lossy(body))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueDispatch {
    pub dispositions: Vec<QueueDisposition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueDisposition {
    pub id: String,
    pub outcome: QueueDispositionKind,
    #[serde(default)]
    pub delay_seconds: Option<u32>,
    #[serde(default)]
    pub dedup_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueueDispositionKind {
    Ack,
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketMessageEvent {
    pub id: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketCloseEvent {
    pub id: String,
    pub code: u16,
    pub reason: String,
    pub was_clean: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketDispatch {
    pub outbound: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowEvent {
    pub instance: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowActivityEvent {
    pub instance: String,
    pub name: String,
    pub task: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerLogEvent {
    pub level: WorkerLogLevel,
    pub message: String,
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TailEvent {
    pub events: Vec<TailRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TailRecord {
    pub outcome: String,
    pub script: String,
    #[serde(rename = "wallTimeMs")]
    pub wall_time_ms: u64,
}
