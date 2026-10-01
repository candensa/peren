use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Deploy {
    #[serde(default = "yes")]
    pub enable_preview: bool,
    #[serde(default = "deploy_poll")]
    pub poll_interval_secs: u64,
    #[serde(default = "resident_age")]
    pub max_resident_age_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Logpush {
    pub endpoint: String,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default = "info")]
    pub min_level: String,
    #[serde(default = "buffer")]
    pub buffer_capacity: usize,
    #[serde(default = "batch")]
    pub batch_max_events: usize,
    #[serde(default = "flush")]
    pub flush_interval_secs: u64,
    #[serde(default = "retries")]
    pub max_retries: u32,
    #[serde(default = "timeout")]
    pub request_timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chronicle {
    pub endpoint: String,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default = "log")]
    pub min_level: String,
    #[serde(default = "buffer")]
    pub buffer_capacity: usize,
    #[serde(default = "batch")]
    pub batch_max_events: usize,
    #[serde(default = "flush")]
    pub flush_interval_secs: u64,
    #[serde(default = "retries")]
    pub max_retries: u32,
    #[serde(default = "timeout")]
    pub request_timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Logging {
    #[serde(default = "trace")]
    pub internal_level: String,
    #[serde(default = "trace")]
    pub worker_console_level: String,
    #[serde(default)]
    pub trace_ownership_hot_path: bool,
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            internal_level: trace(),
            worker_console_level: trace(),
            trace_ownership_hot_path: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Tracing {
    #[serde(default = "sampling")]
    pub sampling_ratio: f64,
}

impl Default for Tracing {
    fn default() -> Self {
        Self {
            sampling_ratio: sampling(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Otlp {
    pub endpoint: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "service_name")]
    pub service_name: String,
    #[serde(default = "otlp_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "batch")]
    pub batch_max_spans: usize,
    #[serde(default = "flush")]
    pub flush_interval_secs: u64,
    #[serde(default = "retries")]
    pub max_retries: u32,
    #[serde(default = "retry_delay")]
    pub retry_base_delay_ms: u64,
    #[serde(default = "timeout")]
    pub request_timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Containers {
    #[serde(default)]
    pub docker_socket: Option<String>,
    #[serde(default = "container_limit")]
    pub max_instances_per_node: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct TimeTravel {
    #[serde(default = "history_days")]
    pub retention_days: u64,
}

impl Default for TimeTravel {
    fn default() -> Self {
        Self {
            retention_days: history_days(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Workflow {
    #[serde(default)]
    pub retention_days: u64,
    #[serde(default = "workflow_sweep")]
    pub sweep_interval_secs: u64,
    #[serde(default = "wakeup_sweep")]
    pub wakeup_sweep_interval_secs: u64,
}

impl Default for Workflow {
    fn default() -> Self {
        Self {
            retention_days: 0,
            sweep_interval_secs: workflow_sweep(),
            wakeup_sweep_interval_secs: wakeup_sweep(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Tenant {
    pub id: String,
    pub cell_quota: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Project {
    pub id: String,
    pub tenant_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Assets {
    pub directory: PathBuf,
    #[serde(default)]
    pub run_worker_first: Option<RunWorkerFirst>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RunWorkerFirst {
    Always(bool),
    Patterns(Vec<String>),
}

const fn yes() -> bool {
    true
}
const fn deploy_poll() -> u64 {
    300
}
const fn resident_age() -> u64 {
    30
}
fn info() -> String {
    "info".into()
}
fn log() -> String {
    "log".into()
}
fn trace() -> String {
    "trace".into()
}
const fn buffer() -> usize {
    10_000
}
const fn batch() -> usize {
    500
}
const fn flush() -> u64 {
    5
}
const fn retries() -> u32 {
    5
}
const fn timeout() -> u64 {
    10
}
const fn sampling() -> f64 {
    1.0
}
fn service_name() -> String {
    "peren-server".into()
}
const fn otlp_capacity() -> usize {
    8_192
}
const fn retry_delay() -> u64 {
    500
}
const fn container_limit() -> usize {
    32
}
const fn history_days() -> u64 {
    30
}
const fn workflow_sweep() -> u64 {
    3_600
}
const fn wakeup_sweep() -> u64 {
    15
}
