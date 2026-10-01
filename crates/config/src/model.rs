use crate::{
    Cache, Chronicle, Containers, Deploy, DispatchNamespace, Logging, Logpush, Otlp, Project,
    Queues, Service, Socket, Tenant, TimeTravel, Tracing, Workflow,
};
use peren_primitives::ServiceName;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FleetConfig {
    #[serde(default)]
    pub seed_peers: Vec<String>,
    pub node: Node,
    pub bucket: Bucket,
    pub mtls: Mtls,
    #[serde(default)]
    pub routing: Routing,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub logging: Logging,
    #[serde(default)]
    pub shutdown: Shutdown,
    #[serde(default)]
    pub rebalance: Rebalance,
    #[serde(default)]
    pub queues: Option<Queues>,
    #[serde(default)]
    pub cache: Cache,
    #[serde(default)]
    pub console: Option<Console>,
    #[serde(default)]
    pub deploy: Option<Deploy>,
    #[serde(default)]
    pub peer_identity_token_ttl_secs: Option<u64>,
    #[serde(default)]
    pub secrets: Secrets,
    #[serde(default)]
    pub secrets_store: BTreeMap<String, String>,
    #[serde(default)]
    pub logpush: Option<Logpush>,
    #[serde(default, rename = "d1_time_travel")]
    pub time_travel: TimeTravel,
    #[serde(default)]
    pub workflow: Workflow,
    #[serde(default)]
    pub tracing: Tracing,
    #[serde(default)]
    pub otlp: Option<Otlp>,
    #[serde(default)]
    pub chronicle_export: Option<Chronicle>,
    #[serde(default)]
    pub containers: Option<Containers>,
    #[serde(default)]
    pub tenants: Vec<Tenant>,
    #[serde(default)]
    pub projects: Vec<Project>,
    #[serde(default)]
    pub dispatch_namespaces: Vec<DispatchNamespace>,
    #[serde(default)]
    pub services: Vec<Service>,
    #[serde(default)]
    pub sockets: Vec<Socket>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Secrets {
    #[serde(default)]
    pub provider: SecretsProvider,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub vault_url: Option<String>,
    #[serde(default)]
    pub credentials_source: CredentialsSource,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretsProvider {
    #[default]
    Local,
    AwsSecretsManager,
    GcpSecretManager,
    AzureKeyVault,
    Vault,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Node {
    #[serde(rename = "node_id")]
    pub id: Uuid,
    pub advertise_addr: String,
    pub listen: String,
    #[serde(default)]
    pub identity_key_path: Option<PathBuf>,
    #[serde(default)]
    pub lease_mode: LeaseMode,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseMode {
    #[default]
    Continuous,
    Lazy,
    Shadow,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Bucket {
    #[serde(default)]
    pub kind: BucketKind,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    #[serde(rename = "bucket")]
    pub name: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub access_key_env: Option<String>,
    #[serde(default)]
    pub secret_key_env: Option<String>,
    #[serde(default)]
    pub azure_account_env: Option<String>,
    #[serde(default)]
    pub azure_access_key_env: Option<String>,
    #[serde(default)]
    pub azure_emulator: bool,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub credentials_source: CredentialsSource,
    #[serde(default)]
    pub path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketKind {
    #[default]
    S3,
    Memory,
    File,
    AzureBlob,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialsSource {
    #[default]
    Configured,
    Environment,
    InstanceRole,
    WorkloadIdentity,
    EksPodIdentity,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Mtls {
    pub ca_cert_path: PathBuf,
    pub leaf_cert_path: PathBuf,
    pub leaf_key_path: PathBuf,
    #[serde(default = "yes")]
    pub require_client_cert: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Routing {
    #[serde(default = "cache_ttl")]
    pub ownership_cache_ttl_secs: u64,
    #[serde(default = "forward_retries")]
    pub max_forward_retries: u32,
    #[serde(default = "gossip_fanout")]
    pub gossip_fanout: usize,
}
impl Default for Routing {
    fn default() -> Self {
        Self {
            ownership_cache_ttl_secs: cache_ttl(),
            max_forward_retries: forward_retries(),
            gossip_fanout: gossip_fanout(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Shutdown {
    #[serde(default = "evacuation_concurrency")]
    pub evacuation_concurrency: usize,
    #[serde(default = "evacuation_deadline")]
    pub evacuation_deadline_secs: u64,
}
impl Default for Shutdown {
    fn default() -> Self {
        Self {
            evacuation_concurrency: evacuation_concurrency(),
            evacuation_deadline_secs: evacuation_deadline(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Rebalance {
    #[serde(default = "rebalance_interval")]
    pub interval_secs: u64,
    #[serde(default)]
    pub placement_weight: Option<u32>,
}
impl Default for Rebalance {
    fn default() -> Self {
        Self {
            interval_secs: rebalance_interval(),
            placement_weight: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Limits {
    #[serde(default = "request_body")]
    #[serde(rename = "max_request_body_bytes")]
    pub request_body_bytes: u64,
    #[serde(default = "heap")]
    #[serde(rename = "max_heap_bytes")]
    pub heap_bytes: u64,
    #[serde(default = "execution")]
    #[serde(rename = "max_execution_time_ms")]
    pub execution_time_ms: u64,
    #[serde(default = "subrequests")]
    #[serde(rename = "max_subrequests_per_invocation")]
    pub subrequests_per_invocation: u32,
    #[serde(default = "isolates", rename = "max_isolates")]
    pub isolates: usize,
    #[serde(default = "fair_share")]
    pub isolate_fair_share_percent: u8,
    #[serde(default = "checkpoint")]
    pub checkpoint_threshold_bytes: u64,
    #[serde(default = "cpu_time", rename = "max_cpu_time_ms")]
    pub cpu_time_ms: u64,
    #[serde(default = "cron_base")]
    pub cron_retry_base_ms: u64,
    #[serde(default = "cron_attempts")]
    pub cron_retry_max_attempts: u32,
    #[serde(default = "cron_backoff")]
    pub cron_retry_max_backoff_ms: u64,
    #[serde(default = "steps", rename = "max_steps_per_instance")]
    pub steps_per_instance: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            request_body_bytes: request_body(),
            heap_bytes: heap(),
            execution_time_ms: execution(),
            subrequests_per_invocation: subrequests(),
            isolates: isolates(),
            isolate_fair_share_percent: fair_share(),
            checkpoint_threshold_bytes: checkpoint(),
            cpu_time_ms: cpu_time(),
            cron_retry_base_ms: cron_base(),
            cron_retry_max_attempts: cron_attempts(),
            cron_retry_max_backoff_ms: cron_backoff(),
            steps_per_instance: steps(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Console {
    pub listen: String,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub backend: Option<ConsoleBackend>,
}

impl Console {
    #[must_use]
    pub fn effective_backend(&self, seed_peers: &[String]) -> ConsoleBackend {
        self.backend.unwrap_or(if seed_peers.is_empty() {
            ConsoleBackend::Sqlite
        } else {
            ConsoleBackend::Bucket
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleBackend {
    Sqlite,
    Bucket,
}

#[derive(Debug)]
pub struct ValidatedConfig {
    pub raw: FleetConfig,
    pub services: BTreeMap<ServiceName, usize>,
    pub advertise_addr: SocketAddr,
    pub peer_listen: SocketAddr,
    pub sockets: BTreeMap<String, SocketAddr>,
    pub console_listen: Option<SocketAddr>,
}

const fn yes() -> bool {
    true
}
const fn cache_ttl() -> u64 {
    45
}
const fn forward_retries() -> u32 {
    4
}
const fn gossip_fanout() -> usize {
    3
}
const fn evacuation_concurrency() -> usize {
    8
}
const fn evacuation_deadline() -> u64 {
    40
}
const fn rebalance_interval() -> u64 {
    5
}
const fn request_body() -> u64 {
    32 * 1024 * 1024
}
const fn heap() -> u64 {
    128 * 1024 * 1024
}
const fn execution() -> u64 {
    30_000
}
const fn subrequests() -> u32 {
    10_000
}
const fn isolates() -> usize {
    256
}
const fn fair_share() -> u8 {
    25
}
const fn checkpoint() -> u64 {
    4 * 1024 * 1024
}
const fn cpu_time() -> u64 {
    30_000
}
const fn cron_base() -> u64 {
    2_000
}
const fn cron_attempts() -> u32 {
    6
}
const fn cron_backoff() -> u64 {
    64_000
}
const fn steps() -> u64 {
    100_000
}
