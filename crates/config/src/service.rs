use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    AiProvider, CredentialsSource, D1Backend, ImageProvider, KvBackend, QueueConsumer,
    VectorProvider,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DispatchNamespace {
    pub name: String,
    #[serde(default)]
    pub scripts: Vec<DispatchScript>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DispatchScript {
    pub name: String,
    pub worker_bundle_path: PathBuf,
    pub compatibility_date: String,
    #[serde(default)]
    pub compatibility_flags: Vec<String>,
    pub cell_quota: u32,
    #[serde(default)]
    pub deployment: DispatchDeployment,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Secret {
    Env(String),
    Ref(SecretRef),
}

impl Secret {
    #[must_use]
    pub fn lookup_key(&self, binding: &str) -> String {
        match self {
            Self::Env(variable) => variable.clone(),
            Self::Ref(reference) => format!("__peren_secret:{binding}:{}", reference.name),
        }
    }

    #[must_use]
    pub fn env_variable(&self) -> Option<&str> {
        match self {
            Self::Env(variable) => Some(variable),
            Self::Ref(SecretRef {
                source: SecretSource::Env,
                name,
            }) => Some(name),
            Self::Ref(SecretRef {
                source: SecretSource::Store,
                ..
            }) => None,
        }
    }

    #[must_use]
    pub fn store_name<'a>(&'a self, binding: &'a str) -> Option<&'a str> {
        match self {
            Self::Env(_) => Some(binding),
            Self::Ref(SecretRef {
                source: SecretSource::Store,
                name,
            }) => Some(name),
            Self::Ref(SecretRef {
                source: SecretSource::Env,
                ..
            }) => None,
        }
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self::Env(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self::Env(value.into())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecretRef {
    pub source: SecretSource,
    pub name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretSource {
    Env,
    Store,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DispatchDeployment {
    #[serde(default)]
    pub requested_unique_key: String,
    #[serde(default)]
    pub kv_namespaces: Vec<String>,
    #[serde(default)]
    pub d1_databases: Vec<String>,
    #[serde(default)]
    pub r2_buckets: Vec<DispatchBucket>,
    #[serde(default)]
    pub durable_object_classes: Vec<String>,
    #[serde(default)]
    pub outbound_hosts: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DispatchBucket {
    pub binding_name: String,
    pub endpoint: String,
    pub bucket: String,
    pub sub_path: String,
    pub credential_scope: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Service {
    pub name: String,
    pub worker_bundle_path: PathBuf,
    pub compatibility_date: String,
    #[serde(default)]
    pub compatibility_flags: Vec<String>,
    #[serde(default)]
    pub entrypoint: Entrypoint,
    #[serde(default)]
    pub cron_triggers: Vec<CronTrigger>,
    #[serde(default)]
    pub tail_consumers: Vec<TailConsumer>,
    #[serde(default)]
    pub consumes_queues: Vec<QueueConsumer>,
    #[serde(default)]
    pub bindings: BTreeMap<String, Binding>,
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    #[serde(default)]
    pub expose_node_id: bool,
    #[serde(default)]
    pub secrets: BTreeMap<String, Secret>,
    #[serde(default)]
    pub secrets_store_refs: BTreeMap<String, String>,
    #[serde(default)]
    pub additional_modules: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub source_maps: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub assets: Option<crate::Assets>,
    #[serde(default)]
    pub isolate_fair_share_percent: Option<u8>,
    #[serde(default)]
    pub checkpoint_threshold_bytes: Option<u64>,
    #[serde(default)]
    pub max_cpu_time_ms: Option<u64>,
    #[serde(default)]
    pub max_subrequests_per_invocation: Option<u32>,
    #[serde(default)]
    pub max_heap_bytes: Option<u64>,
    #[serde(default)]
    pub max_execution_time_ms: Option<u64>,
    #[serde(default)]
    pub workflow_retention_days: Option<u64>,
    #[serde(default)]
    pub deploy_max_resident_age_secs: Option<u64>,
    #[serde(default)]
    pub max_steps_per_instance: Option<u64>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub placement_regions: Vec<String>,
}

impl Service {
    #[must_use]
    pub fn isolation_scope(&self) -> String {
        let mut scope = String::new();
        if let Some(tenant) = self.tenant_id.as_deref() {
            scope.push_str("tenant/");
            scope.push_str(tenant);
            scope.push('/');
        }
        if let Some(project) = self.project_id.as_deref() {
            scope.push_str("project/");
            scope.push_str(project);
            scope.push('/');
        }
        scope.push_str("service/");
        scope.push_str(&self.name);
        scope
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Entrypoint {
    #[default]
    Stateless,
    DurableObject {
        class_name: String,
        unique_key: String,
        id_from: IdSource,
        #[serde(default)]
        container: Option<ContainerClass>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum IdSource {
    Header { name: String },
    FirstPathSegment,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CronTrigger {
    pub expression: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TailConsumer {
    pub service: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContainerClass {
    pub image: String,
    pub default_port: u16,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub memory_mb: Option<u64>,
    #[serde(default)]
    pub cpu_millis: Option<u64>,
    #[serde(default = "container_sleep")]
    pub idle_sleep_secs: u64,
    #[serde(default)]
    pub allow_network_egress: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Binding {
    Service {
        entrypoint: String,
        #[serde(default)]
        props: serde_json::Value,
        #[serde(default)]
        class_name: Option<String>,
    },
    DurableObjectNamespace {
        class_name: String,
        unique_key: String,
    },
    Kv {
        namespace: String,
        unique_key: String,
        #[serde(default)]
        backend: KvBackend,
    },
    #[serde(rename = "d1_database")]
    D1Database {
        database_name: String,
        unique_key: String,
        #[serde(default)]
        backend: D1Backend,
    },
    #[serde(rename = "r2_bucket")]
    R2Bucket {
        endpoint: String,
        bucket: String,
        credential_scope: String,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        access_key_env: Option<String>,
        #[serde(default)]
        secret_key_env: Option<String>,
        #[serde(default)]
        token_env: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        notifications: Vec<R2Notification>,
    },
    Queue {
        queue_name: String,
    },
    Vectorize {
        endpoint: String,
        credential_scope: String,
        #[serde(default)]
        provider: VectorProvider,
    },
    Hyperdrive {
        pgcat_endpoint: String,
        credential_scope: String,
        #[serde(default)]
        caching_disabled: bool,
        #[serde(default = "cache_age")]
        max_age_secs: u64,
        #[serde(default = "stale_age")]
        stale_while_revalidate_secs: u64,
        #[serde(default = "pool_size")]
        pool_max_connections: u32,
    },
    AnalyticsEngine {
        dataset: String,
        credential_scope: String,
    },
    Outbound {
        allowed_hosts: Vec<String>,
    },
    AwsSigv4 {
        credential_source: CredentialsSource,
        region: String,
        service: String,
        allowed_hosts: Vec<String>,
        #[serde(default)]
        access_key_env: Option<String>,
        #[serde(default)]
        secret_key_env: Option<String>,
        #[serde(default)]
        token_env: Option<String>,
    },
    RateLimiter {
        limit: u32,
        period_secs: u32,
    },
    Workflow {
        class_name: String,
        unique_key: String,
    },
    SecretsStoreSecret {
        secret_name: String,
    },
    MtlsCertificate {
        cert_pem_env: String,
        key_pem_env: String,
    },
    Loader,
    Container {
        image: String,
        default_port: u16,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default)]
        memory_mb: Option<u64>,
        #[serde(default)]
        cpu_millis: Option<u64>,
        #[serde(default = "container_sleep")]
        idle_sleep_secs: u64,
        #[serde(default)]
        allow_network_egress: bool,
    },
    Ai {
        endpoint: String,
        credential_scope: String,
        #[serde(default)]
        provider: AiProvider,
    },
    Dispatcher {
        namespace: String,
    },
    Assets,
    Images {
        #[serde(default)]
        provider: ImageProvider,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct R2Notification {
    pub queue_name: String,
    pub event_types: Vec<R2EventType>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum R2EventType {
    ObjectCreate,
    ObjectDelete,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Socket {
    pub name: String,
    pub listen: String,
    pub service: String,
}

const fn container_sleep() -> u64 {
    300
}
const fn cache_age() -> u64 {
    60
}
const fn stale_age() -> u64 {
    15
}
const fn pool_size() -> u32 {
    10
}
