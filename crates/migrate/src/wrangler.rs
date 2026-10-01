use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Default)]
pub(crate) struct WranglerToml {
    pub(crate) name: String,
    pub(crate) main: String,
    #[serde(default)]
    pub(crate) compatibility_date: Option<String>,
    #[serde(default)]
    pub(crate) compatibility_flags: Vec<String>,

    #[serde(default)]
    pub(crate) kv_namespaces: Vec<KvNamespaceEntry>,
    #[serde(default)]
    pub(crate) d1_databases: Vec<D1DatabaseEntry>,
    #[serde(default)]
    pub(crate) r2_buckets: Vec<R2BucketEntry>,
    #[serde(default)]
    pub(crate) queues: Option<QueuesSection>,
    #[serde(default)]
    pub(crate) vars: HashMap<String, toml::Value>,
    #[serde(default)]
    pub(crate) durable_objects: Option<DurableObjectsSection>,
    #[serde(default)]
    pub(crate) services: Vec<ServiceBindingEntry>,
    #[serde(default)]
    pub(crate) triggers: Option<TriggersSection>,

    #[serde(default)]
    pub(crate) ai: Option<AiSection>,
    #[serde(default)]
    pub(crate) send_email: Vec<toml::Value>,
    #[serde(default)]
    pub(crate) browser: Option<toml::Value>,
    #[serde(default)]
    pub(crate) images: Option<toml::Value>,
    #[serde(default)]
    pub(crate) analytics_engine_datasets: Vec<toml::Value>,
    #[serde(default)]
    pub(crate) observability: Option<ObservabilitySection>,
    #[serde(default)]
    pub(crate) assets: Option<AssetsSection>,

    #[serde(default)]
    pub(crate) worker_loaders: Vec<WorkerLoaderEntry>,

    #[serde(default)]
    pub(crate) vectorize: Vec<VectorizeEntry>,
    #[serde(default)]
    pub(crate) hyperdrive: Vec<HyperdriveEntry>,
    #[serde(default)]
    pub(crate) workflows: Vec<WorkflowEntry>,
    #[serde(default)]
    pub(crate) containers: Vec<ContainerEntry>,
    #[serde(default)]
    pub(crate) mtls_certificates: Vec<MtlsCertificateEntry>,
    #[serde(default)]
    pub(crate) secrets_store_secrets: Vec<SecretsStoreSecretEntry>,

    #[serde(default)]
    pub(crate) migrations: Vec<toml::Value>,
    #[serde(default)]
    pub(crate) exports: Option<toml::Value>,
    #[serde(default)]
    pub(crate) routes: Vec<toml::Value>,
    #[serde(default)]
    pub(crate) route: Option<toml::Value>,
    #[serde(default)]
    pub(crate) env: HashMap<String, toml::Value>,
    #[serde(default)]
    pub(crate) placement: Option<toml::Value>,
    #[serde(default)]
    pub(crate) limits: Option<toml::Value>,
    #[serde(default)]
    pub(crate) logpush: Option<bool>,
    #[serde(default)]
    pub(crate) build: Option<BuildSection>,
    #[serde(default)]
    pub(crate) workers_dev: Option<bool>,
    #[serde(default)]
    pub(crate) account_id: Option<String>,
    #[serde(default)]
    pub(crate) preview_urls: Option<bool>,
    #[serde(default)]
    pub(crate) keep_vars: Option<bool>,
    #[serde(default)]
    pub(crate) send_metrics: Option<bool>,
    #[serde(default)]
    pub(crate) tsconfig: Option<String>,
    #[serde(default)]
    pub(crate) rules: Vec<toml::Value>,
    #[serde(default)]
    pub(crate) define: HashMap<String, toml::Value>,
    #[serde(default)]
    pub(crate) dispatch_namespaces: Vec<DispatchNamespaceEntry>,
    #[serde(default)]
    pub(crate) tail_consumers: Vec<toml::Value>,
    #[serde(default)]
    pub(crate) secrets: Option<SecretsSection>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct VectorizeEntry {
    pub(crate) binding: String,
    pub(crate) index_name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HyperdriveEntry {
    pub(crate) binding: String,
    pub(crate) id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WorkflowEntry {
    pub(crate) binding: String,
    pub(crate) name: String,
    pub(crate) class_name: String,
    #[serde(default)]
    pub(crate) script_name: Option<String>,
    #[serde(default)]
    pub(crate) schedules: Option<toml::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContainerEntry {
    pub(crate) class_name: String,
    pub(crate) image: String,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) max_instances: Option<u32>,
    #[serde(default)]
    pub(crate) instance_type: Option<String>,
    #[serde(flatten)]
    pub(crate) unknown: HashMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MtlsCertificateEntry {
    pub(crate) binding: String,
    pub(crate) certificate_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SecretsStoreSecretEntry {
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) store_id: Option<String>,
    #[serde(default)]
    pub(crate) secret_name: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct BuildSection {
    #[serde(default)]
    pub(crate) command: Option<String>,
    #[serde(default)]
    pub(crate) cwd: Option<String>,
    #[serde(default)]
    pub(crate) watch_dir: Option<toml::Value>,
    #[serde(default)]
    pub(crate) no_bundle: Option<bool>,
    #[serde(default)]
    pub(crate) minify: Option<bool>,
    #[serde(default)]
    pub(crate) find_additional_modules: Option<bool>,
    #[serde(default)]
    pub(crate) base_dir: Option<String>,
    #[serde(flatten)]
    pub(crate) unknown: HashMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SecretsSection {
    #[serde(default)]
    pub(crate) required: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WorkerLoaderEntry {
    pub(crate) binding: String,
    #[serde(flatten)]
    pub(crate) unknown: HashMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DispatchNamespaceEntry {
    pub(crate) binding: String,
    pub(crate) namespace: String,
    #[serde(flatten)]
    pub(crate) unknown: HashMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AssetsSection {
    pub(crate) directory: String,
    #[serde(default)]
    pub(crate) binding: Option<String>,
    #[serde(default)]
    pub(crate) html_handling: Option<String>,
    #[serde(default)]
    pub(crate) not_found_handling: Option<String>,
    #[serde(default)]
    pub(crate) run_worker_first: Option<RunWorkerFirstValue>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RunWorkerFirstValue {
    Always(bool),
    Patterns(Vec<String>),
}

#[derive(Debug, Deserialize)]
pub(crate) struct ObservabilitySection {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) head_sampling_rate: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AiSection {
    pub(crate) binding: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct KvNamespaceEntry {
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct D1DatabaseEntry {
    pub(crate) binding: String,
    pub(crate) database_name: String,
    #[serde(default)]
    pub(crate) database_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct R2BucketEntry {
    pub(crate) binding: String,
    pub(crate) bucket_name: String,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct QueuesSection {
    #[serde(default)]
    pub(crate) producers: Vec<QueueProducerEntry>,
    #[serde(default)]
    pub(crate) consumers: Vec<QueueConsumerEntry>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueueProducerEntry {
    pub(crate) binding: String,
    pub(crate) queue: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueueConsumerEntry {
    pub(crate) queue: String,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct DurableObjectsSection {
    #[serde(default)]
    pub(crate) bindings: Vec<DurableObjectBindingEntry>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DurableObjectBindingEntry {
    pub(crate) name: String,
    pub(crate) class_name: String,
    #[serde(default)]
    pub(crate) script_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ServiceBindingEntry {
    pub(crate) binding: String,
    pub(crate) service: String,
    #[serde(default)]
    pub(crate) entrypoint: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct TriggersSection {
    #[serde(default)]
    pub(crate) crons: Vec<String>,
}

const KNOWN_TOP_LEVEL_FIELDS: &[&str] = &[
    "name",
    "main",
    "compatibility_date",
    "compatibility_flags",
    "kv_namespaces",
    "d1_databases",
    "r2_buckets",
    "queues",
    "vars",
    "durable_objects",
    "services",
    "triggers",
    "ai",
    "send_email",
    "browser",
    "images",
    "analytics_engine_datasets",
    "observability",
    "assets",
    "worker_loaders",
    "vectorize",
    "hyperdrive",
    "workflows",
    "containers",
    "mtls_certificates",
    "secrets_store_secrets",
    "migrations",
    "exports",
    "routes",
    "route",
    "env",
    "placement",
    "limits",
    "logpush",
    "build",
    "workers_dev",
    "account_id",
    "preview_urls",
    "keep_vars",
    "send_metrics",
    "tsconfig",
    "rules",
    "define",
    "dispatch_namespaces",
    "tail_consumers",
    "secrets",
];

pub(crate) fn unknown_top_level_fields<'a>(raw_keys: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut unknown: Vec<String> = raw_keys
        .filter(|k| !KNOWN_TOP_LEVEL_FIELDS.contains(k))
        .map(std::string::ToString::to_string)
        .collect();
    unknown.sort_unstable();
    unknown.dedup();
    unknown
}
