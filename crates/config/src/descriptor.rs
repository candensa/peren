use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Binding, Entrypoint, FleetConfig, IdSource, QueueConsumer, Service};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceDescriptor {
    pub name: String,
    pub bundle: PathBuf,
    pub compatibility_date: String,
    pub compatibility_flags: Vec<String>,
    pub entrypoint: EntrypointDescriptor,
    pub bindings: Vec<BindingDescriptor>,
    pub queues: Vec<QueueDescriptor>,
    pub cron: Vec<String>,
    pub tail: Vec<String>,
    pub modules: Vec<String>,
    pub assets: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EntrypointDescriptor {
    Stateless,
    DurableObject { class_name: String, id_from: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BindingDescriptor {
    pub name: String,
    pub kind: String,
    pub contract: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueDescriptor {
    pub queue: String,
}

impl ServiceDescriptor {
    #[must_use]
    pub fn digest(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("service descriptor is serializable");
        let mut digest = Sha256::new();
        digest.update(b"peren-service-descriptor-v1\0");
        digest.update(
            u64::try_from(bytes.len())
                .expect("descriptor length fits in u64")
                .to_be_bytes(),
        );
        digest.update(bytes);
        hex::encode(digest.finalize())
    }
}

impl FleetConfig {
    #[must_use]
    pub fn service_descriptors(&self) -> Vec<ServiceDescriptor> {
        self.services.iter().map(ServiceDescriptor::from).collect()
    }
}

impl From<&Service> for ServiceDescriptor {
    fn from(service: &Service) -> Self {
        let mut compatibility_flags = service.compatibility_flags.clone();
        compatibility_flags.sort();
        let mut modules = service
            .additional_modules
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        modules.sort();
        Self {
            name: service.name.clone(),
            bundle: service.worker_bundle_path.clone(),
            compatibility_date: service.compatibility_date.clone(),
            compatibility_flags,
            entrypoint: entrypoint(&service.entrypoint),
            bindings: bindings(&service.bindings),
            queues: queues(&service.consumes_queues),
            cron: sorted(
                service
                    .cron_triggers
                    .iter()
                    .map(|trigger| trigger.expression.clone())
                    .collect(),
            ),
            tail: sorted(
                service
                    .tail_consumers
                    .iter()
                    .map(|consumer| consumer.service.clone())
                    .collect(),
            ),
            modules,
            assets: service.assets.is_some(),
        }
    }
}

fn entrypoint(entrypoint: &Entrypoint) -> EntrypointDescriptor {
    match entrypoint {
        Entrypoint::Stateless => EntrypointDescriptor::Stateless,
        Entrypoint::DurableObject {
            class_name,
            id_from,
            ..
        } => EntrypointDescriptor::DurableObject {
            class_name: class_name.clone(),
            id_from: id_source(id_from).to_string(),
        },
    }
}

fn id_source(source: &IdSource) -> &'static str {
    match source {
        IdSource::Header { .. } => "header",
        IdSource::FirstPathSegment => "first_path_segment",
    }
}

fn bindings(bindings: &BTreeMap<String, Binding>) -> Vec<BindingDescriptor> {
    bindings
        .iter()
        .map(|(name, binding)| BindingDescriptor {
            name: name.clone(),
            kind: kind(binding).to_string(),
            contract: contract(binding),
        })
        .collect()
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

fn queues(consumers: &[QueueConsumer]) -> Vec<QueueDescriptor> {
    let mut queues = consumers
        .iter()
        .map(|consumer| QueueDescriptor {
            queue: match consumer {
                QueueConsumer::Name(queue) => queue.clone(),
                QueueConsumer::Settings(settings) => settings.queue.clone(),
            },
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.queue.cmp(&right.queue));
    queues
}

fn kind(binding: &Binding) -> &'static str {
    match binding {
        Binding::Service { .. } => "service",
        Binding::DurableObjectNamespace { .. } => "durable_object_namespace",
        Binding::Kv { .. } => "kv",
        Binding::D1Database { .. } => "d1_database",
        Binding::R2Bucket { .. } => "r2_bucket",
        Binding::Queue { .. } => "queue",
        Binding::Vectorize { .. } => "vectorize",
        Binding::Hyperdrive { .. } => "hyperdrive",
        Binding::AnalyticsEngine { .. } => "analytics_engine",
        Binding::Outbound { .. } => "outbound",
        Binding::AwsSigv4 { .. } => "aws_sigv4",
        Binding::RateLimiter { .. } => "rate_limiter",
        Binding::Workflow { .. } => "workflow",
        Binding::SecretsStoreSecret { .. } => "secrets_store_secret",
        Binding::MtlsCertificate { .. } => "mtls_certificate",
        Binding::Loader => "loader",
        Binding::Container { .. } => "container",
        Binding::Ai { .. } => "ai",
        Binding::Dispatcher { .. } => "dispatcher",
        Binding::Assets => "assets",
        Binding::Images { .. } => "images",
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "binding descriptor mapping is clearer as one exhaustive match"
)]
fn contract(binding: &Binding) -> BTreeMap<String, String> {
    // Descriptors are safe to persist and compare: omit credential environment
    // variables, endpoints that may reveal private infrastructure, and secret material.
    let mut contract = BTreeMap::new();
    match binding {
        Binding::Service {
            entrypoint,
            class_name,
            ..
        } => {
            contract.insert("entrypoint".into(), entrypoint.clone());
            if let Some(class_name) = class_name {
                contract.insert("class_name".into(), class_name.clone());
            }
        }
        Binding::DurableObjectNamespace {
            class_name,
            unique_key,
        }
        | Binding::Workflow {
            class_name,
            unique_key,
        } => insert_class_key(&mut contract, class_name, unique_key),
        Binding::Kv {
            namespace,
            unique_key,
            backend,
        } => {
            contract.insert("namespace".into(), namespace.clone());
            contract.insert("unique_key".into(), unique_key.clone());
            contract.insert("backend".into(), provider_kind(backend));
        }
        Binding::D1Database {
            database_name,
            unique_key,
            backend,
        } => {
            contract.insert("database".into(), database_name.clone());
            contract.insert("unique_key".into(), unique_key.clone());
            contract.insert("backend".into(), provider_kind(backend));
        }
        Binding::R2Bucket {
            bucket,
            region,
            prefix,
            notifications,
            ..
        } => {
            contract.insert("bucket".into(), bucket.clone());
            if let Some(region) = region {
                contract.insert("region".into(), region.clone());
            }
            if let Some(prefix) = prefix {
                contract.insert("prefix".into(), prefix.clone());
            }
            contract.insert("notifications".into(), notifications.len().to_string());
        }
        Binding::Queue { queue_name } => {
            contract.insert("queue".into(), queue_name.clone());
        }
        Binding::Vectorize { provider, .. } => {
            contract.insert("provider".into(), provider_kind(provider));
        }
        Binding::Hyperdrive {
            caching_disabled,
            pool_max_connections,
            ..
        } => {
            contract.insert("caching_disabled".into(), caching_disabled.to_string());
            contract.insert(
                "pool_max_connections".into(),
                pool_max_connections.to_string(),
            );
        }
        Binding::AnalyticsEngine { dataset, .. } => {
            contract.insert("dataset".into(), dataset.clone());
        }
        Binding::Outbound { allowed_hosts } => {
            contract.insert("allowed_hosts".into(), allowed_hosts.len().to_string());
        }
        Binding::AwsSigv4 {
            credential_source,
            region,
            service,
            allowed_hosts,
            ..
        } => {
            contract.insert("credential_source".into(), format!("{credential_source:?}"));
            contract.insert("region".into(), region.clone());
            contract.insert("service".into(), service.clone());
            contract.insert("allowed_hosts".into(), allowed_hosts.len().to_string());
        }
        Binding::RateLimiter { limit, period_secs } => {
            contract.insert("limit".into(), limit.to_string());
            contract.insert("period_secs".into(), period_secs.to_string());
        }
        Binding::SecretsStoreSecret { secret_name } => {
            contract.insert("secret_name".into(), secret_name.clone());
        }
        Binding::MtlsCertificate { .. } | Binding::Loader | Binding::Assets => {}
        Binding::Container {
            image,
            default_port,
            ..
        } => {
            contract.insert("image".into(), image.clone());
            contract.insert("default_port".into(), default_port.to_string());
        }
        Binding::Ai { provider, .. } => {
            contract.insert("provider".into(), provider_kind(provider));
        }
        Binding::Dispatcher { namespace } => {
            contract.insert("namespace".into(), namespace.clone());
        }
        Binding::Images { provider } => {
            contract.insert("provider".into(), provider_kind(provider));
        }
    }
    contract
}

fn insert_class_key(contract: &mut BTreeMap<String, String>, class_name: &str, unique_key: &str) {
    contract.insert("class_name".into(), class_name.to_string());
    contract.insert("unique_key".into(), unique_key.to_string());
}

fn provider_kind<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| {
            value
                .get("kind")
                .and_then(|kind| kind.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "configured".to_string())
}
