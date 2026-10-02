use std::collections::BTreeMap;

use peren_config::{Binding, D1Backend, KvBackend, Service};
use peren_primitives::NodeId;
use peren_runtime::WorkerEnvironment;

use crate::{Providers, process::ProcessError};

fn kv_provider_metadata(backend: &KvBackend) -> serde_json::Value {
    match backend {
        KvBackend::Native => serde_json::json!({ "kind": "native" }),
        KvBackend::Redis { .. } => serde_json::json!({ "kind": "redis" }),
        KvBackend::Bucket { .. } => serde_json::json!({ "kind": "bucket" }),
    }
}

fn d1_provider_metadata(backend: &D1Backend) -> serde_json::Value {
    match backend {
        D1Backend::NativeSqlite => serde_json::json!({ "kind": "native_sqlite" }),
        D1Backend::Turso { replica_path, .. } => serde_json::json!({
            "kind": "turso",
            "replica": replica_path.is_some(),
        }),
        D1Backend::External { driver, .. } => serde_json::json!({
            "kind": "external",
            "driver": driver,
            "supported": false,
        }),
    }
}
fn image_route(
    provider: &peren_config::ImageProvider,
    providers: &Providers,
) -> Result<serde_json::Value, ProcessError> {
    Ok(match provider {
        peren_config::ImageProvider::Local => serde_json::json!({ "kind": "local" }),
        peren_config::ImageProvider::Http { url, token_env } => serde_json::json!({
            "kind": "http",
            "url": url,
            "authorization": optional_provider_token(providers, token_env.as_deref())?
                .map(|token| format!("Bearer {token}")),
        }),
    })
}

fn vector_route(
    endpoint: &str,
    provider: &peren_config::VectorProvider,
    providers: &Providers,
) -> Result<serde_json::Value, ProcessError> {
    Ok(match provider {
        peren_config::VectorProvider::Local => serde_json::json!({
            "kind": "local",
            "index": endpoint,
        }),
        peren_config::VectorProvider::Http { url, token_env } => serde_json::json!({
            "kind": "http",
            "url": url,
            "authorization": optional_provider_token(providers, token_env.as_deref())?
                .map(|token| format!("Bearer {token}")),
        }),
        peren_config::VectorProvider::Qdrant {
            url,
            collection,
            api_key_env,
        } => serde_json::json!({
            "kind": "qdrant",
            "url": url,
            "collection": collection,
            "apiKey": optional_provider_token(providers, api_key_env.as_deref())?,
        }),
        peren_config::VectorProvider::Pinecone {
            url,
            index,
            api_key_env,
            namespace,
        } => serde_json::json!({
            "kind": "pinecone",
            "url": url,
            "index": index,
            "namespace": namespace,
            "apiKey": required_provider_token(providers, api_key_env)?,
        }),
        peren_config::VectorProvider::Weaviate {
            url,
            class_name,
            api_key_env,
        } => serde_json::json!({
            "kind": "weaviate",
            "url": url,
            "className": class_name,
            "apiKey": optional_provider_token(providers, api_key_env.as_deref())?,
        }),
    })
}

fn ai_route(
    endpoint: &str,
    provider: &peren_config::AiProvider,
    providers: &Providers,
) -> Result<serde_json::Value, ProcessError> {
    Ok(match provider {
        peren_config::AiProvider::Http => serde_json::json!({
            "kind": "http",
            "url": endpoint,
        }),
        peren_config::AiProvider::OpenAi {
            api_key_env,
            base_url,
        } => serde_json::json!({
            "kind": "open_ai",
            "url": base_url,
            "authorization": format!("Bearer {}", required_provider_token(providers, api_key_env)?),
        }),
        peren_config::AiProvider::Anthropic {
            api_key_env,
            base_url,
            version,
        } => serde_json::json!({
            "kind": "anthropic",
            "url": base_url,
            "xApiKey": required_provider_token(providers, api_key_env)?,
            "anthropicVersion": version,
        }),
        peren_config::AiProvider::Gemini {
            api_key_env,
            base_url,
        } => serde_json::json!({
            "kind": "gemini",
            "url": base_url,
            "apiKey": required_provider_token(providers, api_key_env)?,
        }),
        peren_config::AiProvider::WorkersAi {
            account_id_env,
            api_token_env,
        } => {
            let account = required_provider_token(providers, account_id_env)?;
            let token = required_provider_token(providers, api_token_env)?;
            serde_json::json!({
                "kind": "workers_ai",
                "url": format!("https://api.cloudflare.com/client/v4/accounts/{account}/ai/run"),
                "authorization": format!("Bearer {token}"),
            })
        }
        peren_config::AiProvider::Local { command } => serde_json::json!({
            "kind": "local",
            "url": endpoint,
            "command": command,
        }),
    })
}

fn optional_provider_token<'a>(
    providers: &'a Providers,
    variable: Option<&str>,
) -> Result<Option<&'a str>, ProcessError> {
    variable
        .map(|name| required_provider_token(providers, name))
        .transpose()
}

fn required_provider_token<'a>(
    providers: &'a Providers,
    variable: &str,
) -> Result<&'a str, ProcessError> {
    providers
        .resolved(variable)
        .ok_or_else(|| ProcessError::SecretVariable(variable.to_string()))
}

pub(in crate::process) fn environments(
    node_id: NodeId,
    services: &[Service],
    dispatch_namespaces: &[peren_config::DispatchNamespace],
    cache: &peren_config::Cache,
    providers: &Providers,
    queue_provider: &serde_json::Value,
) -> Result<BTreeMap<String, WorkerEnvironment>, ProcessError> {
    services
        .iter()
        .map(|service| {
            environment(
                node_id,
                service,
                dispatch_namespaces,
                cache,
                providers,
                queue_provider,
            )
            .map(|env| (service.name.clone(), env))
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
pub(in crate::process) fn environment(
    node_id: NodeId,
    service: &Service,
    dispatch_namespaces: &[peren_config::DispatchNamespace],
    cache: &peren_config::Cache,
    providers: &Providers,
    queue_provider: &serde_json::Value,
) -> Result<WorkerEnvironment, ProcessError> {
    let mut values = service.vars.clone();
    if service.expose_node_id {
        values.insert("PEREN_NODE_ID".into(), node_id.as_uuid().to_string());
    }
    let mut bindings = serde_json::Map::new();
    for (name, secret) in &service.secrets {
        let key = secret.lookup_key(name);
        let value = providers
            .resolved(&key)
            .ok_or_else(|| ProcessError::SecretVariable(key.clone()))?;
        values.insert(name.clone(), value.to_string());
    }
    values.insert(
        "__perenCache".into(),
        serde_json::to_string(cache).map_err(ProcessError::Environment)?,
    );
    for (name, secret) in &service.secrets_store_refs {
        let value = providers
            .secret(secret)
            .ok_or_else(|| ProcessError::SecretStore(secret.clone()))?;
        values.insert(name.clone(), value.to_string());
    }
    for (name, binding) in &service.bindings {
        match binding {
            Binding::Service { entrypoint, .. } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "service",
                        "service": entrypoint,
                    }),
                );
            }
            Binding::Dispatcher { namespace } => {
                let scripts = dispatch_namespaces
                    .iter()
                    .find(|candidate| candidate.name == *namespace)
                    .map(|candidate| {
                        candidate
                            .scripts
                            .iter()
                            .map(|script| script.name.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "dispatcher",
                        "namespace": namespace,
                        "scripts": scripts,
                    }),
                );
            }
            Binding::Kv {
                namespace, backend, ..
            } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "kv",
                        "scope": namespace,
                        "provider": kv_provider_metadata(backend),
                    }),
                );
            }
            Binding::D1Database { backend, .. }
                if matches!(backend, D1Backend::NativeSqlite | D1Backend::Turso { .. }) =>
            {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "d1",
                        "provider": d1_provider_metadata(backend),
                    }),
                );
            }
            Binding::Queue { queue_name } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "queue",
                        "queue": queue_name,
                        "provider": queue_provider,
                    }),
                );
            }
            Binding::R2Bucket {
                endpoint,
                bucket,
                prefix,
                allow_http,
                ..
            } => {
                let provider = if endpoint.starts_with("memory://") {
                    serde_json::json!({ "kind": "memory" })
                } else {
                    serde_json::json!({
                        "kind": "s3",
                        "endpoint": endpoint,
                        "bucket": bucket,
                        "allowHttp": allow_http,
                    })
                };
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "r2",
                        "bucket": bucket,
                        "prefix": prefix.clone().unwrap_or_default(),
                        "provider": provider,
                    }),
                );
            }
            Binding::Vectorize {
                endpoint, provider, ..
            } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "vectorize",
                        "index": endpoint,
                        "provider": provider,
                        "route": vector_route(endpoint, provider, providers)?,
                    }),
                );
            }
            Binding::Workflow { .. } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "workflow",
                        "provider": { "kind": "native" },
                    }),
                );
            }
            Binding::Loader => {
                bindings.insert(name.clone(), serde_json::json!({ "type": "loader" }));
            }
            Binding::Container {
                image,
                default_port,
                memory_mb,
                cpu_millis,
                idle_sleep_secs,
                allow_network_egress,
                ..
            } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "container",
                        "image": image,
                        "port": default_port,
                        "memoryMb": memory_mb,
                        "cpuMillis": cpu_millis,
                        "idleSleepSecs": idle_sleep_secs,
                        "allowNetworkEgress": allow_network_egress,
                        "provider": { "kind": "local" },
                    }),
                );
            }
            Binding::RateLimiter { limit, period_secs } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "rate_limiter",
                        "limit": limit,
                        "periodSecs": period_secs,
                        "provider": { "kind": "memory" },
                    }),
                );
            }
            Binding::AnalyticsEngine { dataset, .. } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "analytics_engine",
                        "dataset": dataset,
                        "provider": { "kind": "buffer", "dataset": dataset },
                    }),
                );
            }
            Binding::Outbound { allowed_hosts } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "outbound",
                        "allowedHosts": allowed_hosts,
                    }),
                );
            }
            Binding::AwsSigv4 {
                region,
                service,
                allowed_hosts,
                ..
            } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "aws_sigv4",
                        "region": region,
                        "service": service,
                        "allowedHosts": allowed_hosts,
                    }),
                );
            }
            Binding::Ai {
                endpoint, provider, ..
            } => {
                let route = ai_route(endpoint, provider, providers)?;
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "ai",
                        "endpoint": endpoint,
                        "provider": provider,
                        "route": route,
                    }),
                );
            }
            Binding::Hyperdrive {
                pgcat_endpoint,
                caching_disabled,
                max_age_secs,
                stale_while_revalidate_secs,
                pool_max_connections,
                ..
            } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "hyperdrive",
                        "connectionString": pgcat_endpoint,
                        "cachingDisabled": caching_disabled,
                        "maxAge": max_age_secs,
                        "staleWhileRevalidate": stale_while_revalidate_secs,
                        "poolMaxConnections": pool_max_connections,
                    }),
                );
            }
            Binding::Images { provider } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "images",
                        "provider": image_route(provider, providers)?,
                    }),
                );
            }
            Binding::DurableObjectNamespace { class_name, .. } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({
                        "type": "durable_object_namespace",
                        "className": class_name,
                    }),
                );
            }
            Binding::SecretsStoreSecret { secret_name } => {
                let value = providers
                    .secret(secret_name)
                    .ok_or_else(|| ProcessError::SecretStore(secret_name.clone()))?;
                values.insert(name.clone(), value.to_string());
            }
            Binding::MtlsCertificate { .. } => {
                bindings.insert(
                    name.clone(),
                    serde_json::json!({ "type": "mtls_certificate" }),
                );
            }
            _ => {}
        }
    }
    if !bindings.is_empty() {
        values.insert(
            "__perenBindings".into(),
            serde_json::Value::Object(bindings).to_string(),
        );
    }
    Ok(WorkerEnvironment::new(values))
}

pub(in crate::process) fn has_binding(environment: &WorkerEnvironment, kind: &str) -> bool {
    environment
        .get("__perenBindings")
        .and_then(|bindings| serde_json::from_str::<serde_json::Value>(bindings).ok())
        .and_then(|bindings| bindings.as_object().cloned())
        .is_some_and(|bindings| {
            bindings.values().any(|binding| {
                binding.get("type").and_then(serde_json::Value::as_str) == Some(kind)
            })
        })
}
