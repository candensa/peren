use std::{collections::BTreeMap, sync::Arc};

use peren_runtime::{CompatibilityError, ResolvedCompatibility};

pub(super) struct Secrets {
    pub(super) store: BTreeMap<String, Arc<str>>,
    pub(super) environment: BTreeMap<String, Arc<str>>,
}

pub(super) fn collect(
    config: &peren_config::ValidatedConfig,
    environment: &impl super::Environment,
) -> Result<Secrets, super::ProviderError> {
    let mut secrets = BTreeMap::new();
    let mut resolved = BTreeMap::new();
    for (name, variable) in &config.raw.secrets_store {
        let value = match crate::secret::active_value(config, environment, name)? {
            Some(value) => value,
            None => secret(environment, variable)?,
        };
        secrets.insert(name.clone(), Arc::from(value));
    }
    for variable in cache_environment_references(&config.raw.cache) {
        resolve(environment, variable, &mut resolved)?;
    }
    for service in &config.raw.services {
        for (name, secret) in &service.secrets {
            resolve_worker_secret(config, environment, name, secret, &mut resolved)?;
        }
        for binding in service.bindings.values() {
            for variable in environment_references(binding) {
                resolve(environment, variable, &mut resolved)?;
            }
        }
    }
    Ok(Secrets {
        store: secrets,
        environment: resolved,
    })
}

pub(super) fn resolve_compatibility(
    config: &peren_config::ValidatedConfig,
) -> Result<BTreeMap<String, ResolvedCompatibility>, CompatibilityError> {
    config
        .raw
        .services
        .iter()
        .map(|service| {
            ResolvedCompatibility::resolve(
                &service.compatibility_date,
                &service.compatibility_flags,
            )
            .map(|compatibility| (service.name.clone(), compatibility))
        })
        .collect()
}

fn resolve_worker_secret(
    config: &peren_config::ValidatedConfig,
    environment: &impl super::Environment,
    binding: &str,
    declaration: &peren_config::Secret,
    resolved: &mut BTreeMap<String, Arc<str>>,
) -> Result<(), super::ProviderError> {
    let key = declaration.lookup_key(binding);
    if resolved.contains_key(&key) {
        return Ok(());
    }
    let value = if let Some(name) = declaration.store_name(binding) {
        match crate::secret::active_value(config, environment, name)? {
            Some(value) => value,
            None => match declaration.env_variable() {
                Some(variable) => secret(environment, variable)?,
                None => return Err(crate::SecretError::Missing(name.into()).into()),
            },
        }
    } else {
        let variable = declaration
            .env_variable()
            .expect("secret declaration without a store must have an environment variable");
        secret(environment, variable)?
    };
    resolved.insert(key, Arc::from(value));
    Ok(())
}

fn resolve(
    environment: &impl super::Environment,
    variable: &str,
    resolved: &mut BTreeMap<String, Arc<str>>,
) -> Result<(), super::ProviderError> {
    if !resolved.contains_key(variable) {
        resolved.insert(variable.into(), Arc::from(secret(environment, variable)?));
    }
    Ok(())
}

fn cache_environment_references(cache: &peren_config::Cache) -> Vec<&str> {
    match cache {
        peren_config::Cache::Redis { url_env } => vec![url_env],
        peren_config::Cache::Bucket {
            endpoint,
            access_key_id_env,
            secret_access_key_env,
            ..
        } if !endpoint.starts_with("memory://") => vec![access_key_id_env, secret_access_key_env],
        peren_config::Cache::Memory
        | peren_config::Cache::Kv { .. }
        | peren_config::Cache::Bucket { .. } => Vec::new(),
    }
}

fn environment_references(binding: &peren_config::Binding) -> Vec<&str> {
    match binding {
        peren_config::Binding::Kv {
            backend: peren_config::KvBackend::Redis { url_env },
            ..
        }
        | peren_config::Binding::D1Database {
            backend: peren_config::D1Backend::External { url_env, .. },
            ..
        } => vec![url_env],
        peren_config::Binding::D1Database {
            backend:
                peren_config::D1Backend::Turso {
                    url_env, token_env, ..
                },
            ..
        } => vec![url_env, token_env],
        peren_config::Binding::Kv {
            backend:
                peren_config::KvBackend::Bucket {
                    endpoint,
                    access_key_id_env,
                    secret_access_key_env,
                    ..
                },
            ..
        } if !endpoint.starts_with("memory://") => vec![access_key_id_env, secret_access_key_env],
        peren_config::Binding::MtlsCertificate {
            cert_pem_env,
            key_pem_env,
        } => vec![cert_pem_env, key_pem_env],
        peren_config::Binding::R2Bucket {
            access_key_env,
            secret_key_env,
            token_env,
            ..
        } => access_key_env
            .iter()
            .chain(secret_key_env.iter())
            .chain(token_env.iter())
            .map(String::as_str)
            .collect(),
        peren_config::Binding::AwsSigv4 {
            credential_source:
                peren_config::CredentialsSource::Configured
                | peren_config::CredentialsSource::Environment,
            access_key_env,
            secret_key_env,
            token_env,
            ..
        } => access_key_env
            .iter()
            .chain(secret_key_env.iter())
            .chain(token_env.iter())
            .map(String::as_str)
            .collect(),
        peren_config::Binding::AwsSigv4 { .. } => Vec::new(),
        peren_config::Binding::Vectorize { provider, .. } => {
            vector_environment_references(provider)
        }
        peren_config::Binding::Ai { provider, .. } => ai_environment_references(provider),
        peren_config::Binding::Images { provider } => image_environment_references(provider),
        _ => Vec::new(),
    }
}

fn vector_environment_references(provider: &peren_config::VectorProvider) -> Vec<&str> {
    match provider {
        peren_config::VectorProvider::Local => Vec::new(),
        peren_config::VectorProvider::Http { token_env, .. }
        | peren_config::VectorProvider::Qdrant {
            api_key_env: token_env,
            ..
        }
        | peren_config::VectorProvider::Weaviate {
            api_key_env: token_env,
            ..
        } => token_env.iter().map(String::as_str).collect(),
        peren_config::VectorProvider::Pinecone { api_key_env, .. } => vec![api_key_env],
    }
}

fn ai_environment_references(provider: &peren_config::AiProvider) -> Vec<&str> {
    match provider {
        peren_config::AiProvider::Http | peren_config::AiProvider::Local { .. } => Vec::new(),
        peren_config::AiProvider::OpenAi { api_key_env, .. }
        | peren_config::AiProvider::Anthropic { api_key_env, .. }
        | peren_config::AiProvider::Gemini { api_key_env, .. } => vec![api_key_env],
        peren_config::AiProvider::WorkersAi {
            account_id_env,
            api_token_env,
        } => vec![account_id_env, api_token_env],
    }
}

fn image_environment_references(provider: &peren_config::ImageProvider) -> Vec<&str> {
    match provider {
        peren_config::ImageProvider::Local => Vec::new(),
        peren_config::ImageProvider::Http { token_env, .. } => {
            token_env.iter().map(String::as_str).collect()
        }
    }
}

pub(super) fn secret(
    environment: &impl super::Environment,
    variable: &str,
) -> Result<String, super::ProviderError> {
    environment
        .get(variable)
        .ok_or_else(|| super::ProviderError::MissingEnvironment(variable.into()))
}

pub(super) fn required<T>(
    value: Option<T>,
    field: &'static str,
) -> Result<T, super::ProviderError> {
    value.ok_or(super::ProviderError::MissingField(field))
}
