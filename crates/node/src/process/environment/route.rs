use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use peren_config::{Binding, D1Backend, KvBackend, Service};
use peren_provider_object_store::{R2Store, S3Credentials, S3Options};

use crate::{
    Providers,
    host::KvStore,
    process::{D1Route, ProcessError},
};

pub(in crate::process) fn d1_routes(
    services: &[Service],
    providers: &Providers,
    data: &Path,
) -> Result<BTreeMap<String, BTreeMap<String, D1Route>>, ProcessError> {
    services
        .iter()
        .map(|service| {
            let routes = service
                .bindings
                .iter()
                .filter_map(|(name, binding)| match binding {
                    Binding::D1Database {
                        backend:
                            D1Backend::Turso {
                                url_env,
                                token_env,
                                replica_path,
                            },
                        ..
                    } => Some((name, url_env, token_env, replica_path)),
                    _ => None,
                })
                .map(|(name, url_env, token_env, replica_path)| {
                    let url = providers
                        .resolved(url_env)
                        .ok_or_else(|| ProcessError::SecretVariable(url_env.clone()))?;
                    let token = providers
                        .resolved(token_env)
                        .ok_or_else(|| ProcessError::SecretVariable(token_env.clone()))?;
                    let replica = replica_path.as_ref().map(|path| data_path(data, path));
                    Ok((
                        name.clone(),
                        D1Route {
                            url: Arc::from(url),
                            token: Arc::from(token),
                            replica,
                        },
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, ProcessError>>()?;
            Ok((service.name.clone(), routes))
        })
        .collect()
}

pub(in crate::process) fn r2_routes(
    services: &[Service],
    providers: &Providers,
) -> Result<BTreeMap<String, BTreeMap<String, Arc<R2Store>>>, ProcessError> {
    services
        .iter()
        .map(|service| {
            let routes = service
                .bindings
                .values()
                .filter_map(|binding| match binding {
                    Binding::R2Bucket { .. } => Some(r2_route(binding, providers)),
                    _ => None,
                })
                .collect::<Result<BTreeMap<_, _>, ProcessError>>()?;
            Ok((service.name.clone(), routes))
        })
        .collect()
}

fn r2_route(
    binding: &Binding,
    providers: &Providers,
) -> Result<(String, Arc<R2Store>), ProcessError> {
    let Binding::R2Bucket {
        endpoint,
        bucket,
        region,
        access_key_env,
        secret_key_env,
        token_env,
        allow_http,
        ..
    } = binding
    else {
        unreachable!("r2_route only accepts R2 bucket bindings");
    };
    if endpoint.starts_with("memory://") {
        return Ok((
            bucket.clone(),
            Arc::new(R2Store::new(
                Arc::new(object_store::memory::InMemory::new()),
            )),
        ));
    }
    let access_key_env =
        access_key_env
            .as_deref()
            .ok_or_else(|| ProcessError::MissingR2Credential {
                bucket: bucket.clone(),
                field: "access_key_env",
            })?;
    let secret_key_env =
        secret_key_env
            .as_deref()
            .ok_or_else(|| ProcessError::MissingR2Credential {
                bucket: bucket.clone(),
                field: "secret_key_env",
            })?;
    let access_key = providers
        .resolved(access_key_env)
        .ok_or_else(|| ProcessError::SecretVariable(access_key_env.to_string()))?;
    let secret_key = providers
        .resolved(secret_key_env)
        .ok_or_else(|| ProcessError::SecretVariable(secret_key_env.to_string()))?;
    let token = token_env
        .as_deref()
        .map(|name| {
            providers
                .resolved(name)
                .map(str::to_string)
                .ok_or_else(|| ProcessError::SecretVariable(name.to_string()))
        })
        .transpose()?;
    let store = R2Store::s3(
        S3Options {
            bucket: bucket.clone(),
            region: region.as_deref().unwrap_or("auto").to_string(),
            endpoint: Some(endpoint.clone()),
            allow_http: *allow_http,
            virtual_hosted: false,
        },
        S3Credentials::new(access_key.to_string(), secret_key.to_string(), token),
    )?;
    Ok((bucket.clone(), Arc::new(store)))
}

pub(in crate::process) fn kv_routes(
    services: &[Service],
    providers: &Providers,
) -> Result<BTreeMap<String, BTreeMap<String, KvStore>>, ProcessError> {
    services
        .iter()
        .map(|service| {
            let routes = service
                .bindings
                .values()
                .filter_map(|binding| match binding {
                    Binding::Kv {
                        namespace, backend, ..
                    } => match backend {
                        KvBackend::Native => None,
                        KvBackend::Redis { url_env } => {
                            Some(kv_redis_route(namespace, url_env, providers))
                        }
                        KvBackend::Bucket {
                            endpoint,
                            bucket,
                            prefix,
                            access_key_id_env,
                            secret_access_key_env,
                            allow_http,
                        } => Some(kv_bucket_route(
                            namespace,
                            endpoint,
                            bucket,
                            prefix,
                            access_key_id_env,
                            secret_access_key_env,
                            *allow_http,
                            providers,
                        )),
                    },
                    _ => None,
                })
                .collect::<Result<BTreeMap<_, _>, ProcessError>>()?;
            Ok((service.name.clone(), routes))
        })
        .collect()
}

fn kv_redis_route(
    namespace: &str,
    url_env: &str,
    providers: &Providers,
) -> Result<(String, KvStore), ProcessError> {
    let url = providers
        .resolved(url_env)
        .ok_or_else(|| ProcessError::SecretVariable(url_env.to_string()))?;
    Ok((
        namespace.to_string(),
        KvStore::redis(url.to_string(), "peren"),
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "matches the validated KV bucket backend fields"
)]
fn kv_bucket_route(
    namespace: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    access_key_id_env: &str,
    secret_access_key_env: &str,
    allow_http: bool,
    providers: &Providers,
) -> Result<(String, KvStore), ProcessError> {
    if endpoint.starts_with("memory://") {
        return Ok((
            namespace.to_string(),
            KvStore::bucket(
                Arc::new(R2Store::new(
                    Arc::new(object_store::memory::InMemory::new()),
                )),
                prefix.to_string(),
            ),
        ));
    }
    let access_key = providers
        .resolved(access_key_id_env)
        .ok_or_else(|| ProcessError::SecretVariable(access_key_id_env.to_string()))?;
    let secret_key = providers
        .resolved(secret_access_key_env)
        .ok_or_else(|| ProcessError::SecretVariable(secret_access_key_env.to_string()))?;
    let store = R2Store::s3(
        S3Options {
            bucket: bucket.to_string(),
            region: "us-east-1".into(),
            endpoint: Some(endpoint.to_string()),
            allow_http,
            virtual_hosted: false,
        },
        S3Credentials::new(access_key.to_string(), secret_key.to_string(), None),
    )?;
    Ok((
        namespace.to_string(),
        KvStore::bucket(Arc::new(store), prefix.to_string()),
    ))
}

fn data_path(data: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        data.join(path)
    }
}
