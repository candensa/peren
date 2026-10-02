use std::path::PathBuf;

use peren_config::ValidatedConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Environment, ProcessError};

mod artifact;
mod clock;
mod registry;

use artifact::{SUPPORTED_DESCRIPTOR_SCHEMA, descriptor_digest, source_maps, verify_descriptor};
use clock::now_ms;
use registry::Registry;

#[derive(Debug)]
pub struct Record {
    pub percent: u8,
    pub preview: bool,
}

#[derive(Debug)]
pub struct RecordReport {
    pub generations: Vec<Generation>,
}

#[derive(Debug)]
pub struct List {
    pub service: Option<String>,
}

#[derive(Debug)]
pub struct ListReport {
    pub generations: Vec<Generation>,
}

#[derive(Debug)]
pub struct Prune {
    pub service: String,
    pub keep: usize,
    pub dry: bool,
}

#[derive(Debug)]
pub struct PruneReport {
    pub removed: Vec<Generation>,
    pub dry: bool,
}

#[derive(Debug)]
pub struct Verify {
    pub service: Option<String>,
}

#[derive(Debug)]
pub struct VerifyReport {
    pub generations: Vec<VerifiedGeneration>,
}

#[derive(Debug)]
pub struct Health {
    pub service: Option<String>,
}

#[derive(Debug)]
pub struct HealthReport {
    pub services: Vec<HealthyService>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthyService {
    pub service: String,
    pub scope: String,
    pub digest: String,
    pub percent: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerifiedGeneration {
    pub generation: Generation,
    pub current: String,
}

#[derive(Debug)]
pub struct Rollback {
    pub service: String,
    pub digest: Option<String>,
}

#[derive(Debug)]
pub struct RollbackReport {
    pub generation: Generation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Generation {
    pub service: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    pub digest: String,
    pub entry: String,
    pub modules: usize,
    #[serde(default = "default_descriptor_schema")]
    pub descriptor_schema: u16,
    #[serde(default)]
    pub descriptor_digest: Option<String>,
    #[serde(default)]
    pub required_features: Vec<String>,
    #[serde(default)]
    pub source_maps: usize,
    #[serde(default)]
    pub source_map_digest: Option<String>,
    pub created_at_ms: i64,
    pub percent: u8,
    #[serde(default)]
    pub preview: bool,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuditEvent {
    pub action: String,
    pub service: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub digest: Option<String>,
    pub created_at_ms: i64,
}

const fn default_descriptor_schema() -> u16 {
    SUPPORTED_DESCRIPTOR_SCHEMA
}

pub struct Deployment<'a, E> {
    config: &'a ValidatedConfig,
    environment: &'a E,
}

impl<'a, E> Deployment<'a, E>
where
    E: Environment,
{
    #[must_use]
    pub const fn new(config: &'a ValidatedConfig, environment: &'a E) -> Self {
        Self {
            config,
            environment,
        }
    }

    pub fn record(&self, request: &Record) -> Result<RecordReport, DeployError> {
        record(self.config, self.environment, request)
    }

    pub fn list(&self, request: &List) -> Result<ListReport, DeployError> {
        list(self.config, self.environment, request)
    }

    pub fn prune(&self, request: &Prune) -> Result<PruneReport, DeployError> {
        prune(self.config, self.environment, request)
    }

    pub fn verify(&self, request: &Verify) -> Result<VerifyReport, DeployError> {
        verify(self.config, self.environment, request)
    }

    pub fn health(&self, request: &Health) -> Result<HealthReport, DeployError> {
        health(self.config, self.environment, request)
    }

    pub fn rollback(&self, request: &Rollback) -> Result<RollbackReport, DeployError> {
        rollback(self.config, self.environment, request)
    }
}

pub fn record(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Record,
) -> Result<RecordReport, DeployError> {
    let mut registry = Registry::load(root(environment))?;
    let mut generations = Vec::new();
    for service in &config.raw.services {
        let scope = service.isolation_scope();
        let bundle = crate::bundle::load(service)?;
        let maps = source_maps(service)?;
        let digest = bundle.digest().to_string();
        let descriptor_digest = descriptor_digest(service, &bundle, &maps);
        let generation = Generation {
            service: service.name.clone(),
            scope: scope.clone(),
            tenant: service.tenant_id.clone(),
            project: service.project_id.clone(),
            digest,
            entry: bundle.entry().to_string(),
            modules: bundle.modules().len(),
            descriptor_schema: SUPPORTED_DESCRIPTOR_SCHEMA,
            descriptor_digest: Some(descriptor_digest),
            required_features: Vec::new(),
            source_maps: maps.count,
            source_map_digest: maps.digest,
            created_at_ms: now_ms()?,
            percent: if request.preview { 0 } else { request.percent },
            preview: request.preview,
            active: !request.preview && request.percent > 0,
        };
        let generation = registry.insert(generation);
        registry.audit(AuditEvent {
            action: if generation.preview {
                "preview"
            } else {
                "record"
            }
            .into(),
            service: generation.service.clone(),
            scope: generation.scope.clone(),
            digest: Some(generation.digest.clone()),
            created_at_ms: now_ms()?,
        });
        generations.push(generation);
    }
    registry.save()?;
    Ok(RecordReport { generations })
}

pub fn list(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &List,
) -> Result<ListReport, DeployError> {
    ensure_service(config, request.service.as_deref())?;
    let registry = Registry::load(root(environment))?;
    Ok(ListReport {
        generations: registry
            .generations
            .into_iter()
            .filter(|generation| {
                request
                    .service
                    .as_ref()
                    .is_none_or(|service| generation.service == *service)
                    && config
                        .raw
                        .services
                        .iter()
                        .any(|service| same_scope(generation, service))
            })
            .collect(),
    })
}

pub fn verify(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Verify,
) -> Result<VerifyReport, DeployError> {
    ensure_service(config, request.service.as_deref())?;
    let registry = Registry::load(root(environment))?;
    let mut verified = Vec::new();
    for service in &config.raw.services {
        if request
            .service
            .as_ref()
            .is_some_and(|name| *name != service.name)
        {
            continue;
        }
        let bundle = crate::bundle::load(service)?;
        let current = bundle.digest().to_string();
        let maps = source_maps(service)?;
        for generation in registry
            .generations
            .iter()
            .filter(|generation| same_scope(generation, service) && generation.active)
        {
            if generation.digest != current {
                return Err(DeployError::Drift {
                    service: service.name.clone(),
                    recorded: generation.digest.clone(),
                    current,
                });
            }
            if generation.source_map_digest != maps.digest {
                return Err(DeployError::MapDrift {
                    service: service.name.clone(),
                    recorded: generation.source_map_digest.clone(),
                    current: maps.digest,
                });
            }
            verify_descriptor(service, &bundle, &maps, generation)?;
            verified.push(VerifiedGeneration {
                generation: generation.clone(),
                current: current.clone(),
            });
        }
    }
    Ok(VerifyReport {
        generations: verified,
    })
}

pub fn health(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Health,
) -> Result<HealthReport, DeployError> {
    ensure_service(config, request.service.as_deref())?;
    let registry = Registry::load(root(environment))?;
    let mut services = Vec::new();
    for service in &config.raw.services {
        if request
            .service
            .as_ref()
            .is_some_and(|name| *name != service.name)
        {
            continue;
        }
        let active = registry
            .generations
            .iter()
            .filter(|generation| same_scope(generation, service) && generation.active)
            .collect::<Vec<_>>();
        match active.as_slice() {
            [] => return Err(DeployError::NoActive(service.name.clone())),
            [generation] if generation.preview => {
                return Err(DeployError::ActivePreview(service.name.clone()));
            }
            [generation] => {
                let bundle = crate::bundle::load(service)?;
                let current = bundle.digest().to_string();
                let maps = source_maps(service)?;
                if generation.digest != current {
                    return Err(DeployError::Drift {
                        service: service.name.clone(),
                        recorded: generation.digest.clone(),
                        current,
                    });
                }
                if generation.source_map_digest != maps.digest {
                    return Err(DeployError::MapDrift {
                        service: service.name.clone(),
                        recorded: generation.source_map_digest.clone(),
                        current: maps.digest,
                    });
                }
                verify_descriptor(service, &bundle, &maps, generation)?;
                services.push(HealthyService {
                    service: service.name.clone(),
                    scope: service.isolation_scope(),
                    digest: generation.digest.clone(),
                    percent: generation.percent,
                });
            }
            _ => return Err(DeployError::MultipleActive(service.name.clone())),
        }
    }
    Ok(HealthReport { services })
}

pub fn rollback(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Rollback,
) -> Result<RollbackReport, DeployError> {
    ensure_service(config, Some(&request.service))?;
    let mut registry = Registry::load(root(environment))?;
    let service = config
        .raw
        .services
        .iter()
        .find(|service| service.name == request.service)
        .ok_or_else(|| DeployError::Service(request.service.clone()))?;
    let digest = match &request.digest {
        Some(digest) => digest.clone(),
        None => registry
            .generations
            .iter()
            .filter(|generation| same_scope(generation, service) && !generation.active)
            .max_by(|left, right| {
                left.created_at_ms
                    .cmp(&right.created_at_ms)
                    .then_with(|| left.digest.cmp(&right.digest))
            })
            .map(|generation| generation.digest.clone())
            .ok_or_else(|| DeployError::RollbackTarget(request.service.clone()))?,
    };
    let mut selected = None;
    for generation in &mut registry.generations {
        if same_scope(generation, service) {
            generation.active = generation.digest == digest;
            if generation.active {
                selected = Some(generation.clone());
            }
        }
    }
    let generation = selected.ok_or_else(|| DeployError::Generation {
        service: request.service.clone(),
        digest,
    })?;
    let bundle = crate::bundle::load(service)?;
    let current = bundle.digest().to_string();
    if generation.digest != current {
        return Err(DeployError::Drift {
            service: request.service.clone(),
            recorded: generation.digest,
            current,
        });
    }
    let maps = source_maps(service)?;
    if generation.source_map_digest != maps.digest {
        return Err(DeployError::MapDrift {
            service: request.service.clone(),
            recorded: generation.source_map_digest,
            current: maps.digest,
        });
    }
    verify_descriptor(service, &bundle, &maps, &generation)?;
    registry.audit(AuditEvent {
        action: "rollback".into(),
        service: request.service.clone(),
        scope: service.isolation_scope(),
        digest: Some(generation.digest.clone()),
        created_at_ms: now_ms()?,
    });
    registry.save()?;
    Ok(RollbackReport { generation })
}

pub fn prune(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Prune,
) -> Result<PruneReport, DeployError> {
    ensure_service(config, Some(&request.service))?;
    let mut registry = Registry::load(root(environment))?;
    let service_config = config
        .raw
        .services
        .iter()
        .find(|service| service.name == request.service)
        .ok_or_else(|| DeployError::Service(request.service.clone()))?;
    let mut service = registry
        .generations
        .iter()
        .filter(|generation| same_scope(generation, service_config))
        .cloned()
        .collect::<Vec<_>>();
    service.sort_by(|left, right| {
        right
            .created_at_ms
            .cmp(&left.created_at_ms)
            .then_with(|| right.digest.cmp(&left.digest))
    });
    let removed = service
        .into_iter()
        .filter(|generation| !generation.active)
        .skip(request.keep)
        .collect::<Vec<_>>();
    if !request.dry && !removed.is_empty() {
        registry.generations.retain(|generation| {
            !removed.iter().any(|removed| {
                removed.scope == generation.scope
                    && removed.service == generation.service
                    && removed.digest == generation.digest
            })
        });
        for generation in &removed {
            registry.audit(AuditEvent {
                action: "prune".into(),
                service: generation.service.clone(),
                scope: generation.scope.clone(),
                digest: Some(generation.digest.clone()),
                created_at_ms: now_ms()?,
            });
        }
        registry.save()?;
    }
    Ok(PruneReport {
        removed,
        dry: request.dry,
    })
}

fn ensure_service(config: &ValidatedConfig, service: Option<&str>) -> Result<(), DeployError> {
    if let Some(service) = service
        && !config.raw.services.iter().any(|item| item.name == service)
    {
        return Err(DeployError::Service(service.to_string()));
    }
    Ok(())
}

fn same_scope(generation: &Generation, service: &peren_config::Service) -> bool {
    if generation.scope.is_empty() {
        generation.service == service.name
            && generation.tenant.is_none()
            && generation.project.is_none()
            && service.tenant_id.is_none()
            && service.project_id.is_none()
    } else {
        generation.scope == service.isolation_scope()
    }
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(super::process::default_data, PathBuf::from)
        .join("deploy")
}

#[derive(Debug, Error)]
pub enum DeployError {
    #[error("service {0:?} is not present in the fleet config")]
    Service(String),
    #[error("service {0:?} has no inactive generation to roll back to")]
    RollbackTarget(String),
    #[error("service {service:?} has no deployment generation {digest:?}")]
    Generation { service: String, digest: String },
    #[error("service {0:?} has no active deployment generation")]
    NoActive(String),
    #[error("service {0:?} has more than one active deployment generation")]
    MultipleActive(String),
    #[error("service {0:?} has a preview generation marked active")]
    ActivePreview(String),
    #[error("service {service:?} artifact digest drifted: recorded {recorded}, current {current}")]
    Drift {
        service: String,
        recorded: String,
        current: String,
    },
    #[error(
        "service {service:?} source map digest drifted: recorded {recorded:?}, current {current:?}"
    )]
    MapDrift {
        service: String,
        recorded: Option<String>,
        current: Option<String>,
    },
    #[error(
        "service {service:?} descriptor digest drifted: recorded {recorded}, current {current}"
    )]
    DescriptorDrift {
        service: String,
        recorded: String,
        current: String,
    },
    #[error(
        "service {service:?} deployment generation {digest:?} uses unsupported descriptor schema {schema}"
    )]
    DescriptorSchema {
        service: String,
        digest: String,
        schema: u16,
    },
    #[error(
        "service {service:?} deployment generation {digest:?} requires unsupported feature {feature:?}"
    )]
    DescriptorFeature {
        service: String,
        digest: String,
        feature: String,
    },
    #[error("service {service:?} deployment generation {digest:?} has no descriptor digest")]
    DescriptorMissing { service: String, digest: String },
    #[error("system clock cannot produce a valid deployment timestamp")]
    Time,
    #[error("failed to read source map {path:?}")]
    MapRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read deployment registry {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create deployment registry directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write deployment registry {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Bundle(#[from] ProcessError),
}
