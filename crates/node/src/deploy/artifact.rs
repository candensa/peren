use std::{fs, path::Path};

use peren_config::{Service as ConfigService, ServiceDescriptor};
use peren_runtime::{ModuleKind, WorkerBundle};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{DeployError, Generation};

pub(super) const SUPPORTED_DESCRIPTOR_SCHEMA: u16 = 1;
const SUPPORTED_REQUIRED_FEATURES: &[&str] = &[];

#[derive(Serialize)]
struct DeploymentDescriptor {
    schema: u16,
    required_features: Vec<String>,
    service: ServiceDescriptor,
    artifact: ArtifactDescriptor,
}

#[derive(Serialize)]
struct ArtifactDescriptor {
    bundle_digest: String,
    entry: String,
    modules: Vec<ModuleDescriptor>,
    source_maps: usize,
    source_map_digest: Option<String>,
}

#[derive(Serialize)]
struct ModuleDescriptor {
    name: String,
    kind: &'static str,
}

pub(super) fn descriptor_digest(
    service: &ConfigService,
    bundle: &WorkerBundle,
    maps: &Maps,
) -> String {
    let descriptor = DeploymentDescriptor {
        schema: SUPPORTED_DESCRIPTOR_SCHEMA,
        required_features: Vec::new(),
        service: ServiceDescriptor::from(service),
        artifact: ArtifactDescriptor {
            bundle_digest: bundle.digest().to_string(),
            entry: bundle.entry().to_string(),
            modules: bundle
                .modules()
                .map(|(name, module)| ModuleDescriptor {
                    name: name.to_string(),
                    kind: match module.kind() {
                        ModuleKind::JavaScript => "javascript",
                        ModuleKind::CommonJs => "commonjs",
                        ModuleKind::Wasm => "wasm",
                        ModuleKind::Python => "python",
                    },
                })
                .collect(),
            source_maps: maps.count,
            source_map_digest: maps.digest.clone(),
        },
    };
    let bytes = serde_json::to_vec(&descriptor).expect("deployment descriptor is serializable");
    let mut digest = Sha256::new();
    digest.update(b"peren-deployment-descriptor-v1\0");
    hash(&mut digest, &bytes);
    hex::encode(digest.finalize())
}

pub(super) fn verify_descriptor(
    service: &ConfigService,
    bundle: &WorkerBundle,
    maps: &Maps,
    generation: &Generation,
) -> Result<(), DeployError> {
    if generation.descriptor_schema != SUPPORTED_DESCRIPTOR_SCHEMA {
        return Err(DeployError::DescriptorSchema {
            service: service.name.clone(),
            digest: generation.digest.clone(),
            schema: generation.descriptor_schema,
        });
    }
    for feature in &generation.required_features {
        if !SUPPORTED_REQUIRED_FEATURES.contains(&feature.as_str()) {
            return Err(DeployError::DescriptorFeature {
                service: service.name.clone(),
                digest: generation.digest.clone(),
                feature: feature.clone(),
            });
        }
    }
    let current = descriptor_digest(service, bundle, maps);
    match &generation.descriptor_digest {
        Some(recorded) if recorded == &current => Ok(()),
        Some(recorded) => Err(DeployError::DescriptorDrift {
            service: service.name.clone(),
            recorded: recorded.clone(),
            current,
        }),
        None => Err(DeployError::DescriptorMissing {
            service: service.name.clone(),
            digest: generation.digest.clone(),
        }),
    }
}

pub(super) struct Maps {
    pub(super) count: usize,
    pub(super) digest: Option<String>,
}

pub(super) fn source_maps(service: &ConfigService) -> Result<Maps, DeployError> {
    if service.source_maps.is_empty() {
        return Ok(Maps {
            count: 0,
            digest: None,
        });
    }
    let mut digest = Sha256::new();
    digest.update(b"peren-source-maps-v1\0");
    for (name, path) in &service.source_maps {
        hash(&mut digest, name.as_bytes());
        hash(&mut digest, &read(path)?);
    }
    Ok(Maps {
        count: service.source_maps.len(),
        digest: Some(hex::encode(digest.finalize())),
    })
}

fn read(path: &Path) -> Result<Vec<u8>, DeployError> {
    fs::read(path).map_err(|source| DeployError::MapRead {
        path: path.to_path_buf(),
        source,
    })
}

fn hash(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .expect("source map limits fit in u64")
            .to_be_bytes(),
    );
    digest.update(value);
}
