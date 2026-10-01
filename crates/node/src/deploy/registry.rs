use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};

use super::{AuditEvent, DeployError, Generation};

const FILE: &str = "deployments.json";

#[derive(Debug, Default, Deserialize, Serialize)]
pub(super) struct Registry {
    pub(super) generations: Vec<Generation>,
    #[serde(default)]
    pub(super) audit: Vec<AuditEvent>,
    #[serde(skip)]
    pub(super) root: PathBuf,
}

impl Registry {
    pub(super) fn load(root: PathBuf) -> Result<Self, DeployError> {
        let path = root.join(FILE);
        match fs::read(&path) {
            Ok(bytes) => {
                let mut registry: Registry = serde_json::from_slice(&bytes)?;
                registry.root = root;
                Ok(registry)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                generations: Vec::new(),
                audit: Vec::new(),
                root,
            }),
            Err(source) => Err(DeployError::Read { path, source }),
        }
    }

    pub(super) fn save(&self) -> Result<(), DeployError> {
        fs::create_dir_all(&self.root).map_err(|source| DeployError::Create {
            path: self.root.clone(),
            source,
        })?;
        let path = self.root.join(FILE);
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&path, bytes).map_err(|source| DeployError::Write { path, source })
    }

    pub(super) fn insert(&mut self, mut generation: Generation) -> Generation {
        for existing in &mut self.generations {
            if !generation.preview && existing.service == generation.service {
                existing.active = false;
            }
        }
        if let Some(existing) = self.generations.iter_mut().find(|existing| {
            existing.service == generation.service && existing.digest == generation.digest
        }) {
            generation.created_at_ms = existing.created_at_ms;
            *existing = generation.clone();
        } else {
            self.generations.push(generation.clone());
        }
        self.sort();
        generation
    }

    pub(super) fn audit(&mut self, event: AuditEvent) {
        self.audit.push(event);
        self.audit.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.action.cmp(&right.action))
                .then_with(|| left.service.cmp(&right.service))
                .then_with(|| left.digest.cmp(&right.digest))
        });
    }

    pub(super) fn sort(&mut self) {
        self.generations.sort_by(|left, right| {
            left.service
                .cmp(&right.service)
                .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
                .then_with(|| left.digest.cmp(&right.digest))
        });
    }
}
