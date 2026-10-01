use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use peren_config::ValidatedConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Environment;

const FILE: &str = "tenants.json";

#[derive(Debug)]
pub struct Revoke {
    pub tenant: String,
    pub reason: Option<String>,
}

#[derive(Debug)]
pub struct Delete {
    pub tenant: String,
    pub reason: Option<String>,
}

#[derive(Debug)]
pub struct RevokeReport {
    pub revocation: Revocation,
    pub audit: Audit,
}

#[derive(Debug)]
pub struct DeleteReport {
    pub deletion: Deletion,
    pub audit: Audit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Revocation {
    pub tenant: String,
    pub reason: Option<String>,
    pub revoked_at_ms: i64,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Deletion {
    pub tenant: String,
    pub reason: Option<String>,
    pub deleted_at_ms: i64,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Audit {
    pub tenant: String,
    pub action: Action,
    pub reason: Option<String>,
    pub recorded_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Revoke,
    Delete,
}

pub fn revoke(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: Revoke,
) -> Result<RevokeReport, TenantError> {
    ensure_declared(config, &request.tenant)?;
    let mut registry = Registry::load(root(environment))?;
    if registry.deleted(&request.tenant) {
        return Err(TenantError::Deleted(request.tenant));
    }
    let reason = request.reason.filter(|reason| !reason.trim().is_empty());
    let timestamp = now_ms()?;
    let revocation = Revocation {
        tenant: request.tenant.clone(),
        reason: reason.clone(),
        revoked_at_ms: timestamp,
        active: true,
    };
    let audit = Audit {
        tenant: request.tenant,
        action: Action::Revoke,
        reason,
        recorded_at_ms: timestamp,
    };
    registry.insert_revocation(revocation.clone());
    registry.audit.push(audit.clone());
    registry.save()?;
    Ok(RevokeReport { revocation, audit })
}

pub fn delete(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: Delete,
) -> Result<DeleteReport, TenantError> {
    ensure_declared(config, &request.tenant)?;
    let mut registry = Registry::load(root(environment))?;
    let reason = request.reason.filter(|reason| !reason.trim().is_empty());
    let timestamp = now_ms()?;
    let deletion = Deletion {
        tenant: request.tenant.clone(),
        reason: reason.clone(),
        deleted_at_ms: timestamp,
        active: true,
    };
    let audit = Audit {
        tenant: request.tenant,
        action: Action::Delete,
        reason,
        recorded_at_ms: timestamp,
    };
    registry.insert_deletion(deletion.clone());
    registry.audit.push(audit.clone());
    registry.save()?;
    Ok(DeleteReport { deletion, audit })
}

fn ensure_declared(config: &ValidatedConfig, tenant: &str) -> Result<(), TenantError> {
    if config
        .raw
        .tenants
        .iter()
        .any(|declared| declared.id == tenant)
    {
        Ok(())
    } else {
        Err(TenantError::Unknown(tenant.to_string()))
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Registry {
    #[serde(default)]
    revocations: Vec<Revocation>,
    #[serde(default)]
    deletions: Vec<Deletion>,
    #[serde(default)]
    audit: Vec<Audit>,
    #[serde(skip)]
    root: PathBuf,
}

impl Registry {
    fn load(root: PathBuf) -> Result<Self, TenantError> {
        let path = root.join(FILE);
        match fs::read(&path) {
            Ok(bytes) => {
                let mut registry: Registry = serde_json::from_slice(&bytes)?;
                registry.root = root;
                Ok(registry)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                revocations: Vec::new(),
                deletions: Vec::new(),
                audit: Vec::new(),
                root,
            }),
            Err(source) => Err(TenantError::Read { path, source }),
        }
    }

    fn save(&self) -> Result<(), TenantError> {
        fs::create_dir_all(&self.root).map_err(|source| TenantError::Create {
            path: self.root.clone(),
            source,
        })?;
        let path = self.root.join(FILE);
        fs::write(&path, serde_json::to_vec_pretty(self)?)
            .map_err(|source| TenantError::Write { path, source })
    }

    fn insert_revocation(&mut self, revocation: Revocation) {
        for existing in &mut self.revocations {
            if existing.tenant == revocation.tenant {
                existing.active = false;
            }
        }
        self.revocations.push(revocation);
        self.revocations.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| right.revoked_at_ms.cmp(&left.revoked_at_ms))
        });
    }

    fn insert_deletion(&mut self, deletion: Deletion) {
        for existing in &mut self.deletions {
            if existing.tenant == deletion.tenant {
                existing.active = false;
            }
        }
        for existing in &mut self.revocations {
            if existing.tenant == deletion.tenant {
                existing.active = false;
            }
        }
        self.deletions.push(deletion);
        self.deletions.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| right.deleted_at_ms.cmp(&left.deleted_at_ms))
        });
    }

    fn deleted(&self, tenant: &str) -> bool {
        self.deletions
            .iter()
            .any(|deletion| deletion.tenant == tenant && deletion.active)
    }
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(super::process::default_data, PathBuf::from)
        .join("tenants")
}

fn now_ms() -> Result<i64, TenantError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TenantError::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| TenantError::Time)
}

#[derive(Debug, Error)]
pub enum TenantError {
    #[error("tenant {0:?} is not declared by the fleet config")]
    Unknown(String),
    #[error("tenant {0:?} has been deleted")]
    Deleted(String),
    #[error("system clock cannot produce a valid tenant timestamp")]
    Time,
    #[error("failed to read tenant registry {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create tenant registry directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write tenant registry {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::DataEnv;
    use peren_config::FleetConfig;
    use uuid::Uuid;

    #[test]
    fn revoke_records_active_tenant_revocation() {
        let temp = std::env::temp_dir().join(format!("peren-tenant-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let config = config(&temp.join("worker.js"));
        let env = DataEnv::new(temp.join("data"));

        revoke(
            &config,
            &env,
            Revoke {
                tenant: "acme".into(),
                reason: Some("abuse".into()),
            },
        )
        .unwrap();
        let second = revoke(
            &config,
            &env,
            Revoke {
                tenant: "acme".into(),
                reason: Some("billing".into()),
            },
        )
        .unwrap();

        assert_eq!(second.revocation.reason.as_deref(), Some("billing"));
        assert_eq!(second.audit.action, Action::Revoke);
        let registry = Registry::load(env.path().join("tenants")).unwrap();
        assert_eq!(
            registry
                .revocations
                .iter()
                .filter(|item| item.active)
                .count(),
            1
        );
        assert_eq!(registry.audit.len(), 2);
        assert!(matches!(
            revoke(
                &config,
                &env,
                Revoke {
                    tenant: "missing".into(),
                    reason: None,
                },
            ),
            Err(TenantError::Unknown(tenant)) if tenant == "missing"
        ));
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn delete_records_tombstone_audit_and_blocks_future_revocation() {
        let temp = std::env::temp_dir().join(format!("peren-tenant-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let config = config(&temp.join("worker.js"));
        let env = DataEnv::new(temp.join("data"));

        revoke(
            &config,
            &env,
            Revoke {
                tenant: "acme".into(),
                reason: Some("risk".into()),
            },
        )
        .unwrap();
        let report = delete(
            &config,
            &env,
            Delete {
                tenant: "acme".into(),
                reason: Some("contract ended".into()),
            },
        )
        .unwrap();

        assert_eq!(report.deletion.tenant, "acme");
        assert_eq!(report.audit.action, Action::Delete);
        let registry = Registry::load(env.path().join("tenants")).unwrap();
        assert!(registry.deleted("acme"));
        assert_eq!(
            registry
                .revocations
                .iter()
                .filter(|item| item.active)
                .count(),
            0
        );
        assert_eq!(registry.audit.len(), 2);
        assert!(matches!(
            revoke(
                &config,
                &env,
                Revoke {
                    tenant: "acme".into(),
                    reason: None,
                },
            ),
            Err(TenantError::Deleted(tenant)) if tenant == "acme"
        ));
        fs::remove_dir_all(temp).unwrap();
    }

    fn config(bundle: &std::path::Path) -> ValidatedConfig {
        fs::write(
            bundle,
            "export default { fetch() { return new Response('ok') } };",
        )
        .unwrap();
        let text = format!(
            r#"
[node]
node_id = "{}"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"

[bucket]
kind = "memory"

[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"

[[tenants]]
id = "acme"
cell_quota = 10

[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2024-01-01"
tenant_id = "acme"
"#,
            Uuid::new_v4(),
            bundle.display()
        );
        toml::from_str::<FleetConfig>(&text)
            .unwrap()
            .validate()
            .unwrap()
    }
}
