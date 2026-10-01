use peren_cell::OwnershipLease;
use peren_primitives::{CellId, NodeId};
use thiserror::Error;

use crate::{Environment, NodeRepository, ProviderError, Providers};

pub struct Diagnose {
    pub storage: bool,
    pub readonly: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Report {
    pub node: NodeId,
    pub services: usize,
    pub sockets: usize,
    pub storage: Storage,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Storage {
    Skipped,
    ReadOnly,
    Writable,
}

pub async fn run(
    config: peren_config::ValidatedConfig,
    environment: &impl Environment,
    command: Diagnose,
) -> Result<Report, DiagnoseError> {
    let node = NodeId::from_uuid(config.raw.node.id);
    let services = config.raw.services.len();
    let sockets = config.sockets.len();
    let providers = Providers::build(&config, environment).await?;
    let storage = if command.storage {
        if command.readonly {
            Storage::ReadOnly
        } else {
            probe_storage(node, &providers.repository).await?;
            Storage::Writable
        }
    } else {
        Storage::Skipped
    };
    Ok(Report {
        node,
        services,
        sockets,
        storage,
    })
}

async fn probe_storage(node: NodeId, repository: &crate::Repository) -> Result<(), DiagnoseError> {
    let lease = repository.acquire(node, probe_cell()).await?;
    lease.verify().await?;
    lease.release().await?;
    Ok(())
}

fn probe_cell() -> CellId {
    use sha2::{Digest, Sha256};

    CellId::from_bytes(Sha256::digest(b"peren-diagnose-probe-v1").into())
}

#[derive(Debug, Error)]
pub enum DiagnoseError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Ownership(#[from] peren_fleet::RepositoryError),
    #[error(transparent)]
    Lease(#[from] peren_cell::LeaseError),
}

#[cfg(test)]
mod tests {

    use peren_config::FleetConfig;

    use super::*;

    struct TestEnvironment;

    impl Environment for TestEnvironment {
        fn get(&self, _name: &str) -> Option<String> {
            None
        }
    }

    fn config() -> peren_config::ValidatedConfig {
        FleetConfig::from_toml(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
        )
        .unwrap()
        .validate()
        .unwrap()
    }

    #[tokio::test]
    async fn reports_validated_fleet_without_storage_probe() {
        let report = run(
            config(),
            &TestEnvironment,
            Diagnose {
                storage: false,
                readonly: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.services, 1);
        assert_eq!(report.sockets, 1);
        assert_eq!(report.storage, Storage::Skipped);
    }

    #[tokio::test]
    async fn storage_probe_uses_the_ownership_repository() {
        let report = run(
            config(),
            &TestEnvironment,
            Diagnose {
                storage: true,
                readonly: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.storage, Storage::Writable);
    }

    #[tokio::test]
    async fn readonly_storage_check_does_not_create_a_probe_lease() {
        let report = run(
            config(),
            &TestEnvironment,
            Diagnose {
                storage: true,
                readonly: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.storage, Storage::ReadOnly);
    }

    #[tokio::test]
    async fn provider_errors_are_reported_without_secret_values() {
        let mut config = config();
        config.raw.services[0]
            .secrets
            .insert("TOKEN".into(), "MISSING_TOKEN".into());
        let error = run(
            config,
            &TestEnvironment,
            Diagnose {
                storage: false,
                readonly: false,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "required environment variable \"MISSING_TOKEN\" is not set"
        );
    }
}
