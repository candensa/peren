use peren_cell::OwnershipLease;
use peren_primitives::{CellId, NodeId, OwnershipEpoch, StorageRevision};
use peren_replication::ReplicaRepository;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{Environment, NodeRepository, ProviderError, Providers};

#[derive(Debug)]
pub struct Storage;

#[derive(Debug, Eq, PartialEq)]
pub struct StorageReport {
    pub node: NodeId,
    pub cell: CellId,
    pub epoch: OwnershipEpoch,
    pub revision: StorageRevision,
    pub bytes: usize,
    pub range_read: bool,
}

pub async fn storage(
    config: peren_config::ValidatedConfig,
    environment: &impl Environment,
    _request: Storage,
) -> Result<StorageReport, Error> {
    let node = NodeId::from_uuid(config.raw.node.id);
    let providers = Providers::build(&config, environment).await?;
    let cell = cell(node);
    let lease = providers.repository.acquire(node, cell).await?;
    lease.verify().await?;
    let epoch = lease.epoch();
    let revision = StorageRevision::new(1);
    let database = payload(node, cell);
    providers
        .repository
        .checkpoint(cell, epoch, revision, &database)
        .await?;
    let restored = providers
        .repository
        .restore(cell)
        .await?
        .ok_or(Error::Missing)?;
    if restored.epoch != epoch
        || restored.revision != revision
        || restored.database != database
        || !restored.wal.is_empty()
    {
        return Err(Error::Mismatch);
    }
    providers
        .repository
        .verify_range_read(&format!("storage-{}", node.as_uuid().simple()))
        .await?;
    lease.release().await?;
    Ok(StorageReport {
        node,
        cell,
        epoch,
        revision,
        bytes: database.len(),
        range_read: true,
    })
}

fn cell(node: NodeId) -> CellId {
    let mut digest = Sha256::new();
    digest.update(b"peren-conformance-storage-v1\0");
    digest.update(node.as_uuid().as_bytes());
    CellId::from_bytes(digest.finalize().into())
}

fn payload(node: NodeId, cell: CellId) -> Vec<u8> {
    let mut bytes = b"peren-storage-conformance-v1\0".to_vec();
    bytes.extend_from_slice(node.as_uuid().as_bytes());
    bytes.extend_from_slice(cell.as_bytes());
    bytes
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Ownership(#[from] peren_fleet::RepositoryError),
    #[error(transparent)]
    Lease(#[from] peren_cell::LeaseError),
    #[error(transparent)]
    Replica(#[from] peren_replication::RepositoryError),
    #[error("storage conformance restore did not return the probe replica")]
    Missing,
    #[error("storage conformance restore returned different replica bytes")]
    Mismatch,
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

    #[tokio::test]
    async fn storage_conformance_checks_ownership_and_replica_restore() {
        let report = storage(config(), &TestEnvironment, Storage).await.unwrap();

        assert_eq!(report.revision, StorageRevision::new(1));
        assert!(report.bytes > 32);
        assert!(report.range_read);
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
"#,
        )
        .unwrap()
        .validate()
        .unwrap()
    }
}
