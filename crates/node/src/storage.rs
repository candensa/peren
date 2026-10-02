use peren_primitives::CellId;
use peren_provider_object_store::{ReplicaDeleteReport, ReplicaPruneReport};
use thiserror::Error;

use crate::{Environment, ProviderError, Providers};

#[derive(Debug)]
pub struct Prune {
    pub dry_run: bool,
    pub retention_secs: u64,
}

#[derive(Debug)]
pub struct EraseCell {
    pub cell: CellId,
    pub dry_run: bool,
}

pub async fn prune_replicas(
    config: peren_config::ValidatedConfig,
    environment: &impl Environment,
    request: Prune,
) -> Result<ReplicaPruneReport, Error> {
    let providers = Providers::build(&config, environment).await?;
    providers
        .repository
        .prune_replicas(request.dry_run, request.retention_secs)
        .await
        .map_err(Error::Replica)
}

pub async fn erase_cell_replicas(
    config: peren_config::ValidatedConfig,
    environment: &impl Environment,
    request: EraseCell,
) -> Result<ReplicaDeleteReport, Error> {
    let providers = Providers::build(&config, environment).await?;
    providers
        .repository
        .delete_cell_replicas(request.cell, request.dry_run)
        .await
        .map_err(Error::Replica)
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Replica(#[from] peren_replication::RepositoryError),
}
