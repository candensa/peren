use std::{future::Future, num::NonZeroUsize, path::Path, pin::Pin, sync::Arc};

use peren_bindings::SharedStorageHost;
use peren_primitives::{CellId, NodeId, OwnershipEpoch};
use peren_replication::{
    DurableReceipt, ReplicaPayload, ReplicaRepository, Replicator, RepositoryError,
};
use peren_runtime::{
    Capabilities, DurableStorageHost, EngineError, HttpRequest, HttpResponse, InvocationLimits,
    IsolateLimits, QueueDispatch, QueueEvent, R2BucketHost, ScheduledEvent, TailEvent,
    WebSocketCloseEvent, WebSocketDispatch, WebSocketMessageEvent, WorkerBundle, WorkerEnvironment,
    WorkerLogEvent, WorkerRuntime, WorkflowEvent,
};
use peren_storage::{CellStorage, StorageError};
use thiserror::Error;
use tokio::sync::Mutex;

pub trait OwnershipLease: Send + Sync {
    fn cell(&self) -> CellId;
    fn owner(&self) -> NodeId;
    fn epoch(&self) -> OwnershipEpoch;
    fn verify(&self) -> impl Future<Output = Result<(), LeaseError>> + Send;
    fn release(self) -> impl Future<Output = Result<(), LeaseError>> + Send;
}

const REPLICA_RETAINED_GENERATIONS: NonZeroUsize = NonZeroUsize::MIN;

pub struct WorkerCell<L, R> {
    lease: L,
    storage: Arc<Mutex<CellStorage>>,
    replicator: Replicator<R>,
    runtime: WorkerRuntime,
    wal_offset: u64,
    wal_bytes_since_generation: u64,
    generation: peren_primitives::StorageRevision,
    last_commit: Option<CellCommitSummary>,
    state: CellState,
}

impl<L: OwnershipLease, R: ReplicaRepository> WorkerCell<L, R> {
    pub async fn activate(
        path: &Path,
        lease: L,
        repository: R,
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
    ) -> Result<Self, CellError> {
        Self::activate_with_host(
            path,
            lease,
            repository,
            bundle,
            limits,
            environment,
            |storage| Arc::new(SharedStorageHost::new(storage)),
        )
        .await
    }

    pub async fn activate_with_host<F>(
        path: &Path,
        lease: L,
        repository: R,
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        host: F,
    ) -> Result<Self, CellError>
    where
        F: FnOnce(Arc<Mutex<CellStorage>>) -> Arc<dyn DurableStorageHost>,
    {
        let storage = Arc::new(Mutex::new(CellStorage::open(path)?));
        let runtime = WorkerRuntime::load_with_environment(
            bundle,
            limits,
            environment,
            host(Arc::clone(&storage)),
        )
        .await?;
        Ok(Self {
            lease,
            storage,
            replicator: Replicator::new(repository),
            runtime,
            wal_offset: 0,
            wal_bytes_since_generation: 0,
            generation: peren_primitives::StorageRevision::default(),
            last_commit: None,
            state: CellState::Active,
        })
    }

    pub async fn activate_with_r2_host<F, H>(
        path: &Path,
        lease: L,
        repository: R,
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        host: F,
    ) -> Result<Self, CellError>
    where
        F: FnOnce(Arc<Mutex<CellStorage>>) -> Arc<H>,
        H: DurableStorageHost + R2BucketHost + 'static,
    {
        let storage = Arc::new(Mutex::new(CellStorage::open(path)?));
        let host = host(Arc::clone(&storage));
        let runtime =
            WorkerRuntime::load_with_r2(bundle, limits, environment, host.clone(), host).await?;
        Ok(Self {
            lease,
            storage,
            replicator: Replicator::new(repository),
            runtime,
            wal_offset: 0,
            wal_bytes_since_generation: 0,
            generation: peren_primitives::StorageRevision::default(),
            last_commit: None,
            state: CellState::Active,
        })
    }

    pub async fn activate_with_capabilities<F, H>(
        path: &Path,
        lease: L,
        repository: R,
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        host: F,
    ) -> Result<Self, CellError>
    where
        F: FnOnce(Arc<Mutex<CellStorage>>) -> Arc<H>,
        H: DurableStorageHost
            + peren_runtime::OutboundFetchHost
            + peren_runtime::QueueProducerHost
            + R2BucketHost
            + peren_runtime::ServiceBindingHost
            + peren_runtime::DurableObjectHost
            + peren_runtime::CacheHost
            + peren_runtime::KvHost
            + peren_runtime::AiHost
            + 'static,
    {
        let storage = Arc::new(Mutex::new(CellStorage::open(path)?));
        let host = host(Arc::clone(&storage));
        let runtime = WorkerRuntime::load_with_capabilities(
            bundle,
            limits,
            environment,
            Capabilities {
                storage: host.clone(),
                fetch: Some(host.clone()),
                queue: Some(host.clone()),
                r2: Some(host.clone()),
                service: Some(host.clone()),
                durable: Some(host.clone()),
                cache: Some(host.clone()),
                kv: Some(host.clone()),
                ai: Some(host),
            },
        )
        .await?;
        Ok(Self {
            lease,
            storage,
            replicator: Replicator::new(repository),
            runtime,
            wal_offset: 0,
            wal_bytes_since_generation: 0,
            generation: peren_primitives::StorageRevision::default(),
            last_commit: None,
            state: CellState::Active,
        })
    }

    pub fn bind_durable_class(&mut self, class_name: &str) -> Result<(), CellError> {
        self.runtime.bind_durable_class(class_name)?;
        Ok(())
    }

    pub fn bind_durable_object_context(
        &mut self,
        namespace: &str,
        id: &str,
        name: Option<&str>,
        props: &serde_json::Value,
    ) -> Result<(), CellError> {
        self.runtime
            .bind_durable_object_context(namespace, id, name, props)?;
        Ok(())
    }

    pub async fn dispatch_http(
        &mut self,
        request: HttpRequest,
        limits: InvocationLimits,
    ) -> Result<HttpResponse, CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_http(request, limits)))
            .await
    }

    pub fn take_console_events(&mut self) -> Vec<WorkerLogEvent> {
        self.runtime.take_console_events()
    }

    #[must_use]
    pub const fn last_commit(&self) -> Option<CellCommitSummary> {
        self.last_commit
    }

    pub async fn dispatch_alarm(&mut self) -> Result<(), CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_alarm()))
            .await
    }

    pub async fn dispatch_scheduled(&mut self, event: ScheduledEvent) -> Result<(), CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_scheduled(event)))
            .await
    }

    pub async fn dispatch_queue(&mut self, event: QueueEvent) -> Result<QueueDispatch, CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_queue(event)))
            .await
    }

    pub async fn dispatch_tail(&mut self, event: TailEvent) -> Result<(), CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_tail(event)))
            .await
    }

    pub async fn dispatch_websocket_message(
        &mut self,
        event: WebSocketMessageEvent,
    ) -> Result<WebSocketDispatch, CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_websocket_message(event)))
            .await
    }

    pub async fn dispatch_websocket_close(
        &mut self,
        event: WebSocketCloseEvent,
    ) -> Result<WebSocketDispatch, CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_websocket_close(event)))
            .await
    }

    pub async fn dispatch_workflow(&mut self, event: WorkflowEvent) -> Result<(), CellError> {
        self.dispatch_with_lifecycle(|runtime| Box::pin(runtime.dispatch_workflow(event)))
            .await
    }

    async fn dispatch_with_lifecycle<T>(
        &mut self,
        dispatch: impl for<'a> FnOnce(
            &'a mut WorkerRuntime,
        )
            -> Pin<Box<dyn Future<Output = Result<T, EngineError>> + 'a>>,
    ) -> Result<T, CellError> {
        self.ensure_active()?;
        self.last_commit = None;
        self.verify().await?;
        let result = dispatch(&mut self.runtime).await?;
        let revision = self.runtime.committed_revision();
        if revision.get() > 0 {
            self.publish(revision).await?;
        }
        self.verify().await?;
        Ok(result)
    }

    pub async fn checkpoint(&mut self) -> Result<(), CellError> {
        self.ensure_active()?;
        self.last_commit = None;
        self.verify().await?;
        let checkpoint = self.storage.lock().await.checkpoint()?;
        if let Err(error) = self
            .replicator
            .checkpoint(
                self.lease.cell(),
                self.lease.epoch(),
                checkpoint.revision,
                &checkpoint.database,
            )
            .await
        {
            self.state = if matches!(error, RepositoryError::Fenced) {
                CellState::Fenced
            } else {
                CellState::Draining
            };
            return Err(error.into());
        }
        self.generation = checkpoint.revision;
        self.wal_offset = 0;
        self.wal_bytes_since_generation = 0;
        self.last_commit = Some(CellCommitSummary {
            revision: checkpoint.revision,
            receipt: DurableReceipt::new(
                self.lease.cell(),
                self.lease.epoch(),
                checkpoint.revision,
                checkpoint.revision,
            ),
            wal_bytes: 0,
        });
        self.prune().await?;
        self.verify().await
    }

    pub async fn checkpoint_if_wal_exceeds(
        &mut self,
        threshold_bytes: u64,
    ) -> Result<(), CellError> {
        if threshold_bytes == 0 {
            return Ok(());
        }
        if self.wal_bytes_since_generation >= threshold_bytes {
            self.checkpoint().await?;
        }
        Ok(())
    }

    pub async fn delete(mut self) -> Result<(), CellError> {
        self.ensure_active()?;
        self.verify().await?;
        self.storage.lock().await.purge()?;
        self.state = CellState::Deleted;
        self.lease.release().await?;
        Ok(())
    }

    pub async fn release(mut self) -> Result<(), CellError> {
        self.state = CellState::Draining;
        self.lease.release().await?;
        Ok(())
    }

    async fn prune(&mut self) -> Result<(), CellError> {
        match self
            .replicator
            .prune(self.lease.cell(), REPLICA_RETAINED_GENERATIONS)
            .await
        {
            Ok(_) => Ok(()),
            Err(RepositoryError::Fenced) => {
                self.state = CellState::Fenced;
                Err(RepositoryError::Fenced.into())
            }
            Err(error) => {
                self.state = CellState::Draining;
                Err(error.into())
            }
        }
    }

    #[must_use]
    pub const fn state(&self) -> CellState {
        self.state
    }

    fn ensure_active(&self) -> Result<(), CellError> {
        (self.state == CellState::Active)
            .then_some(())
            .ok_or(CellError::Inactive(self.state))
    }

    async fn verify(&mut self) -> Result<(), CellError> {
        if self.lease.verify().await.is_err() {
            self.state = CellState::Fenced;
            return Err(LeaseError.into());
        }
        Ok(())
    }

    async fn publish(
        &mut self,
        revision: peren_primitives::StorageRevision,
    ) -> Result<(), CellError> {
        self.verify().await?;
        let replica = self
            .storage
            .lock()
            .await
            .replica(self.wal_offset)
            .inspect_err(|_| {
                self.state = CellState::Draining;
            })?;
        let offset = replica.offset;
        let wal_bytes = replica.frames.len();
        let receipt = self
            .replicator
            .publish(
                self.lease.cell(),
                self.lease.epoch(),
                revision,
                &ReplicaPayload {
                    generation: self.generation,
                    database: replica.database,
                    wal_header: replica.header,
                    wal_frames: replica.frames,
                },
            )
            .await
            .map_err(|error| {
                self.state = if matches!(error, RepositoryError::Fenced) {
                    CellState::Fenced
                } else {
                    CellState::Draining
                };
                error
            })?;
        if receipt.cell() != self.lease.cell()
            || receipt.epoch() != self.lease.epoch()
            || receipt.generation() != self.generation
            || receipt.through() < revision
        {
            self.state = CellState::Draining;
            return Err(CellError::InvalidReceipt);
        }
        self.wal_offset = offset;
        self.wal_bytes_since_generation = self
            .wal_bytes_since_generation
            .saturating_add(u64::try_from(wal_bytes).unwrap_or(u64::MAX));
        self.last_commit = Some(CellCommitSummary {
            revision,
            receipt,
            wal_bytes,
        });
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellCommitSummary {
    pub revision: peren_primitives::StorageRevision,
    pub receipt: DurableReceipt,
    pub wal_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CellState {
    Active,
    Deleted,
    Draining,
    Fenced,
}

#[derive(Debug, Error)]
#[error("ownership lease is no longer valid")]
pub struct LeaseError;

#[derive(Debug, Error)]
pub enum CellError {
    #[error("cell is not active: {0:?}")]
    Inactive(CellState),
    #[error("replication returned a receipt for a different durability boundary")]
    InvalidReceipt,
    #[error(transparent)]
    Lease(#[from] LeaseError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

impl CellError {
    #[must_use]
    pub fn is_non_retryable(&self) -> bool {
        matches!(self, Self::Engine(error) if error.is_non_retryable())
    }
}
