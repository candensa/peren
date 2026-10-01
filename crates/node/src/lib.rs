use std::{collections::HashSet, fs, path::PathBuf, sync::Arc};

use peren_cell::{CellError, OwnershipLease, WorkerCell};
use peren_primitives::{CellId, NodeId, OwnershipEpoch, StorageRevision};
use peren_provider_object_store::{BucketLease, BucketStore, MemoryStore, StoreLease};
use peren_replication::{
    DurableReceipt, ReplicaImage, ReplicaRepository, Replicator, RepositoryError as ReplicaError,
};
use peren_runtime::{
    HttpRequest, HttpResponse, InvocationLimits, IsolateLimits, QueueEvent, ScheduledEvent,
    TailEvent, WorkerBundle, WorkerEnvironment, WorkflowEvent,
};
use std::sync::Mutex;
use thiserror::Error;

mod admission;
mod asset;
mod backup;
mod bundle;
mod conformance;
mod console;
mod control;
mod d1;
mod deploy;
mod development;
mod diagnose;
mod fleet;
mod host;
mod kv;
mod metrics;
mod process;
mod provider;
mod queue;
mod secret;
mod storage;
mod supervisor;
mod tail;
mod tenant;
mod tls;
mod trace;
mod upgrade;
mod websocket;
mod workflow;

pub use backup::{
    Backup, BackupReport, Error as BackupError, Restore, RestoreReport, Uninstall, UninstallReport,
    backup, restore, uninstall,
};
pub use conformance::{
    Error as ConformanceError, Storage as ConformanceStorage,
    StorageReport as ConformanceStorageReport, storage as conformance_storage,
};
pub use console::{
    Bootstrap as ConsoleBootstrap, BootstrapReport as ConsoleBootstrapReport,
    CAPABILITIES as CONSOLE_CAPABILITIES, Capability as ConsoleCapability,
    CapabilityStatus as ConsoleCapabilityStatus, ConsoleError, Register as ConsoleRegister,
    RegisterReport as ConsoleRegisterReport, bootstrap as bootstrap_console,
    register as register_console, unsupported_capabilities as unsupported_console_capabilities,
};
pub use d1::{
    Migration as D1Migration, MigrationReport as D1MigrationReport, Prune as D1Prune,
    PruneReport as D1PruneReport, Query as D1Query, QueryError as D1QueryError,
    QueryOutput as D1QueryOutput, migrate as migrate_d1, prune as prune_d1, query as query_d1,
};
pub use deploy::{
    DeployError, Deployment, Generation as DeployGeneration, Health as DeployHealth,
    HealthReport as DeployHealthReport, HealthyService as DeployHealthyService, List as DeployList,
    ListReport as DeployListReport, Prune as DeployPrune, PruneReport as DeployPruneReport,
    Record as DeployRecord, RecordReport as DeployRecordReport, Rollback as DeployRollback,
    RollbackReport as DeployRollbackReport, VerifiedGeneration as DeployVerifiedGeneration,
    Verify as DeployVerify, VerifyReport as DeployVerifyReport, health as health_deployments,
    list as list_deployments, prune as prune_deployments, record as record_deployment,
    rollback as rollback_deployment, verify as verify_deployments,
};
pub use development::{DevelopmentError, prepare_test_server};
pub use diagnose::{
    Diagnose, DiagnoseError, Report as DiagnoseReport, Storage as DiagnoseStorage, run as diagnose,
};
pub use fleet::{
    Drain as NodeDrain, Error as FleetError, Join as NodeJoin, Remove as NodeRemove,
    Report as NodeReport, State as NodeState, drain as drain_node, join as join_node,
    remove as remove_node,
};
pub use kv::{
    Import as KvImport, ImportError as KvImportError, ImportReport as KvImportReport,
    import as import_kv,
};
pub use process::{Process, ProcessError};
pub use provider::{Environment, ProcessEnvironment, ProviderError, Providers, Repository};
pub use queue::{
    Delivery as QueueDelivery, DeliveryError as QueueDeliveryError, Depth as QueueDepth,
    DepthReport as QueueDepthReport, EffectPublish as QueueEffectPublish, Pause as QueuePause,
    PauseReport as QueuePauseReport, Purge as QueuePurge, PurgeReport as QueuePurgeReport,
    QueueEffect, QueueEffectError, Redrive as QueueRedrive, RedriveReport as QueueRedriveReport,
    Report as QueueDeliveryReport, Resume as QueueResume, deliver as deliver_queue,
    depth as queue_depth, pause as queue_pause, publish_queue_effects,
    publish_queue_effects_with_lease, purge as queue_purge, redrive as queue_redrive,
    resume as queue_resume,
};
pub use secret::{
    DeleteReport as SecretDeleteReport, GetReport as SecretGetReport,
    ListReport as SecretListReport, Metadata as SecretMetadata, Rotate as SecretRotate,
    RotateReport as SecretRotateReport, SecretError, delete as delete_secret, get as get_secret,
    list as list_secrets, rotate as rotate_secret,
};
pub use storage::{
    DeleteCell as StorageDeleteCell, Error as StorageError, Prune as StoragePrune,
    delete_cell_replicas as delete_cell_storage, prune_replicas as prune_storage,
};
pub use supervisor::{Phase, Shutdown, Supervisor, SupervisorError, TaskError};
pub use tail::{
    ConsoleEvent as TailConsoleEvent, ConsoleLevel as TailConsoleLevel, Event as TailLogEvent,
    Level as TailLevel, Read as TailRead, Report as TailReport, RequestEvent as TailRequestEvent,
    TailError, read as read_tail,
};
pub use tenant::{
    Action as TenantAction, Audit as TenantAudit, Delete as TenantDelete,
    DeleteReport as TenantDeleteReport, Deletion as TenantDeletion, Revocation as TenantRevocation,
    Revoke as TenantRevoke, RevokeReport as TenantRevokeReport, TenantError,
    delete as delete_tenant, revoke as revoke_tenant,
};
pub use trace::{
    Event as TraceEvent, Read as TraceRead, Report as TraceReport, SpanKind as TraceSpanKind,
    TraceError, read as read_trace,
};
pub use upgrade::{
    Check as UpgradeCheck, CheckReport as UpgradeCheckReport, Plan as UpgradePlan,
    PlanReport as UpgradePlanReport, Step as UpgradeStep, check as check_upgrade,
    plan as plan_upgrade,
};
pub use workflow::{
    ActivityEffect as WorkflowActivityEffect, ActivityEffectError as WorkflowActivityEffectError,
    ActivityEffectPublish as WorkflowActivityEffectPublish, ActivityRun as WorkflowActivityRun,
    ActivityRunReport as WorkflowActivityRunReport, Cancel as WorkflowCancel,
    Report as WorkflowReport, State as WorkflowState, Status as WorkflowStatus,
    Target as WorkflowTarget, WorkflowError, cancel as cancel_workflow, delete as delete_workflow,
    publish_activity_effects as publish_workflow_activity_effects,
    publish_activity_effects_with_lease as publish_workflow_activity_effects_with_lease,
    run_activity as run_workflow_activity, status as workflow_status,
};

pub trait NodeRepository: ReplicaRepository + Clone + Send + Sync + 'static {
    type Lease: OwnershipLease;

    fn acquire(
        &self,
        local: NodeId,
        cell: CellId,
    ) -> impl Future<Output = Result<Self::Lease, peren_fleet::RepositoryError>> + Send;
}

impl NodeRepository for MemoryStore {
    type Lease = StoreLease;

    async fn acquire(
        &self,
        local: NodeId,
        cell: CellId,
    ) -> Result<Self::Lease, peren_fleet::RepositoryError> {
        self.acquire(local, cell).await
    }
}

impl NodeRepository for BucketStore {
    type Lease = BucketLease;

    async fn acquire(
        &self,
        local: NodeId,
        cell: CellId,
    ) -> Result<Self::Lease, peren_fleet::RepositoryError> {
        self.acquire(local, cell).await
    }
}

#[derive(Debug)]
pub(crate) struct EmptyDispatchResult {
    pub(crate) logs: Vec<peren_runtime::WorkerLogEvent>,
}

impl EmptyDispatchResult {
    fn discard(self) {
        drop(self.logs);
    }
}

#[derive(Debug)]
pub(crate) struct QueueDispatchResult {
    pub(crate) dispatch: peren_runtime::QueueDispatch,
}

#[derive(Clone)]
pub struct Node<R> {
    id: NodeId,
    data: PathBuf,
    store: R,
    alarms: Arc<Mutex<HashSet<CellId>>>,
    active: Arc<Mutex<HashSet<CellId>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreSummary {
    pub cell: CellId,
    pub source: RestoreSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreSource {
    Empty,
    Restored {
        epoch: OwnershipEpoch,
        generation: StorageRevision,
        revision: StorageRevision,
        database_bytes: usize,
        wal_bytes: usize,
    },
}

pub struct CellDispatchInput<R: NodeRepository> {
    pub path: PathBuf,
    pub lease: R::Lease,
    pub repository: R,
    pub restore: RestoreSummary,
}

impl<R: NodeRepository> Node<R> {
    #[must_use]
    pub fn new(id: NodeId, data: PathBuf, store: R) -> Self {
        Self {
            id,
            data,
            store,
            alarms: Arc::new(Mutex::new(HashSet::new())),
            active: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn dispatch_http(
        &self,
        cell: CellId,
        request: HttpRequest,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
        invocation: InvocationLimits,
    ) -> Result<HttpResponse, NodeError> {
        self.dispatch_http_with_environment(
            cell,
            request,
            bundle,
            isolate,
            invocation,
            WorkerEnvironment::empty(),
        )
        .await
    }

    pub async fn dispatch_http_after_receipt(
        &self,
        receipt: DurableReceipt,
        request: HttpRequest,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
        invocation: InvocationLimits,
    ) -> Result<HttpResponse, NodeError> {
        self.dispatch_http_after_receipt_with_environment(
            receipt,
            request,
            bundle,
            isolate,
            invocation,
            WorkerEnvironment::empty(),
        )
        .await
    }

    pub async fn dispatch_http_after_receipt_with_environment(
        &self,
        receipt: DurableReceipt,
        request: HttpRequest,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
        invocation: InvocationLimits,
        environment: WorkerEnvironment,
    ) -> Result<HttpResponse, NodeError> {
        if !self.restore_receipt(receipt).await? {
            return Err(NodeError::ReceiptUnsatisfied);
        }
        self.dispatch_http_with_environment(
            receipt.cell(),
            request,
            bundle,
            isolate,
            invocation,
            environment,
        )
        .await
    }

    pub async fn dispatch_http_with_environment(
        &self,
        cell: CellId,
        request: HttpRequest,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
        invocation: InvocationLimits,
        environment: WorkerEnvironment,
    ) -> Result<HttpResponse, NodeError> {
        self.restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident =
                WorkerCell::activate(&path, lease, store, bundle, isolate, environment).await?;
            let response = resident.dispatch_http(request, invocation).await?;
            resident.release().await?;
            Ok(response)
        })
        .await
    }

    pub async fn dispatch_alarm(
        &self,
        cell: CellId,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<(), NodeError> {
        self.dispatch_alarm_with_logs(cell, bundle, isolate)
            .await
            .map(EmptyDispatchResult::discard)
    }

    pub(crate) async fn dispatch_alarm_with_logs(
        &self,
        cell: CellId,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<EmptyDispatchResult, NodeError> {
        let _alarm = self.enter_alarm(cell)?;
        self.restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = WorkerCell::activate(
                &path,
                lease,
                store,
                bundle,
                isolate,
                WorkerEnvironment::empty(),
            )
            .await?;
            resident.dispatch_alarm().await?;
            let logs = resident.take_console_events();
            resident.release().await?;
            Ok(EmptyDispatchResult { logs })
        })
        .await
    }

    pub async fn dispatch_scheduled(
        &self,
        cell: CellId,
        event: ScheduledEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<(), NodeError> {
        self.dispatch_scheduled_with_logs(cell, event, bundle, isolate)
            .await
            .map(EmptyDispatchResult::discard)
    }

    pub(crate) async fn dispatch_scheduled_with_logs(
        &self,
        cell: CellId,
        event: ScheduledEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<EmptyDispatchResult, NodeError> {
        self.restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = WorkerCell::activate(
                &path,
                lease,
                store,
                bundle,
                isolate,
                WorkerEnvironment::empty(),
            )
            .await?;
            resident.dispatch_scheduled(event).await?;
            let logs = resident.take_console_events();
            resident.release().await?;
            Ok(EmptyDispatchResult { logs })
        })
        .await
    }

    pub async fn dispatch_queue(
        &self,
        cell: CellId,
        event: QueueEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<peren_runtime::QueueDispatch, NodeError> {
        self.dispatch_queue_with_logs(cell, event, bundle, isolate)
            .await
            .map(|result| result.dispatch)
    }

    pub(crate) async fn dispatch_queue_with_logs(
        &self,
        cell: CellId,
        event: QueueEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<QueueDispatchResult, NodeError> {
        self.restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = WorkerCell::activate(
                &path,
                lease,
                store,
                bundle,
                isolate,
                WorkerEnvironment::empty(),
            )
            .await?;
            let dispatch = resident.dispatch_queue(event).await?;
            resident.release().await?;
            Ok(QueueDispatchResult { dispatch })
        })
        .await
    }

    pub async fn dispatch_tail(
        &self,
        cell: CellId,
        event: TailEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<(), NodeError> {
        self.dispatch_tail_with_logs(cell, event, bundle, isolate)
            .await
            .map(EmptyDispatchResult::discard)
    }

    pub(crate) async fn dispatch_tail_with_logs(
        &self,
        cell: CellId,
        event: TailEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<EmptyDispatchResult, NodeError> {
        self.restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = WorkerCell::activate(
                &path,
                lease,
                store,
                bundle,
                isolate,
                WorkerEnvironment::empty(),
            )
            .await?;
            resident.dispatch_tail(event).await?;
            let logs = resident.take_console_events();
            resident.release().await?;
            Ok(EmptyDispatchResult { logs })
        })
        .await
    }

    pub async fn dispatch_workflow(
        &self,
        cell: CellId,
        event: WorkflowEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<(), NodeError> {
        self.dispatch_workflow_with_logs(cell, event, bundle, isolate)
            .await
            .map(EmptyDispatchResult::discard)
    }

    pub(crate) async fn dispatch_workflow_with_logs(
        &self,
        cell: CellId,
        event: WorkflowEvent,
        bundle: WorkerBundle,
        isolate: IsolateLimits,
    ) -> Result<EmptyDispatchResult, NodeError> {
        self.restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = WorkerCell::activate(
                &path,
                lease,
                store,
                bundle,
                isolate,
                WorkerEnvironment::empty(),
            )
            .await?;
            resident.dispatch_workflow(event).await?;
            let logs = resident.take_console_events();
            resident.release().await?;
            Ok(EmptyDispatchResult { logs })
        })
        .await
    }

    fn enter_alarm(&self, cell: CellId) -> Result<AlarmGuard, NodeError> {
        let mut alarms = self.alarms.lock().map_err(|_| NodeError::AlarmGate)?;
        if !alarms.insert(cell) {
            return Err(NodeError::AlarmOverlap(cell));
        }
        Ok(AlarmGuard {
            cell,
            alarms: Arc::clone(&self.alarms),
        })
    }

    pub async fn restore_receipt(&self, receipt: DurableReceipt) -> Result<bool, NodeError> {
        fs::create_dir_all(&self.data)?;
        let path = self.data.join(format!("{}.sqlite", receipt.cell()));
        remove_if_present(&PathBuf::from(format!("{}-wal", path.display())))?;
        remove_if_present(&PathBuf::from(format!("{}-shm", path.display())))?;
        let Some(replica) = Replicator::new(self.store.clone())
            .restore_satisfying(receipt)
            .await?
        else {
            return Ok(false);
        };
        fs::write(&path, replica.database)?;
        if !replica.wal.is_empty() {
            fs::write(format!("{}-wal", path.display()), replica.wal)?;
        }
        Ok(true)
    }

    pub(crate) async fn restore_and_dispatch<F, Fut, T>(
        &self,
        cell: CellId,
        dispatch: F,
    ) -> Result<T, NodeError>
    where
        F: FnOnce(CellDispatchInput<R>) -> Fut,
        Fut: Future<Output = Result<T, NodeError>>,
    {
        fs::create_dir_all(&self.data)?;
        let path = self.data.join(format!("{cell}.sqlite"));
        let _guard = CellDispatchGuard::enter(&self.active, cell)?;
        let lease = self.store.acquire(self.id, cell).await?;
        remove_if_present(&PathBuf::from(format!("{}-wal", path.display())))?;
        remove_if_present(&PathBuf::from(format!("{}-shm", path.display())))?;
        let restore = if let Some(replica) = self.store.restore(cell).await? {
            restore_replica(&path, cell, replica)?
        } else {
            remove_if_present(&path)?;
            RestoreSummary {
                cell,
                source: RestoreSource::Empty,
            }
        };
        dispatch(CellDispatchInput {
            path,
            lease,
            repository: self.store.clone(),
            restore,
        })
        .await
    }
}

fn restore_replica(
    path: &std::path::Path,
    cell: CellId,
    replica: ReplicaImage,
) -> Result<RestoreSummary, std::io::Error> {
    let database_bytes = replica.database.len();
    let wal_bytes = replica.wal.len();
    let epoch = replica.epoch;
    let generation = replica.generation;
    let revision = replica.revision;
    fs::write(path, replica.database)?;
    if !replica.wal.is_empty() {
        fs::write(format!("{}-wal", path.display()), replica.wal)?;
    }
    Ok(RestoreSummary {
        cell,
        source: RestoreSource::Restored {
            epoch,
            generation,
            revision,
            database_bytes,
            wal_bytes,
        },
    })
}

struct CellDispatchGuard {
    cell: CellId,
    active: Arc<Mutex<HashSet<CellId>>>,
}

impl CellDispatchGuard {
    fn enter(active: &Arc<Mutex<HashSet<CellId>>>, cell: CellId) -> Result<Self, NodeError> {
        let mut cells = active.lock().map_err(|_| NodeError::AlarmGate)?;
        if !cells.insert(cell) {
            return Err(NodeError::CellBusy(cell));
        }
        Ok(Self {
            cell,
            active: Arc::clone(active),
        })
    }
}

impl Drop for CellDispatchGuard {
    fn drop(&mut self) {
        if let Ok(mut cells) = self.active.lock() {
            cells.remove(&self.cell);
        }
    }
}

struct AlarmGuard {
    cell: CellId,
    alarms: Arc<Mutex<HashSet<CellId>>>,
}

impl Drop for AlarmGuard {
    fn drop(&mut self) {
        if let Ok(mut alarms) = self.alarms.lock() {
            alarms.remove(&self.cell);
        }
    }
}

fn remove_if_present(path: &std::path::Path) -> Result<(), std::io::Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Error)]
pub enum NodeError {
    #[error(transparent)]
    Cell(#[from] CellError),
    #[error(transparent)]
    Ownership(#[from] peren_fleet::RepositoryError),
    #[error(transparent)]
    Replica(#[from] ReplicaError),
    #[error("node data operation failed")]
    Io(#[from] std::io::Error),
    #[error("alarm dispatch already active for cell {0}")]
    AlarmOverlap(CellId),
    #[error("cell {0} is already dispatching on this node")]
    CellBusy(CellId),
    #[error("durable receipt is not satisfied by the available replica")]
    ReceiptUnsatisfied,
    #[error("alarm dispatch gate is poisoned")]
    AlarmGate,
}

impl NodeError {
    #[must_use]
    pub fn is_non_retryable(&self) -> bool {
        matches!(self, Self::Cell(error) if error.is_non_retryable())
    }
}

#[cfg(test)]
#[cfg(test)]
mod fixture;
#[cfg(test)]
mod replica;
