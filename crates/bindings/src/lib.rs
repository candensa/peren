use peren_primitives::StorageRevision;
use peren_runtime::{
    DurableStorageHost, HostError, ListEntry, ListOptions, ListPage, SqlQuery, SqlResult, SqlValue,
};
use peren_storage::{AttachmentStore, ListStore, MutationStore, SqlStore};
use std::sync::Arc;
use tokio::sync::Mutex;

mod identity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingKind {
    Service,
    DurableObjectNamespace,
    Kv,
    D1Database,
    R2Bucket,
    Queue,
    Vectorize,
    Hyperdrive,
    AnalyticsEngine,
    Outbound,
    RateLimiter,
    Workflow,
    SecretsStoreSecret,
    MtlsCertificate,
    Loader,
    Container,
    Ai,
    Dispatcher,
    Assets,
    Images,
}

impl BindingKind {
    #[must_use]
    pub const fn status(self) -> BindingStatus {
        match self {
            Self::Service
            | Self::DurableObjectNamespace
            | Self::Kv
            | Self::D1Database
            | Self::R2Bucket
            | Self::Queue
            | Self::Vectorize
            | Self::Hyperdrive
            | Self::AnalyticsEngine
            | Self::Outbound
            | Self::RateLimiter
            | Self::Workflow
            | Self::SecretsStoreSecret
            | Self::MtlsCertificate
            | Self::Assets
            | Self::Ai
            | Self::Dispatcher
            | Self::Loader
            | Self::Container
            | Self::Images => BindingStatus::Supported,
        }
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::DurableObjectNamespace => "durable_object_namespace",
            Self::Kv => "kv",
            Self::D1Database => "d1_database",
            Self::R2Bucket => "r2_bucket",
            Self::Queue => "queue",
            Self::Vectorize => "vectorize",
            Self::Hyperdrive => "hyperdrive",
            Self::AnalyticsEngine => "analytics_engine",
            Self::Outbound => "outbound",
            Self::RateLimiter => "rate_limiter",
            Self::Workflow => "workflow",
            Self::SecretsStoreSecret => "secrets_store_secret",
            Self::MtlsCertificate => "mtls_certificate",
            Self::Loader => "loader",
            Self::Container => "container",
            Self::Ai => "ai",
            Self::Dispatcher => "dispatcher",
            Self::Assets => "assets",
            Self::Images => "images",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingStatus {
    Supported,
    Unsupported,
}

pub const BINDINGS: &[BindingKind] = &[
    BindingKind::Service,
    BindingKind::DurableObjectNamespace,
    BindingKind::Kv,
    BindingKind::D1Database,
    BindingKind::R2Bucket,
    BindingKind::Queue,
    BindingKind::Vectorize,
    BindingKind::Hyperdrive,
    BindingKind::AnalyticsEngine,
    BindingKind::Outbound,
    BindingKind::RateLimiter,
    BindingKind::Workflow,
    BindingKind::SecretsStoreSecret,
    BindingKind::MtlsCertificate,
    BindingKind::Loader,
    BindingKind::Container,
    BindingKind::Ai,
    BindingKind::Dispatcher,
    BindingKind::Assets,
    BindingKind::Images,
];

#[must_use]
pub fn unsupported_bindings() -> Vec<BindingKind> {
    BINDINGS
        .iter()
        .copied()
        .filter(|binding| binding.status() == BindingStatus::Unsupported)
        .collect()
}

pub use identity::{
    d1_cell_id, derive_cell_id, facet_cell_id, kv_cell_id, new_unique_cell_id,
    verify_cell_id_membership,
};

pub struct StorageHost<'a, S> {
    storage: Mutex<&'a mut S>,
}

impl<'a, S> StorageHost<'a, S> {
    #[must_use]
    pub const fn new(storage: &'a mut S) -> Self {
        Self {
            storage: Mutex::const_new(storage),
        }
    }
}

#[async_trait::async_trait]
impl<S: AttachmentStore + ListStore + MutationStore + SqlStore> DurableStorageHost
    for StorageHost<'_, S>
{
    async fn begin(&self) -> Result<(), HostError> {
        self.storage
            .lock()
            .await
            .begin()
            .await
            .map_err(|_| HostError)
    }

    async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        self.storage
            .lock()
            .await
            .load(scope, key)
            .await
            .map_err(|_| HostError)
    }

    async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError> {
        self.storage
            .lock()
            .await
            .put(scope, key, value)
            .await
            .map_err(|_| HostError)
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
        self.storage
            .lock()
            .await
            .delete(scope, key)
            .await
            .map_err(|_| HostError)
    }

    async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError> {
        let storage_options = peren_storage::ListOptions {
            prefix: options.prefix.as_deref(),
            cursor: options.cursor.as_deref(),
            limit: options.limit.unwrap_or(peren_storage::MAX_LIST_ENTRIES),
            ..Default::default()
        };
        let mut storage = self.storage.lock().await;
        ListStore::list(&mut **storage, scope, &storage_options)
            .await
            .map(from_list)
            .map_err(|_| HostError)
    }

    async fn sql(&self, query: SqlQuery) -> Result<SqlResult, HostError> {
        let parameters = query.parameters.iter().map(to_storage).collect::<Vec<_>>();
        self.storage
            .lock()
            .await
            .sql(&query.sql, &parameters)
            .await
            .map(from_result)
            .map_err(|_| HostError)
    }

    async fn mutation_outcome(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| HostError)?;
        let mut storage = self.storage.lock().await;
        MutationStore::mutation_outcome(&mut **storage, id)
            .await
            .map(|record| record.map(|record| record.outcome))
            .map_err(|_| HostError)
    }

    async fn record_mutation_outcome(&self, id: &str, outcome: &[u8]) -> Result<(), HostError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| HostError)?;
        let mut storage = self.storage.lock().await;
        MutationStore::record_mutation_outcome(&mut **storage, id, outcome)
            .await
            .map_err(|_| HostError)
    }

    async fn attachment(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let mut storage = self.storage.lock().await;
        AttachmentStore::attachment(&mut **storage, id)
            .await
            .map_err(|_| HostError)
    }

    async fn set_attachment(&self, id: &str, bytes: &[u8]) -> Result<(), HostError> {
        let mut storage = self.storage.lock().await;
        AttachmentStore::set_attachment(&mut **storage, id, bytes)
            .await
            .map_err(|_| HostError)
    }

    async fn delete_attachment(&self, id: &str) -> Result<bool, HostError> {
        let mut storage = self.storage.lock().await;
        AttachmentStore::delete_attachment(&mut **storage, id)
            .await
            .map_err(|_| HostError)
    }

    async fn commit(&self) -> Result<StorageRevision, HostError> {
        self.storage
            .lock()
            .await
            .commit()
            .await
            .map(|commit| commit.revision)
            .map_err(|_| HostError)
    }

    async fn rollback(&self) -> Result<(), HostError> {
        self.storage
            .lock()
            .await
            .rollback()
            .await
            .map_err(|_| HostError)
    }
}

fn from_list(page: peren_storage::ListPage) -> ListPage {
    let list_complete = page.next_cursor.is_none();
    ListPage {
        keys: page
            .entries
            .into_iter()
            .map(|(key, _)| ListEntry {
                name: String::from_utf8_lossy(&key).into_owned(),
            })
            .collect(),
        cursor: page.next_cursor,
        list_complete,
    }
}

fn to_storage(value: &SqlValue) -> peren_storage::SqlValue {
    match value {
        SqlValue::Null => peren_storage::SqlValue::Null,
        SqlValue::Integer(value) => peren_storage::SqlValue::Integer(*value),
        SqlValue::Real(value) => peren_storage::SqlValue::Real(*value),
        SqlValue::Text(value) => peren_storage::SqlValue::Text(value.clone()),
        SqlValue::Blob(value) => peren_storage::SqlValue::Blob(value.clone()),
    }
}

fn from_storage(value: peren_storage::SqlValue) -> SqlValue {
    match value {
        peren_storage::SqlValue::Null => SqlValue::Null,
        peren_storage::SqlValue::Integer(value) => SqlValue::Integer(value),
        peren_storage::SqlValue::Real(value) => SqlValue::Real(value),
        peren_storage::SqlValue::Text(value) => SqlValue::Text(value),
        peren_storage::SqlValue::Blob(value) => SqlValue::Blob(value),
    }
}

fn from_result(result: peren_storage::SqlResult) -> SqlResult {
    SqlResult {
        columns: result.columns,
        rows: result
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(from_storage).collect())
            .collect(),
        changes: result.changes,
        last_insert_rowid: result.last_insert_rowid,
    }
}

pub struct SharedStorageHost<S> {
    storage: Arc<Mutex<S>>,
}

impl<S> Clone for SharedStorageHost<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

impl<S> SharedStorageHost<S> {
    #[must_use]
    pub fn new(storage: Arc<Mutex<S>>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl<S: AttachmentStore + ListStore + MutationStore + SqlStore> DurableStorageHost
    for SharedStorageHost<S>
{
    async fn begin(&self) -> Result<(), HostError> {
        self.storage
            .lock()
            .await
            .begin()
            .await
            .map_err(|_| HostError)
    }

    async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        self.storage
            .lock()
            .await
            .load(scope, key)
            .await
            .map_err(|_| HostError)
    }

    async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError> {
        self.storage
            .lock()
            .await
            .put(scope, key, value)
            .await
            .map_err(|_| HostError)
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
        self.storage
            .lock()
            .await
            .delete(scope, key)
            .await
            .map_err(|_| HostError)
    }

    async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError> {
        let storage_options = peren_storage::ListOptions {
            prefix: options.prefix.as_deref(),
            cursor: options.cursor.as_deref(),
            limit: options.limit.unwrap_or(peren_storage::MAX_LIST_ENTRIES),
            ..Default::default()
        };
        let mut storage = self.storage.lock().await;
        ListStore::list(&mut *storage, scope, &storage_options)
            .await
            .map(from_list)
            .map_err(|_| HostError)
    }

    async fn sql(&self, query: SqlQuery) -> Result<SqlResult, HostError> {
        let parameters = query.parameters.iter().map(to_storage).collect::<Vec<_>>();
        self.storage
            .lock()
            .await
            .sql(&query.sql, &parameters)
            .await
            .map(from_result)
            .map_err(|_| HostError)
    }

    async fn mutation_outcome(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| HostError)?;
        let mut storage = self.storage.lock().await;
        MutationStore::mutation_outcome(&mut *storage, id)
            .await
            .map(|record| record.map(|record| record.outcome))
            .map_err(|_| HostError)
    }

    async fn record_mutation_outcome(&self, id: &str, outcome: &[u8]) -> Result<(), HostError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| HostError)?;
        let mut storage = self.storage.lock().await;
        MutationStore::record_mutation_outcome(&mut *storage, id, outcome)
            .await
            .map_err(|_| HostError)
    }

    async fn attachment(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let mut storage = self.storage.lock().await;
        AttachmentStore::attachment(&mut *storage, id)
            .await
            .map_err(|_| HostError)
    }

    async fn set_attachment(&self, id: &str, bytes: &[u8]) -> Result<(), HostError> {
        let mut storage = self.storage.lock().await;
        AttachmentStore::set_attachment(&mut *storage, id, bytes)
            .await
            .map_err(|_| HostError)
    }

    async fn delete_attachment(&self, id: &str) -> Result<bool, HostError> {
        let mut storage = self.storage.lock().await;
        AttachmentStore::delete_attachment(&mut *storage, id)
            .await
            .map_err(|_| HostError)
    }

    async fn commit(&self) -> Result<StorageRevision, HostError> {
        self.storage
            .lock()
            .await
            .commit()
            .await
            .map(|commit| commit.revision)
            .map_err(|_| HostError)
    }

    async fn rollback(&self) -> Result<(), HostError> {
        self.storage
            .lock()
            .await
            .rollback()
            .await
            .map_err(|_| HostError)
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    #[test]
    fn binding_capability_table_has_explicit_status_for_every_kind() {
        let names = BINDINGS
            .iter()
            .map(|binding| binding.name())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), BINDINGS.len());
        assert_eq!(unsupported_bindings(), Vec::new());
    }

    #[test]
    fn supported_bindings_match_the_runtime_surface() {
        let supported = BINDINGS
            .iter()
            .copied()
            .filter(|binding| binding.status() == BindingStatus::Supported)
            .map(BindingKind::name)
            .collect::<std::collections::BTreeSet<_>>();
        for name in [
            "service",
            "durable_object_namespace",
            "kv",
            "d1_database",
            "r2_bucket",
            "queue",
            "vectorize",
            "hyperdrive",
            "outbound",
            "analytics_engine",
            "rate_limiter",
            "workflow",
            "secrets_store_secret",
            "mtls_certificate",
            "assets",
            "ai",
            "dispatcher",
            "loader",
            "container",
            "images",
        ] {
            assert!(supported.contains(name), "{name} should be supported");
        }
    }
}
