use std::{collections::BTreeMap, sync::Arc};

use peren_bindings::SharedStorageHost;
use peren_provider_object_store::R2Store;
use peren_provider_turso::TursoStore;
use peren_queues::Send;
use peren_runtime::{
    CacheEntry, CacheGet, CacheHost, CachePut, DurableObjectFetch, DurableObjectHost,
    DurableStorageHost, HostError, HttpResponse, KvGet, KvHost, KvList, KvPut, ListOptions,
    ListPage, QueueProducerHost, QueueSend, R2BucketHost, R2Delete, R2Get, R2List, R2ListPage,
    R2Object, R2Put, ServiceBindingHost, ServiceFetch, SqlQuery, SqlResult,
};
use peren_storage::CellStorage;
use tokio::sync::Mutex;

use crate::process::QueueBrokerState;

use super::{CacheStore, KvStore};

#[derive(Clone)]
pub struct RoutedHost {
    cell: SharedStorageHost<CellStorage>,
    d1: BTreeMap<String, Arc<Mutex<TursoStore>>>,
    r2: BTreeMap<String, Arc<R2Store>>,
    queues: QueueBrokerState,
    cache: CacheStore,
    kv: BTreeMap<String, KvStore>,
}

impl RoutedHost {
    #[must_use]
    pub fn new(
        cell: Arc<Mutex<CellStorage>>,
        d1: BTreeMap<String, Arc<Mutex<TursoStore>>>,
        r2: BTreeMap<String, Arc<R2Store>>,
        queues: QueueBrokerState,
        cache: CacheStore,
        kv: BTreeMap<String, KvStore>,
    ) -> Self {
        Self {
            cell: SharedStorageHost::new(cell),
            d1,
            r2,
            queues,
            cache,
            kv,
        }
    }

    async fn turso(&self, query: SqlQuery) -> Result<Option<SqlResult>, HostError> {
        let Some(database) = &query.database else {
            return Ok(None);
        };
        let Some(store) = self.d1.get(database) else {
            return Ok(None);
        };
        let parameters = query.parameters.iter().map(to_storage).collect::<Vec<_>>();
        let mut store = store.lock().await;
        peren_storage::SqlStore::sql(&mut *store, &query.sql, &parameters)
            .await
            .map(from_storage)
            .map(Some)
            .map_err(|_| HostError)
    }
}

#[async_trait::async_trait]
impl CacheHost for RoutedHost {
    async fn match_entry(&self, request: CacheGet) -> Result<Option<CacheEntry>, HostError> {
        self.cache.match_entry(request).await
    }

    async fn put_entry(&self, entry: CachePut) -> Result<(), HostError> {
        self.cache.put_entry(entry).await
    }

    async fn delete_entry(&self, request: CacheGet) -> Result<bool, HostError> {
        self.cache.delete_entry(request).await
    }
}

#[async_trait::async_trait]
impl KvHost for RoutedHost {
    async fn get(&self, request: KvGet) -> Result<Option<Vec<u8>>, HostError> {
        self.kv
            .get(&request.namespace)
            .ok_or(HostError)?
            .get(request)
            .await
    }

    async fn put(&self, request: KvPut) -> Result<(), HostError> {
        self.kv
            .get(&request.namespace)
            .ok_or(HostError)?
            .put(request)
            .await
    }

    async fn delete(&self, request: KvGet) -> Result<bool, HostError> {
        self.kv
            .get(&request.namespace)
            .ok_or(HostError)?
            .delete(request)
            .await
    }

    async fn list(&self, request: KvList) -> Result<ListPage, HostError> {
        self.kv
            .get(&request.namespace)
            .ok_or(HostError)?
            .list(request)
            .await
    }
}

#[async_trait::async_trait]
impl QueueProducerHost for RoutedHost {
    async fn send(&self, message: QueueSend) -> Result<(), HostError> {
        let delay = chrono::Duration::seconds(i64::from(message.delay_seconds.unwrap_or(0)));
        self.queues
            .send(
                Send {
                    queue: message.queue,
                    body: message.body,
                    content_type: message.content_type,
                    partition: message.partition,
                    delay,
                    dedup_id: message.dedup_id,
                },
                chrono::Utc::now(),
            )
            .await
            .map(|_| ())
            .map_err(|_| HostError)
    }
}

#[async_trait::async_trait]
impl ServiceBindingHost for RoutedHost {
    async fn fetch(&self, _request: ServiceFetch) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl DurableObjectHost for RoutedHost {
    async fn fetch(&self, _request: DurableObjectFetch) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl R2BucketHost for RoutedHost {
    async fn put(&self, object: R2Put) -> Result<(), HostError> {
        self.r2
            .get(&object.bucket)
            .ok_or(HostError)?
            .put(object)
            .await
    }

    async fn get(&self, object: R2Get) -> Result<Option<R2Object>, HostError> {
        self.r2
            .get(&object.bucket)
            .ok_or(HostError)?
            .get(object)
            .await
    }

    async fn delete(&self, object: R2Delete) -> Result<(), HostError> {
        self.r2
            .get(&object.bucket)
            .ok_or(HostError)?
            .delete(object)
            .await
    }

    async fn list(&self, request: R2List) -> Result<R2ListPage, HostError> {
        self.r2
            .get(&request.bucket)
            .ok_or(HostError)?
            .list(request)
            .await
    }
}

#[async_trait::async_trait]
impl DurableStorageHost for RoutedHost {
    async fn begin(&self) -> Result<(), HostError> {
        self.cell.begin().await?;
        for store in self.d1.values() {
            peren_storage::CellStore::begin(&mut *store.lock().await)
                .await
                .map_err(|_| HostError)?;
        }
        Ok(())
    }

    async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        self.cell.load(scope, key).await
    }

    async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError> {
        self.cell.put(scope, key, value).await
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
        self.cell.delete(scope, key).await
    }

    async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError> {
        self.cell.list(scope, options).await
    }

    async fn sql(&self, query: SqlQuery) -> Result<SqlResult, HostError> {
        if let Some(result) = self.turso(query.clone()).await? {
            return Ok(result);
        }
        self.cell.sql(query).await
    }

    async fn mutation_outcome(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        self.cell.mutation_outcome(id).await
    }

    async fn record_mutation_outcome(&self, id: &str, outcome: &[u8]) -> Result<(), HostError> {
        self.cell.record_mutation_outcome(id, outcome).await
    }

    async fn attachment(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        self.cell.attachment(id).await
    }

    async fn set_attachment(&self, id: &str, bytes: &[u8]) -> Result<(), HostError> {
        self.cell.set_attachment(id, bytes).await
    }

    async fn delete_attachment(&self, id: &str) -> Result<bool, HostError> {
        self.cell.delete_attachment(id).await
    }

    async fn commit(&self) -> Result<peren_primitives::StorageRevision, HostError> {
        let revision = self.cell.commit().await?;
        for store in self.d1.values() {
            peren_storage::CellStore::commit(&mut *store.lock().await)
                .await
                .map_err(|_| HostError)?;
        }
        Ok(revision)
    }

    async fn rollback(&self) -> Result<(), HostError> {
        self.cell.rollback().await?;
        for store in self.d1.values() {
            peren_storage::CellStore::rollback(&mut *store.lock().await)
                .await
                .map_err(|_| HostError)?;
        }
        Ok(())
    }
}

fn to_storage(value: &peren_runtime::SqlValue) -> peren_storage::SqlValue {
    match value {
        peren_runtime::SqlValue::Null => peren_storage::SqlValue::Null,
        peren_runtime::SqlValue::Integer(value) => peren_storage::SqlValue::Integer(*value),
        peren_runtime::SqlValue::Real(value) => peren_storage::SqlValue::Real(*value),
        peren_runtime::SqlValue::Text(value) => peren_storage::SqlValue::Text(value.clone()),
        peren_runtime::SqlValue::Blob(value) => peren_storage::SqlValue::Blob(value.clone()),
    }
}

fn from_storage(result: peren_storage::SqlResult) -> SqlResult {
    SqlResult {
        columns: result.columns,
        rows: result
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(from_value).collect())
            .collect(),
        changes: result.changes,
        last_insert_rowid: result.last_insert_rowid,
    }
}

fn from_value(value: peren_storage::SqlValue) -> peren_runtime::SqlValue {
    match value {
        peren_storage::SqlValue::Null => peren_runtime::SqlValue::Null,
        peren_storage::SqlValue::Integer(value) => peren_runtime::SqlValue::Integer(value),
        peren_storage::SqlValue::Real(value) => peren_runtime::SqlValue::Real(value),
        peren_storage::SqlValue::Text(value) => peren_runtime::SqlValue::Text(value),
        peren_storage::SqlValue::Blob(value) => peren_runtime::SqlValue::Blob(value),
    }
}
