pub use peren_primitives::StorageRevision;
pub use peren_runtime::{
    AiHost, AiRun, CAPABILITIES, CacheEntry, CacheGet, CacheHost, CachePut, Capabilities,
    CapabilityKind, CapabilityStatus, DurableObjectFetch, DurableObjectHost, DurableStorageHost,
    EngineError, HostError, HttpRequest, HttpResponse, InvocationLimits, IsolateLimits, LimitError,
    Module, ModuleKind, ModuleName, OutboundFetchHost, QueueDispositionKind, QueueEvent,
    QueueMessage, QueueMetrics, QueueProducerHost, QueueSend, R2BucketHost, R2Delete, R2Get,
    R2List, R2ListPage, R2Object, R2ObjectEntry, R2Put, ScheduledEvent, ServiceBindingHost,
    ServiceFetch, TailEvent, TailRecord, WorkerBundle, WorkerEnvironment, WorkerRuntime,
    WorkflowEvent,
};
pub use peren_storage::{CellStore, ListStore, SqlStore};
pub use std::path::Path;
pub use std::time::Duration;
pub use std::{collections::BTreeMap, sync::Arc};
pub use tokio::sync::Mutex;

pub(super) fn limits() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

pub(super) fn bundle(source: &str) -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(ModuleKind::JavaScript, source.as_bytes()).unwrap(),
        )]),
    )
    .unwrap()
}

#[derive(Default)]
pub(super) struct Host {
    value: Mutex<Option<Vec<u8>>>,
    staged: Mutex<Option<Vec<u8>>>,
}

#[async_trait::async_trait]
impl DurableStorageHost for Host {
    async fn begin(&self) -> Result<(), HostError> {
        *self.staged.lock().await = None;
        Ok(())
    }

    async fn load(&self, _scope: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        Ok(self.value.lock().await.clone())
    }

    async fn put(&self, _scope: &str, _key: &[u8], value: &[u8]) -> Result<(), HostError> {
        *self.staged.lock().await = Some(value.to_vec());
        Ok(())
    }

    async fn delete(&self, _scope: &str, _key: &[u8]) -> Result<bool, HostError> {
        *self.staged.lock().await = None;
        Ok(true)
    }

    async fn list(
        &self,
        _scope: &str,
        _options: peren_runtime::ListOptions,
    ) -> Result<peren_runtime::ListPage, HostError> {
        Err(HostError)
    }

    async fn sql(
        &self,
        _query: peren_runtime::SqlQuery,
    ) -> Result<peren_runtime::SqlResult, HostError> {
        Err(HostError)
    }

    async fn commit(&self) -> Result<StorageRevision, HostError> {
        let staged = self.staged.lock().await.take();
        *self.value.lock().await = staged;
        Ok(StorageRevision::new(7))
    }

    async fn rollback(&self) -> Result<(), HostError> {
        *self.staged.lock().await = None;
        Ok(())
    }
}

pub(super) struct SqlHost {
    pub(super) storage: Mutex<peren_storage::CellStorage>,
    pub(super) databases: Mutex<Vec<Option<String>>>,
}

impl SqlHost {
    pub(super) fn new() -> Self {
        Self {
            storage: Mutex::new(peren_storage::CellStorage::open(Path::new(":memory:")).unwrap()),
            databases: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl DurableStorageHost for SqlHost {
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
        let mut storage = self.storage.lock().await;
        CellStore::put(&mut *storage, scope, key, value)
            .await
            .map_err(|_| HostError)
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
        let mut storage = self.storage.lock().await;
        CellStore::delete(&mut *storage, scope, key)
            .await
            .map_err(|_| HostError)
    }

    async fn list(
        &self,
        scope: &str,
        options: peren_runtime::ListOptions,
    ) -> Result<peren_runtime::ListPage, HostError> {
        let storage_options = peren_storage::ListOptions {
            prefix: options.prefix.as_deref(),
            cursor: options.cursor.as_deref(),
            limit: options.limit.unwrap_or(peren_storage::MAX_LIST_ENTRIES),
            ..Default::default()
        };
        let mut storage = self.storage.lock().await;
        let page = ListStore::list(&mut *storage, scope, &storage_options)
            .await
            .map_err(|_| HostError)?;
        Ok(peren_runtime::ListPage {
            keys: page
                .entries
                .into_iter()
                .map(|(key, _)| peren_runtime::ListEntry {
                    name: String::from_utf8_lossy(&key).into_owned(),
                })
                .collect(),
            cursor: page.next_cursor.clone(),
            list_complete: page.next_cursor.is_none(),
        })
    }

    async fn sql(
        &self,
        query: peren_runtime::SqlQuery,
    ) -> Result<peren_runtime::SqlResult, HostError> {
        self.databases.lock().await.push(query.database.clone());
        let parameters = query
            .parameters
            .iter()
            .map(|value| match value {
                peren_runtime::SqlValue::Null => peren_storage::SqlValue::Null,
                peren_runtime::SqlValue::Integer(value) => peren_storage::SqlValue::Integer(*value),
                peren_runtime::SqlValue::Real(value) => peren_storage::SqlValue::Real(*value),
                peren_runtime::SqlValue::Text(value) => {
                    peren_storage::SqlValue::Text(value.clone())
                }
                peren_runtime::SqlValue::Blob(value) => {
                    peren_storage::SqlValue::Blob(value.clone())
                }
            })
            .collect::<Vec<_>>();
        let mut storage = self.storage.lock().await;
        let result = SqlStore::sql(&mut *storage, &query.sql, &parameters)
            .await
            .map_err(|_| HostError)?;
        Ok(peren_runtime::SqlResult {
            columns: result.columns,
            rows: result
                .rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| match value {
                            peren_storage::SqlValue::Null => peren_runtime::SqlValue::Null,
                            peren_storage::SqlValue::Integer(value) => {
                                peren_runtime::SqlValue::Integer(value)
                            }
                            peren_storage::SqlValue::Real(value) => {
                                peren_runtime::SqlValue::Real(value)
                            }
                            peren_storage::SqlValue::Text(value) => {
                                peren_runtime::SqlValue::Text(value)
                            }
                            peren_storage::SqlValue::Blob(value) => {
                                peren_runtime::SqlValue::Blob(value)
                            }
                        })
                        .collect()
                })
                .collect(),
            changes: result.changes,
            last_insert_rowid: result.last_insert_rowid,
        })
    }

    async fn mutation_outcome(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| HostError)?;
        let mut storage = self.storage.lock().await;
        peren_storage::MutationStore::mutation_outcome(&mut *storage, id)
            .await
            .map(|record| record.map(|record| record.outcome))
            .map_err(|_| HostError)
    }

    async fn record_mutation_outcome(&self, id: &str, outcome: &[u8]) -> Result<(), HostError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| HostError)?;
        let mut storage = self.storage.lock().await;
        peren_storage::MutationStore::record_mutation_outcome(&mut *storage, id, outcome)
            .await
            .map_err(|_| HostError)
    }

    async fn attachment(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let mut storage = self.storage.lock().await;
        peren_storage::AttachmentStore::attachment(&mut *storage, id)
            .await
            .map_err(|_| HostError)
    }

    async fn set_attachment(&self, id: &str, bytes: &[u8]) -> Result<(), HostError> {
        let mut storage = self.storage.lock().await;
        peren_storage::AttachmentStore::set_attachment(&mut *storage, id, bytes)
            .await
            .map_err(|_| HostError)
    }

    async fn delete_attachment(&self, id: &str) -> Result<bool, HostError> {
        let mut storage = self.storage.lock().await;
        peren_storage::AttachmentStore::delete_attachment(&mut *storage, id)
            .await
            .map_err(|_| HostError)
    }

    async fn commit(&self) -> Result<StorageRevision, HostError> {
        let mut storage = self.storage.lock().await;
        CellStore::commit(&mut *storage)
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

#[derive(Default)]
pub(super) struct R2Host {
    objects: Mutex<BTreeMap<(String, String), R2Object>>,
}

#[async_trait::async_trait]
impl R2BucketHost for R2Host {
    async fn put(&self, object: R2Put) -> Result<(), HostError> {
        let stored = R2Object {
            key: object.key.clone(),
            size: object.body.len(),
            body: object.body,
            content_type: object.content_type,
            custom_metadata: object.custom_metadata,
        };
        self.objects
            .lock()
            .await
            .insert((object.bucket, object.key), stored);
        Ok(())
    }

    async fn get(&self, object: R2Get) -> Result<Option<R2Object>, HostError> {
        Ok(self
            .objects
            .lock()
            .await
            .get(&(object.bucket, object.key))
            .cloned())
    }

    async fn delete(&self, object: R2Delete) -> Result<(), HostError> {
        self.objects
            .lock()
            .await
            .remove(&(object.bucket, object.key));
        Ok(())
    }

    async fn list(&self, request: R2List) -> Result<R2ListPage, HostError> {
        let limit = request.limit.unwrap_or(1000);
        let prefix = request.prefix.unwrap_or_default();
        let cursor = request.cursor.unwrap_or_default();
        let mut objects = self
            .objects
            .lock()
            .await
            .iter()
            .filter(|((bucket, key), _)| {
                bucket == &request.bucket && key.starts_with(&prefix) && key > &cursor
            })
            .map(|((_, key), object)| R2ObjectEntry {
                key: key.clone(),
                size: object.size,
                custom_metadata: object.custom_metadata.clone(),
            })
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        let cursor = if objects.len() > limit {
            objects.truncate(limit);
            objects.last().map(|object| object.key.clone())
        } else {
            None
        };
        Ok(R2ListPage {
            objects,
            list_complete: cursor.is_none(),
            cursor,
        })
    }
}

pub(super) struct AiEcho;

#[async_trait::async_trait]
impl AiHost for AiEcho {
    async fn run(&self, request: AiRun) -> Result<serde_json::Value, HostError> {
        Ok(serde_json::json!({
            "model": request.model,
            "input": request.input,
            "options": request.options,
        }))
    }
}

#[derive(Default)]
pub(super) struct CacheMap {
    entries: Mutex<BTreeMap<(String, String), CacheEntry>>,
}

#[async_trait::async_trait]
impl CacheHost for CacheMap {
    async fn match_entry(&self, request: CacheGet) -> Result<Option<CacheEntry>, HostError> {
        Ok(self
            .entries
            .lock()
            .await
            .get(&(request.cache, request.key))
            .cloned())
    }

    async fn put_entry(&self, entry: CachePut) -> Result<(), HostError> {
        self.entries.lock().await.insert(
            (entry.cache, entry.key),
            CacheEntry {
                status: entry.status,
                headers: entry.headers,
                body: entry.body,
            },
        );
        Ok(())
    }

    async fn delete_entry(&self, request: CacheGet) -> Result<bool, HostError> {
        Ok(self
            .entries
            .lock()
            .await
            .remove(&(request.cache, request.key))
            .is_some())
    }
}

#[derive(Default)]
pub(super) struct QueueHost {
    pub(super) messages: Mutex<Vec<QueueSend>>,
}

#[async_trait::async_trait]
impl QueueProducerHost for QueueHost {
    async fn send(&self, message: QueueSend) -> Result<(), HostError> {
        self.messages.lock().await.push(message);
        Ok(())
    }
}

pub(super) struct FetchHost;

#[async_trait::async_trait]
impl OutboundFetchHost for FetchHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        Ok(HttpResponse {
            status: 202,
            headers: vec![("x-upstream".into(), request.method)],
            body: format!("{}:{}", request.url, request.body.len()).into_bytes(),
            upgrade: false,
            websocket_id: None,
        })
    }
}

pub(super) struct MtlsFetchHost {
    pub(super) requests: Mutex<Vec<HttpRequest>>,
}

#[async_trait::async_trait]
impl OutboundFetchHost for MtlsFetchHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        let mtls = request.mtls.clone().unwrap_or_else(|| "none".into());
        self.requests.lock().await.push(request);
        Ok(HttpResponse {
            status: 203,
            headers: vec![("x-mtls".into(), mtls.clone())],
            body: mtls.into_bytes(),
            upgrade: false,
            websocket_id: None,
        })
    }
}

#[derive(Default)]
pub(super) struct ServiceHost {
    pub(super) requests: Mutex<Vec<ServiceFetch>>,
}

pub(super) struct DurableHost {
    pub(super) requests: Mutex<Vec<DurableObjectFetch>>,
}

#[async_trait::async_trait]
impl DurableObjectHost for DurableHost {
    async fn fetch(&self, request: DurableObjectFetch) -> Result<HttpResponse, HostError> {
        let body = String::from_utf8(request.request.body.clone()).unwrap();
        let status = if request.name.as_deref() == Some("lobby") {
            209
        } else {
            210
        };
        let response = HttpResponse {
            status,
            headers: vec![("x-durable-object".into(), request.class_name.clone())],
            body: format!(
                "{}:{}:{}:{}",
                request.namespace, request.class_name, request.request.method, body
            )
            .into_bytes(),
            upgrade: false,
            websocket_id: None,
        };
        self.requests.lock().await.push(request);
        Ok(response)
    }
}

#[async_trait::async_trait]
impl ServiceBindingHost for ServiceHost {
    async fn fetch(&self, request: ServiceFetch) -> Result<HttpResponse, HostError> {
        let body = format!(
            "{}:{}:{}",
            request.service,
            request.request.method,
            String::from_utf8_lossy(&request.request.body)
        );
        self.requests.lock().await.push(request);
        Ok(HttpResponse {
            status: 207,
            headers: vec![("x-service".into(), "auth".into())],
            body: body.into_bytes(),
            upgrade: false,
            websocket_id: None,
        })
    }
}
