use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use peren_primitives::StorageRevision;

use crate::{
    HttpRequest, HttpResponse,
    wire::{
        AiRun, AwsSigv4Fetch, CacheEntry, CacheGet, CachePut, DurableObjectFetch, KvGet, KvList,
        KvPut, ListOptions, ListPage, QueueSend, R2Delete, R2Get, R2List, R2ListPage, R2Object,
        R2Put, ServiceFetch, SqlQuery, SqlResult,
    },
};
use thiserror::Error;

#[async_trait]
pub trait DurableStorageHost: Send + Sync {
    async fn begin(&self) -> Result<(), HostError>;
    async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError>;
    async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError>;
    async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError>;
    async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError>;
    async fn sql(&self, query: SqlQuery) -> Result<SqlResult, HostError>;
    async fn mutation_outcome(&self, _id: &str) -> Result<Option<Vec<u8>>, HostError> {
        Err(HostError)
    }
    async fn record_mutation_outcome(&self, _id: &str, _outcome: &[u8]) -> Result<(), HostError> {
        Err(HostError)
    }
    async fn attachment(&self, _id: &str) -> Result<Option<Vec<u8>>, HostError> {
        Err(HostError)
    }
    async fn set_attachment(&self, _id: &str, _bytes: &[u8]) -> Result<(), HostError> {
        Err(HostError)
    }
    async fn delete_attachment(&self, _id: &str) -> Result<bool, HostError> {
        Err(HostError)
    }
    async fn commit(&self) -> Result<StorageRevision, HostError>;
    async fn rollback(&self) -> Result<(), HostError>;
}

#[async_trait]
pub trait OutboundFetchHost: Send + Sync {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError>;

    async fn fetch_aws(&self, _request: AwsSigv4Fetch) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }
}

#[async_trait]
pub trait QueueProducerHost: Send + Sync {
    async fn send(&self, message: QueueSend) -> Result<(), HostError>;
}

#[async_trait]
pub trait ServiceBindingHost: Send + Sync {
    async fn fetch(&self, request: ServiceFetch) -> Result<HttpResponse, HostError>;
}

#[async_trait]
pub trait DurableObjectHost: Send + Sync {
    async fn fetch(&self, request: DurableObjectFetch) -> Result<HttpResponse, HostError>;
}

#[async_trait]
pub trait R2BucketHost: Send + Sync {
    async fn put(&self, object: R2Put) -> Result<(), HostError>;
    async fn get(&self, object: R2Get) -> Result<Option<R2Object>, HostError>;
    async fn delete(&self, object: R2Delete) -> Result<(), HostError>;
    async fn list(&self, request: R2List) -> Result<R2ListPage, HostError>;
}

#[async_trait]
pub trait CacheHost: Send + Sync {
    async fn match_entry(&self, request: CacheGet) -> Result<Option<CacheEntry>, HostError>;
    async fn put_entry(&self, entry: CachePut) -> Result<(), HostError>;
    async fn delete_entry(&self, request: CacheGet) -> Result<bool, HostError>;
}

#[async_trait]
pub trait KvHost: Send + Sync {
    async fn get(&self, request: KvGet) -> Result<Option<Vec<u8>>, HostError>;
    async fn put(&self, request: KvPut) -> Result<(), HostError>;
    async fn delete(&self, request: KvGet) -> Result<bool, HostError>;
    async fn list(&self, request: KvList) -> Result<ListPage, HostError>;
}

#[async_trait]
pub trait AiHost: Send + Sync {
    async fn run(&self, request: AiRun) -> Result<serde_json::Value, HostError>;
}

#[derive(Clone)]
pub struct InvocationStorage {
    host: Arc<dyn DurableStorageHost>,
    revision: Arc<AtomicU64>,
}

impl InvocationStorage {
    #[must_use]
    pub fn new(host: Arc<dyn DurableStorageHost>) -> Self {
        Self {
            host,
            revision: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn DurableStorageHost> {
        Arc::clone(&self.host)
    }

    pub fn reset(&self) {
        self.revision.store(0, Ordering::Release);
    }

    pub fn record(&self, revision: StorageRevision) {
        self.revision.fetch_max(revision.get(), Ordering::AcqRel);
    }

    #[must_use]
    pub fn revision(&self) -> StorageRevision {
        StorageRevision::new(self.revision.load(Ordering::Acquire))
    }
}

#[derive(Clone)]
pub struct InvocationFetch {
    host: Arc<dyn OutboundFetchHost>,
}

impl InvocationFetch {
    #[must_use]
    pub fn new(host: Arc<dyn OutboundFetchHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn OutboundFetchHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationR2 {
    host: Arc<dyn R2BucketHost>,
}

impl InvocationR2 {
    #[must_use]
    pub fn new(host: Arc<dyn R2BucketHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn R2BucketHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationCache {
    host: Arc<dyn CacheHost>,
}

impl InvocationCache {
    #[must_use]
    pub fn new(host: Arc<dyn CacheHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn CacheHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationKv {
    host: Arc<dyn KvHost>,
}

impl InvocationKv {
    #[must_use]
    pub fn new(host: Arc<dyn KvHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn KvHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationAi {
    host: Arc<dyn AiHost>,
}

impl InvocationAi {
    #[must_use]
    pub fn new(host: Arc<dyn AiHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn AiHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationService {
    host: Arc<dyn ServiceBindingHost>,
}

impl InvocationService {
    #[must_use]
    pub fn new(host: Arc<dyn ServiceBindingHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn ServiceBindingHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationDurableObject {
    host: Arc<dyn DurableObjectHost>,
}

impl InvocationDurableObject {
    #[must_use]
    pub fn new(host: Arc<dyn DurableObjectHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn DurableObjectHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone)]
pub struct InvocationQueue {
    host: Arc<dyn QueueProducerHost>,
}

impl InvocationQueue {
    #[must_use]
    pub fn new(host: Arc<dyn QueueProducerHost>) -> Self {
        Self { host }
    }

    #[must_use]
    pub fn host(&self) -> Arc<dyn QueueProducerHost> {
        Arc::clone(&self.host)
    }
}

#[derive(Clone, Copy, Debug, Error)]
#[error("runtime host operation failed")]
pub struct HostError;
