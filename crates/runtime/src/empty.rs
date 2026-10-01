use crate::{
    AiHost, CacheHost, DurableObjectHost, DurableStorageHost, HostError, HttpRequest, HttpResponse,
    KvHost, OutboundFetchHost, QueueProducerHost, R2BucketHost, ServiceBindingHost,
    wire::{
        AiRun, AwsSigv4Fetch, CacheEntry, CacheGet, CachePut, DurableObjectFetch, KvGet, KvList,
        KvPut, ListOptions, ListPage, QueueSend, R2Delete, R2Get, R2List, R2ListPage, R2Object,
        R2Put, ServiceFetch, SqlQuery, SqlResult,
    },
};

pub(crate) struct NoStorage;

pub(crate) struct NoFetch;

pub(crate) struct NoQueue;

pub(crate) struct NoR2;

pub(crate) struct NoService;
pub(crate) struct NoDurableObject;
pub(crate) struct NoCache;
pub(crate) struct NoKv;
pub(crate) struct NoAi;

#[async_trait::async_trait]
impl DurableStorageHost for NoStorage {
    async fn begin(&self) -> Result<(), HostError> {
        Err(HostError)
    }

    async fn load(&self, _scope: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        Err(HostError)
    }

    async fn put(&self, _scope: &str, _key: &[u8], _value: &[u8]) -> Result<(), HostError> {
        Err(HostError)
    }

    async fn delete(&self, _scope: &str, _key: &[u8]) -> Result<bool, HostError> {
        Err(HostError)
    }

    async fn list(&self, _scope: &str, _options: ListOptions) -> Result<ListPage, HostError> {
        Err(HostError)
    }

    async fn sql(&self, _query: SqlQuery) -> Result<SqlResult, HostError> {
        Err(HostError)
    }

    async fn commit(&self) -> Result<peren_primitives::StorageRevision, HostError> {
        Err(HostError)
    }

    async fn rollback(&self) -> Result<(), HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl R2BucketHost for NoR2 {
    async fn put(&self, _object: R2Put) -> Result<(), HostError> {
        Err(HostError)
    }
    async fn get(&self, _object: R2Get) -> Result<Option<R2Object>, HostError> {
        Err(HostError)
    }
    async fn delete(&self, _object: R2Delete) -> Result<(), HostError> {
        Err(HostError)
    }
    async fn list(&self, _request: R2List) -> Result<R2ListPage, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl QueueProducerHost for NoQueue {
    async fn send(&self, _message: QueueSend) -> Result<(), HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl OutboundFetchHost for NoFetch {
    async fn fetch(&self, _request: HttpRequest) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }

    async fn fetch_aws(&self, _request: AwsSigv4Fetch) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl ServiceBindingHost for NoService {
    async fn fetch(&self, _request: ServiceFetch) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl DurableObjectHost for NoDurableObject {
    async fn fetch(&self, _request: DurableObjectFetch) -> Result<HttpResponse, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl KvHost for NoKv {
    async fn get(&self, _request: KvGet) -> Result<Option<Vec<u8>>, HostError> {
        Err(HostError)
    }

    async fn put(&self, _request: KvPut) -> Result<(), HostError> {
        Err(HostError)
    }

    async fn delete(&self, _request: KvGet) -> Result<bool, HostError> {
        Err(HostError)
    }

    async fn list(&self, _request: KvList) -> Result<ListPage, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl AiHost for NoAi {
    async fn run(&self, _request: AiRun) -> Result<serde_json::Value, HostError> {
        Err(HostError)
    }
}

#[async_trait::async_trait]
impl CacheHost for NoCache {
    async fn match_entry(&self, _request: CacheGet) -> Result<Option<CacheEntry>, HostError> {
        Err(HostError)
    }

    async fn put_entry(&self, _entry: CachePut) -> Result<(), HostError> {
        Err(HostError)
    }

    async fn delete_entry(&self, _request: CacheGet) -> Result<bool, HostError> {
        Err(HostError)
    }
}
