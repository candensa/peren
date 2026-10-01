use peren_primitives::StorageRevision;
use peren_runtime::{
    Capabilities, HostError, HttpRequest, HttpResponse, InvocationLimits, IsolateLimits, Module,
    ModuleKind, ModuleName, OutboundFetchHost, WorkerBundle, WorkerEnvironment, WorkerRuntime,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub(crate) fn limits() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

pub(crate) fn bundle(source: &str) -> WorkerBundle {
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

pub(crate) fn worker_request() -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: "https://worker.invalid/".into(),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

#[derive(Default)]
pub(crate) struct Host;

#[async_trait::async_trait]
impl peren_runtime::DurableStorageHost for Host {
    async fn begin(&self) -> Result<(), HostError> {
        Ok(())
    }
    async fn load(&self, _scope: &str, _key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        Ok(None)
    }
    async fn put(&self, _scope: &str, _key: &[u8], _value: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
    async fn delete(&self, _scope: &str, _key: &[u8]) -> Result<bool, HostError> {
        Ok(false)
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
        Ok(StorageRevision::new(1))
    }
    async fn rollback(&self) -> Result<(), HostError> {
        Ok(())
    }
    async fn attachment(&self, _id: &str) -> Result<Option<Vec<u8>>, HostError> {
        Ok(None)
    }
    async fn set_attachment(&self, _id: &str, _bytes: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
    async fn delete_attachment(&self, _id: &str) -> Result<bool, HostError> {
        Ok(false)
    }
}

#[path = "providers/ai.rs"]
mod ai;
#[path = "providers/kv.rs"]
mod kv;
#[path = "providers/vector.rs"]
mod vector;
