use std::{collections::BTreeMap, sync::Arc};

use peren_provider_object_store::R2Store;
use peren_runtime::{
    CacheEntry, CacheGet, CacheHost, CachePut, HostError, KvGet, KvHost, KvList, KvPut, ListEntry,
    ListPage, R2BucketHost, R2Delete, R2Get, R2List, R2Put,
};
use peren_storage::{CellStorage, CellStore};
use sha2::Digest;
use tokio::sync::Mutex;

use super::redis;

#[derive(Clone)]
pub enum KvStore {
    Bucket {
        store: Arc<R2Store>,
        prefix: Arc<str>,
    },
    Redis {
        url: Arc<str>,
        prefix: Arc<str>,
    },
}

impl KvStore {
    #[must_use]
    pub fn bucket(store: Arc<R2Store>, prefix: impl Into<Arc<str>>) -> Self {
        Self::Bucket {
            store,
            prefix: prefix.into(),
        }
    }

    #[must_use]
    pub fn redis(url: impl Into<Arc<str>>, prefix: impl Into<Arc<str>>) -> Self {
        Self::Redis {
            url: url.into(),
            prefix: prefix.into(),
        }
    }

    fn key(prefix: &str, namespace: &str, key: &str) -> String {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("kv/{namespace}/{key}")
        } else {
            format!("{prefix}/kv/{namespace}/{key}")
        }
    }

    fn prefix(prefix: &str, namespace: &str, key_prefix: Option<&str>) -> String {
        Self::key(prefix, namespace, key_prefix.unwrap_or_default())
    }
}

#[async_trait::async_trait]
impl KvHost for KvStore {
    async fn get(&self, request: KvGet) -> Result<Option<Vec<u8>>, HostError> {
        match self {
            Self::Bucket { store, prefix } => {
                let key = Self::key(prefix, &request.namespace, &request.key);
                Ok(store
                    .get(R2Get {
                        bucket: String::new(),
                        key,
                    })
                    .await?
                    .map(|object| object.body))
            }
            Self::Redis { url, prefix } => {
                let key = Self::key(prefix, &request.namespace, &request.key);
                redis::get(url, &key).await
            }
        }
    }

    async fn put(&self, request: KvPut) -> Result<(), HostError> {
        match self {
            Self::Bucket { store, prefix } => {
                let key = Self::key(prefix, &request.namespace, &request.key);
                store
                    .put(R2Put {
                        bucket: String::new(),
                        key,
                        body: request.value,
                        content_type: None,
                        custom_metadata: BTreeMap::new(),
                    })
                    .await
            }
            Self::Redis { url, prefix } => {
                let key = Self::key(prefix, &request.namespace, &request.key);
                redis::set(url, &key, &request.value).await
            }
        }
    }

    async fn delete(&self, request: KvGet) -> Result<bool, HostError> {
        match self {
            Self::Bucket { store, prefix } => {
                let key = Self::key(prefix, &request.namespace, &request.key);
                let existed = store
                    .get(R2Get {
                        bucket: String::new(),
                        key: key.clone(),
                    })
                    .await?
                    .is_some();
                store
                    .delete(R2Delete {
                        bucket: String::new(),
                        key,
                    })
                    .await?;
                Ok(existed)
            }
            Self::Redis { url, prefix } => {
                let key = Self::key(prefix, &request.namespace, &request.key);
                redis::del(url, &key).await
            }
        }
    }

    async fn list(&self, request: KvList) -> Result<ListPage, HostError> {
        let limit = request.limit.unwrap_or(1000).max(1);
        match self {
            Self::Bucket { store, prefix } => {
                let base = Self::prefix(prefix, &request.namespace, None);
                let wanted = request.prefix.unwrap_or_default();
                let page = store
                    .list(R2List {
                        bucket: String::new(),
                        prefix: Some(base.clone()),
                        cursor: request
                            .cursor
                            .as_ref()
                            .map(|cursor| Self::key(prefix, &request.namespace, cursor)),
                        limit: Some(limit + 1),
                    })
                    .await?;
                let mut keys = page
                    .objects
                    .into_iter()
                    .filter_map(|object| {
                        object.key.strip_prefix(&base).and_then(|name| {
                            name.starts_with(&wanted).then(|| ListEntry {
                                name: name.to_string(),
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                keys.sort_by(|left, right| left.name.cmp(&right.name));
                let cursor = if keys.len() > limit {
                    keys.truncate(limit);
                    keys.last().map(|entry| entry.name.as_bytes().to_vec())
                } else {
                    page.cursor.and_then(|cursor| {
                        cursor
                            .strip_prefix(&base)
                            .map(|value| value.as_bytes().to_vec())
                    })
                };
                Ok(ListPage {
                    keys,
                    list_complete: cursor.is_none(),
                    cursor,
                })
            }
            Self::Redis { url, prefix } => {
                let base = Self::prefix(prefix, &request.namespace, None);
                let pattern = format!(
                    "{}*",
                    Self::prefix(prefix, &request.namespace, request.prefix.as_deref())
                );
                let start_after = request
                    .cursor
                    .map(|cursor| Self::key(prefix, &request.namespace, &cursor));
                let names = redis::scan(url, &pattern).await?;
                let mut names = names
                    .into_iter()
                    .filter(|key| start_after.as_ref().is_none_or(|cursor| key > cursor))
                    .collect::<Vec<_>>();
                names.sort();
                let mut keys = names
                    .into_iter()
                    .filter_map(|key| {
                        key.strip_prefix(&base).map(|name| ListEntry {
                            name: name.to_string(),
                        })
                    })
                    .collect::<Vec<_>>();
                let cursor = if keys.len() > limit {
                    keys.truncate(limit);
                    keys.last().map(|entry| entry.name.as_bytes().to_vec())
                } else {
                    None
                };
                Ok(ListPage {
                    keys,
                    list_complete: cursor.is_none(),
                    cursor,
                })
            }
        }
    }
}

#[derive(Clone)]
pub enum CacheStore {
    Memory(Arc<Mutex<BTreeMap<(String, String), CacheEntry>>>),
    NativeKv {
        storage: Arc<Mutex<CellStorage>>,
        namespace: Arc<str>,
    },
    Bucket {
        store: Arc<R2Store>,
        prefix: Arc<str>,
    },
    Redis {
        url: Arc<str>,
        prefix: Arc<str>,
    },
}

impl CacheStore {
    #[must_use]
    pub fn memory() -> Self {
        Self::Memory(Arc::new(Mutex::new(BTreeMap::new())))
    }

    #[must_use]
    pub fn native_kv(storage: Arc<Mutex<CellStorage>>, namespace: impl Into<Arc<str>>) -> Self {
        Self::NativeKv {
            storage,
            namespace: namespace.into(),
        }
    }

    #[must_use]
    pub fn bucket(store: Arc<R2Store>, prefix: impl Into<Arc<str>>) -> Self {
        Self::Bucket {
            store,
            prefix: prefix.into(),
        }
    }

    #[must_use]
    pub fn redis(url: impl Into<Arc<str>>, prefix: impl Into<Arc<str>>) -> Self {
        Self::Redis {
            url: url.into(),
            prefix: prefix.into(),
        }
    }

    fn key(prefix: &str, cache: &str, key: &str) -> String {
        let digest = sha2::Sha256::digest(format!("{cache}\0{key}").as_bytes());
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("cache/{}", hex::encode(digest))
        } else {
            format!("{prefix}/cache/{}", hex::encode(digest))
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredCache {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[async_trait::async_trait]
impl CacheHost for CacheStore {
    async fn match_entry(&self, request: CacheGet) -> Result<Option<CacheEntry>, HostError> {
        match self {
            Self::Memory(entries) => Ok(entries
                .lock()
                .await
                .get(&(request.cache, request.key))
                .cloned()),
            Self::NativeKv { storage, namespace } => {
                let key = Self::key("", &request.cache, &request.key);
                let mut storage = storage.lock().await;
                let Some(bytes) = storage
                    .load(namespace, key.as_bytes())
                    .await
                    .map_err(|_| HostError)?
                else {
                    return Ok(None);
                };
                decode_cache(&bytes).map(Some)
            }
            Self::Bucket { store, prefix } => {
                let key = Self::key(prefix, &request.cache, &request.key);
                let Some(object) = store
                    .get(peren_runtime::R2Get {
                        bucket: String::new(),
                        key,
                    })
                    .await?
                else {
                    return Ok(None);
                };
                decode_cache(&object.body).map(Some)
            }
            Self::Redis { url, prefix } => {
                let key = Self::key(prefix, &request.cache, &request.key);
                let Some(bytes) = redis::get(url, &key).await? else {
                    return Ok(None);
                };
                decode_cache(&bytes).map(Some)
            }
        }
    }

    async fn put_entry(&self, entry: CachePut) -> Result<(), HostError> {
        match self {
            Self::Memory(entries) => {
                entries.lock().await.insert(
                    (entry.cache, entry.key),
                    CacheEntry {
                        status: entry.status,
                        headers: entry.headers,
                        body: entry.body,
                    },
                );
                Ok(())
            }
            Self::NativeKv { storage, namespace } => {
                let key = Self::key("", &entry.cache, &entry.key);
                let body = encode_cache(entry)?;
                let mut storage = storage.lock().await;
                storage
                    .put(namespace, key.as_bytes(), &body)
                    .map(|_| ())
                    .map_err(|_| HostError)
            }
            Self::Bucket { store, prefix } => {
                let key = Self::key(prefix, &entry.cache, &entry.key);
                let body = encode_cache(entry)?;
                store
                    .put(peren_runtime::R2Put {
                        bucket: String::new(),
                        key,
                        body,
                        content_type: Some("application/json".into()),
                        custom_metadata: BTreeMap::new(),
                    })
                    .await
            }
            Self::Redis { url, prefix } => {
                let key = Self::key(prefix, &entry.cache, &entry.key);
                let body = encode_cache(entry)?;
                redis::set(url, &key, &body).await
            }
        }
    }

    async fn delete_entry(&self, request: CacheGet) -> Result<bool, HostError> {
        match self {
            Self::Memory(entries) => Ok(entries
                .lock()
                .await
                .remove(&(request.cache, request.key))
                .is_some()),
            Self::NativeKv { storage, namespace } => {
                let key = Self::key("", &request.cache, &request.key);
                let mut storage = storage.lock().await;
                storage
                    .delete(namespace, key.as_bytes())
                    .map(|deleted| deleted.value)
                    .map_err(|_| HostError)
            }
            Self::Bucket { store, prefix } => {
                let key = Self::key(prefix, &request.cache, &request.key);
                let existed = store
                    .get(peren_runtime::R2Get {
                        bucket: String::new(),
                        key: key.clone(),
                    })
                    .await?
                    .is_some();
                store
                    .delete(peren_runtime::R2Delete {
                        bucket: String::new(),
                        key,
                    })
                    .await?;
                Ok(existed)
            }
            Self::Redis { url, prefix } => {
                let key = Self::key(prefix, &request.cache, &request.key);
                redis::del(url, &key).await
            }
        }
    }
}

fn encode_cache(entry: CachePut) -> Result<Vec<u8>, HostError> {
    serde_json::to_vec(&StoredCache {
        status: entry.status,
        headers: entry.headers,
        body: entry.body,
    })
    .map_err(|_| HostError)
}

fn decode_cache(bytes: &[u8]) -> Result<CacheEntry, HostError> {
    let stored: StoredCache = serde_json::from_slice(bytes).map_err(|_| HostError)?;
    Ok(CacheEntry {
        status: stored.status,
        headers: stored.headers,
        body: stored.body,
    })
}
