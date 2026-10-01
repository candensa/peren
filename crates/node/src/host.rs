mod redis;
mod route;
mod store;

pub use route::RoutedHost;
pub use store::{CacheStore, KvStore};

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use peren_provider_object_store::R2Store;
    use peren_provider_turso::TursoStore;
    use peren_runtime::{
        CacheEntry, CacheGet, CacheHost, CachePut, DurableStorageHost, ListEntry, ListOptions,
        QueueProducerHost, QueueSend, R2BucketHost, R2Delete, R2Get, R2List, R2Put, SqlQuery,
        SqlValue,
    };
    use peren_storage::CellStorage;
    use tokio::sync::Mutex;

    use super::{CacheStore, RoutedHost, redis};
    use crate::process::QueueBrokerState;
    fn cache() -> Arc<Mutex<BTreeMap<(String, String), CacheEntry>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    use peren_primitives::StorageRevision;
    use uuid::Uuid;

    #[tokio::test]
    async fn routes_named_d1_queries_to_turso_without_using_cell_sql() {
        let cell = Arc::new(Mutex::new(
            CellStorage::open(std::path::Path::new(":memory:")).unwrap(),
        ));
        let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
        let turso = Arc::new(Mutex::new(TursoStore::local(&path).await.unwrap()));
        let host = RoutedHost::new(
            cell,
            BTreeMap::from([("DB".into(), turso)]),
            BTreeMap::new(),
            QueueBrokerState::memory(),
            CacheStore::Memory(cache()),
            BTreeMap::new(),
        );

        host.begin().await.unwrap();
        host.sql(SqlQuery {
            database: Some("DB".into()),
            sql: "CREATE TABLE records(id INTEGER PRIMARY KEY, label TEXT)".into(),
            parameters: Vec::new(),
        })
        .await
        .unwrap();
        host.commit().await.unwrap();

        host.begin().await.unwrap();
        host.sql(SqlQuery {
            database: Some("DB".into()),
            sql: "INSERT INTO records(label) VALUES (?)".into(),
            parameters: vec![SqlValue::Text("turso".into())],
        })
        .await
        .unwrap();
        host.commit().await.unwrap();

        host.begin().await.unwrap();
        let result = host
            .sql(SqlQuery {
                database: Some("DB".into()),
                sql: "SELECT label FROM records WHERE id=?".into(),
                parameters: vec![SqlValue::Integer(1)],
            })
            .await
            .unwrap();
        host.commit().await.unwrap();

        assert_eq!(result.rows, vec![vec![SqlValue::Text("turso".into())]]);
        assert_eq!(host.load("records", b"1").await.unwrap(), None);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn keeps_native_kv_on_cell_storage_while_turso_is_configured() {
        let cell = Arc::new(Mutex::new(
            CellStorage::open(std::path::Path::new(":memory:")).unwrap(),
        ));
        let path = std::env::temp_dir().join(format!("{}.sqlite", Uuid::new_v4()));
        let turso = Arc::new(Mutex::new(TursoStore::local(&path).await.unwrap()));
        let host = RoutedHost::new(
            Arc::clone(&cell),
            BTreeMap::from([("DB".into(), turso)]),
            BTreeMap::new(),
            QueueBrokerState::memory(),
            CacheStore::Memory(cache()),
            BTreeMap::new(),
        );

        host.begin().await.unwrap();
        DurableStorageHost::put(&host, "cache", b"alpha", b"one")
            .await
            .unwrap();
        let revision = host.commit().await.unwrap();
        let page = DurableStorageHost::list(
            &host,
            "cache",
            ListOptions {
                prefix: None,
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();

        assert_eq!(revision, StorageRevision::new(1));
        assert_eq!(
            page.keys,
            vec![ListEntry {
                name: "alpha".into()
            }]
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn routes_named_r2_objects_to_bucket_store() {
        let cell = Arc::new(Mutex::new(
            CellStorage::open(std::path::Path::new(":memory:")).unwrap(),
        ));
        let r2 = Arc::new(R2Store::new(
            Arc::new(object_store::memory::InMemory::new()),
        ));
        let host = RoutedHost::new(
            cell,
            BTreeMap::new(),
            BTreeMap::from([("uploads".into(), r2)]),
            QueueBrokerState::memory(),
            CacheStore::Memory(cache()),
            BTreeMap::new(),
        );

        R2BucketHost::put(
            &host,
            R2Put {
                bucket: "uploads".into(),
                key: "avatars/a.png".into(),
                body: b"image".to_vec(),
                content_type: Some("image/png".into()),
                custom_metadata: BTreeMap::from([("owner".into(), "ada".into())]),
            },
        )
        .await
        .unwrap();

        let object = peren_runtime::R2BucketHost::get(
            &host,
            R2Get {
                bucket: "uploads".into(),
                key: "avatars/a.png".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(object.body, b"image");

        let page = R2BucketHost::list(
            &host,
            R2List {
                bucket: "uploads".into(),
                prefix: Some("avatars".into()),
                cursor: None,
                limit: Some(10),
            },
        )
        .await
        .unwrap();
        assert_eq!(page.objects.len(), 1);

        R2BucketHost::delete(
            &host,
            R2Delete {
                bucket: "uploads".into(),
                key: "avatars/a.png".into(),
            },
        )
        .await
        .unwrap();
        assert!(
            peren_runtime::R2BucketHost::get(
                &host,
                R2Get {
                    bucket: "uploads".into(),
                    key: "avatars/a.png".into(),
                }
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn redis_endpoint_parses_auth_host_port_and_database() {
        let endpoint = redis::endpoint("redis://:secret@redis.internal:6380/2").unwrap();

        assert_eq!(endpoint.address, "redis.internal:6380");
        assert_eq!(endpoint.password.as_deref(), Some("secret"));
        assert_eq!(endpoint.database, Some(2));
    }

    #[test]
    fn redis_endpoint_defaults_to_standard_port() {
        let endpoint = redis::endpoint("redis://localhost").unwrap();

        assert_eq!(endpoint.address, "localhost:6379");
        assert_eq!(endpoint.password, None);
        assert_eq!(endpoint.database, None);
    }

    #[tokio::test]
    #[ignore = "requires PEREN_REDIS_URL and a live Redis server"]
    async fn live_redis_cache_persists_and_deletes_entries() {
        let url = std::env::var("PEREN_REDIS_URL").expect("PEREN_REDIS_URL is required");
        let cache = CacheStore::redis(url, format!("peren-test-{}", uuid::Uuid::new_v4()));
        let key = "https://example.com/redis-cache";

        cache
            .put_entry(CachePut {
                cache: "default".into(),
                key: key.into(),
                status: 204,
                headers: vec![("cache-control".into(), "max-age=60".into())],
                body: b"redis-backed".to_vec(),
            })
            .await
            .unwrap();

        let entry = cache
            .match_entry(CacheGet {
                cache: "default".into(),
                key: key.into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.status, 204);
        assert_eq!(
            entry.headers,
            vec![("cache-control".into(), "max-age=60".into())]
        );
        assert_eq!(entry.body, b"redis-backed");

        assert!(
            cache
                .delete_entry(CacheGet {
                    cache: "default".into(),
                    key: key.into(),
                })
                .await
                .unwrap()
        );
        assert!(
            cache
                .match_entry(CacheGet {
                    cache: "default".into(),
                    key: key.into(),
                })
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn native_kv_cache_persists_entries_in_cell_storage() {
        let path = std::env::temp_dir().join(format!("peren-cache-{}.sqlite", Uuid::new_v4()));
        let storage = Arc::new(Mutex::new(CellStorage::open(&path).unwrap()));
        let cache = CacheStore::native_kv(Arc::clone(&storage), "page-cache");

        cache
            .put_entry(CachePut {
                cache: "default".into(),
                key: "https://example.com/page".into(),
                status: 202,
                headers: vec![("etag".into(), "kv".into())],
                body: b"kv-backed".to_vec(),
            })
            .await
            .unwrap();

        let reopened = CacheStore::native_kv(
            Arc::new(Mutex::new(CellStorage::open(&path).unwrap())),
            "page-cache",
        );
        let entry = reopened
            .match_entry(CacheGet {
                cache: "default".into(),
                key: "https://example.com/page".into(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(entry.status, 202);
        assert_eq!(entry.headers, vec![("etag".into(), "kv".into())]);
        assert_eq!(entry.body, b"kv-backed");
        assert!(
            reopened
                .delete_entry(CacheGet {
                    cache: "default".into(),
                    key: "https://example.com/page".into(),
                })
                .await
                .unwrap()
        );
        assert!(
            reopened
                .match_entry(CacheGet {
                    cache: "default".into(),
                    key: "https://example.com/page".into(),
                })
                .await
                .unwrap()
                .is_none()
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn bucket_cache_persists_entries_in_object_store() {
        let store = Arc::new(R2Store::new(
            Arc::new(object_store::memory::InMemory::new()),
        ));
        let cache = CacheStore::bucket(Arc::clone(&store), "edge");

        cache
            .put_entry(CachePut {
                cache: "default".into(),
                key: "https://example.com/data".into(),
                status: 203,
                headers: vec![("etag".into(), "v1".into())],
                body: b"cached".to_vec(),
            })
            .await
            .unwrap();

        let entry = cache
            .match_entry(CacheGet {
                cache: "default".into(),
                key: "https://example.com/data".into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.status, 203);
        assert_eq!(entry.headers, vec![("etag".into(), "v1".into())]);
        assert_eq!(entry.body, b"cached");

        assert!(
            cache
                .delete_entry(CacheGet {
                    cache: "default".into(),
                    key: "https://example.com/data".into(),
                })
                .await
                .unwrap()
        );
        assert!(
            cache
                .match_entry(CacheGet {
                    cache: "default".into(),
                    key: "https://example.com/data".into(),
                })
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn sends_queue_messages_to_broker() {
        let cell = Arc::new(Mutex::new(
            CellStorage::open(std::path::Path::new(":memory:")).unwrap(),
        ));
        let queues = QueueBrokerState::memory();
        let host = RoutedHost::new(
            cell,
            BTreeMap::new(),
            BTreeMap::new(),
            queues.clone(),
            CacheStore::Memory(cache()),
            BTreeMap::new(),
        );

        QueueProducerHost::send(
            &host,
            QueueSend {
                queue: "jobs".into(),
                body: b"work".to_vec(),
                content_type: Some("text/plain".into()),
                partition: Some("tenant-a".into()),
                delay_seconds: None,
                dedup_id: Some("job-1".into()),
            },
        )
        .await
        .unwrap();

        let leases = queues
            .lease_batch("jobs", 1, chrono::Utc::now(), None)
            .await
            .unwrap()
            .into_leases();
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].message.body, b"work");
        assert_eq!(leases[0].message.partition.as_deref(), Some("tenant-a"));
    }
}
