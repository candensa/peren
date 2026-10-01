use std::sync::Arc;

use object_store::memory::InMemory;
use peren_provider_object_store::{BlobPart, TransactionalBlobStore};
use peren_replication::RepositoryError as ReplicaError;

#[tokio::test]
async fn transactional_blob_is_invisible_until_manifest_commit() {
    let store = TransactionalBlobStore::new(Arc::new(InMemory::new()));
    let first = store
        .stage("avatars/user", "tx-1", 0, b"hello ")
        .await
        .unwrap();
    let second = store
        .stage("avatars/user", "tx-1", 1, b"world")
        .await
        .unwrap();

    assert!(store.read("avatars/user").await.unwrap().is_none());
    let manifest = store
        .commit("avatars/user", "tx-1", vec![second, first])
        .await
        .unwrap();
    let read = store.read("avatars/user").await.unwrap().unwrap();

    assert_eq!(
        manifest
            .parts
            .iter()
            .map(|part| part.index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(read, b"hello world");
}

#[tokio::test]
async fn transactional_blob_commit_refuses_missing_or_mismatched_parts() {
    let store = TransactionalBlobStore::new(Arc::new(InMemory::new()));
    let part = store
        .stage("reports/month", "tx-2", 0, b"ok")
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit(
                "reports/month",
                "tx-2",
                vec![BlobPart { index: 1, size: 4 }],
            )
            .await,
        Err(ReplicaError::Unavailable)
    ));
    assert!(matches!(
        store
            .commit(
                "reports/month",
                "tx-2",
                vec![BlobPart {
                    index: part.index,
                    size: part.size + 1
                }],
            )
            .await,
        Err(ReplicaError::Malformed)
    ));
    assert!(store.read("reports/month").await.unwrap().is_none());
}
