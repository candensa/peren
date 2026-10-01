use std::{collections::BTreeMap, sync::Arc};

use object_store::memory::InMemory;
use peren_provider_object_store::R2Store;
use peren_runtime::R2BucketHost;

#[tokio::test]
async fn r2_store_reads_writes_lists_and_deletes_objects() {
    let store = R2Store::new(Arc::new(InMemory::new()));
    store
        .put(peren_runtime::R2Put {
            bucket: "uploads".into(),
            key: "tenant/a.txt".into(),
            body: b"hello".to_vec(),
            content_type: Some("text/plain".into()),
            custom_metadata: BTreeMap::from([("owner".into(), "ada".into())]),
        })
        .await
        .unwrap();
    store
        .put(peren_runtime::R2Put {
            bucket: "uploads".into(),
            key: "tenant/b.txt".into(),
            body: vec![2],
            content_type: None,
            custom_metadata: BTreeMap::new(),
        })
        .await
        .unwrap();

    let object = store
        .get(peren_runtime::R2Get {
            bucket: "uploads".into(),
            key: "tenant/a.txt".into(),
        })
        .await
        .unwrap()
        .unwrap();
    let page = store
        .list(peren_runtime::R2List {
            bucket: "uploads".into(),
            prefix: Some("tenant/".into()),
            cursor: None,
            limit: Some(1),
        })
        .await
        .unwrap();
    store
        .delete(peren_runtime::R2Delete {
            bucket: "uploads".into(),
            key: "tenant/a.txt".into(),
        })
        .await
        .unwrap();

    assert_eq!(object.body, b"hello");
    assert_eq!(object.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        object.custom_metadata.get("owner").map(String::as_str),
        Some("ada")
    );
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, "tenant/a.txt");
    assert_eq!(page.cursor.as_deref(), Some("tenant/a.txt"));
    assert!(!page.list_complete);
    assert!(
        store
            .get(peren_runtime::R2Get {
                bucket: "uploads".into(),
                key: "tenant/a.txt".into(),
            })
            .await
            .unwrap()
            .is_none()
    );
}
