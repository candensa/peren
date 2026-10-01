use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use object_store::{ObjectStore, PutPayload, memory::InMemory};
use peren_cell::{OwnershipLease, WorkerCell};
use peren_fleet::{RecoveryEvidence, RecoveryObservation, RecoveryPolicy};
use peren_primitives::{CellId, NodeId, OwnershipEpoch, StorageRevision};
use peren_replication::{ReplicaPayload, ReplicaRepository, RepositoryError as ReplicaError};
use peren_runtime::{
    HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle,
};
use uuid::Uuid;

fn node() -> NodeId {
    NodeId::from_uuid(Uuid::new_v4())
}

fn recovery(owner: NodeId, epoch: OwnershipEpoch) -> RecoveryEvidence {
    RecoveryPolicy::new(NonZeroUsize::new(2).unwrap(), 100)
        .authorize(&RecoveryObservation::new(
            owner,
            epoch,
            1_000,
            1_100,
            BTreeSet::from([node(), node()]),
        ))
        .unwrap()
}

fn bundle() -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(
                ModuleKind::JavaScript,
                b"export default { async fetch(request) {
                      if (new URL(request.url).pathname === '/increment') {
                        await Peren.storage.transaction(async (storage) => {
                          const current = await storage.get('counter');
                          const next = current === undefined ? 1 : current[0] + 1;
                          await storage.put('counter', new Uint8Array([next]));
                        });
                      }
                      const value = await Peren.storage.get('counter');
                      return new Response(String(value?.[0] ?? 0));
                    } };"
                    .as_slice(),
            )
            .unwrap(),
        )]),
    )
    .unwrap()
}

fn request(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("https://worker.invalid{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

fn isolate() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

fn invocation() -> InvocationLimits {
    InvocationLimits::new(1024, 10)
}

#[tokio::test]
async fn bucket_store_preserves_cas_and_replica_contracts() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    let cell = CellId::from_bytes([3; 32]);
    let lease = store.acquire(node(), cell).await.unwrap();

    store
        .publish_through(
            cell,
            OwnershipEpoch::new(1),
            StorageRevision::new(4),
            &ReplicaPayload {
                generation: StorageRevision::new(0),
                database: b"database".to_vec(),
                wal_header: Some([1; 32]),
                wal_frames: b"wal".to_vec(),
            },
        )
        .await
        .unwrap();
    let root = store.read_root(cell).await.unwrap().unwrap();
    assert_eq!(root.generation, StorageRevision::new(0));
    assert_eq!(root.revision, StorageRevision::new(4));
    assert_eq!(root.wal, vec![StorageRevision::new(4)]);
    assert_eq!(
        store
            .read_replica(&snapshot_key(
                cell,
                OwnershipEpoch::new(1),
                StorageRevision::new(0)
            ))
            .await
            .unwrap(),
        b"database"
    );
    assert_eq!(
        store
            .read_header(cell, OwnershipEpoch::new(1), StorageRevision::new(0))
            .await
            .unwrap(),
        [1; 32]
    );
    assert_eq!(
        store
            .read_replica(&wal_key(
                cell,
                OwnershipEpoch::new(1),
                StorageRevision::new(4)
            ))
            .await
            .unwrap(),
        b"wal"
    );
    let restored = store.restore(cell).await.unwrap().unwrap();

    assert_eq!(restored.epoch, OwnershipEpoch::new(1));
    assert_eq!(restored.generation, StorageRevision::new(0));
    assert_eq!(restored.revision, StorageRevision::new(4));
    assert_eq!(restored.database, b"database");
    assert_eq!(&restored.wal[..32], &[1; 32]);
    assert_eq!(&restored.wal[32..], b"wal");
    let root = store.read_root(cell).await.unwrap().unwrap();
    assert_eq!(root.generation, StorageRevision::new(0));
    assert_eq!(root.revision, StorageRevision::new(4));
    lease.release().await.unwrap();
    let next = store.acquire(node(), cell).await.unwrap();
    assert_eq!(next.epoch(), OwnershipEpoch::new(2));
}

#[tokio::test]
async fn immutable_snapshot_identity_rejects_different_bytes() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    let cell = CellId::from_bytes([4; 32]);
    let _lease = store.acquire(node(), cell).await.unwrap();
    store
        .publish_through(
            cell,
            OwnershipEpoch::new(1),
            StorageRevision::new(1),
            &ReplicaPayload {
                generation: StorageRevision::new(0),
                database: b"first".to_vec(),
                wal_header: None,
                wal_frames: Vec::new(),
            },
        )
        .await
        .unwrap();

    let error = store
        .publish_through(
            cell,
            OwnershipEpoch::new(1),
            StorageRevision::new(1),
            &ReplicaPayload {
                generation: StorageRevision::new(0),
                database: b"different".to_vec(),
                wal_header: None,
                wal_frames: Vec::new(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ReplicaError::Malformed));
}

#[tokio::test]
async fn recovery_uses_observed_version_and_fences_the_old_lease() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    let cell = CellId::from_bytes([5; 32]);
    let first_node = node();
    let old = store.acquire(first_node, cell).await.unwrap();
    let recovered = store
        .recover(node(), cell, recovery(first_node, OwnershipEpoch::new(1)))
        .await
        .unwrap();

    assert!(old.verify().await.is_err());
    assert_eq!(recovered.epoch(), OwnershipEpoch::new(2));
    assert!(
        store
            .recover(node(), cell, recovery(first_node, OwnershipEpoch::new(1)))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn restore_rejects_wal_without_a_snapshot_baseline() {
    let objects = Arc::new(InMemory::new());
    let store = BucketStore::new(objects.clone());
    let cell = CellId::from_bytes([6; 32]);
    let epoch = OwnershipEpoch::new(1);
    let revision = StorageRevision::new(1);
    objects
        .put(
            &wal_key(cell, epoch, revision),
            PutPayload::from_static(b"frames"),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.restore(cell).await,
        Err(ReplicaError::Malformed)
    ));
    assert_eq!(
        snapshot_key(cell, epoch, revision).as_ref(),
        format!("cells/{cell}/snapshot/e1/00000000000000000001.sqlite")
    );
    assert_eq!(
        wal_key(cell, epoch, revision).as_ref(),
        format!("cells/{cell}/wal/e1/00000000000000000001.bin")
    );
}

#[tokio::test]
async fn pruning_retains_the_newest_complete_restore_point() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    let cell = CellId::from_bytes([8; 32]);
    for epoch in 1_u8..=3 {
        let lease = store.acquire(node(), cell).await.unwrap();
        store
            .publish_through(
                cell,
                OwnershipEpoch::new(u64::from(epoch)),
                StorageRevision::new(1),
                &ReplicaPayload {
                    generation: StorageRevision::new(0),
                    database: vec![epoch],
                    wal_header: Some([epoch; 32]),
                    wal_frames: vec![epoch],
                },
            )
            .await
            .unwrap();
        lease.release().await.unwrap();
    }

    assert_eq!(store.prune(cell, NonZeroUsize::MIN).await.unwrap(), 2);
    let restored = store.restore(cell).await.unwrap().unwrap();
    assert_eq!(restored.epoch, OwnershipEpoch::new(3));
    assert_eq!(restored.generation, StorageRevision::new(0));
    assert_eq!(restored.revision, StorageRevision::new(1));
    assert_eq!(restored.database, [3]);
    assert_eq!(restored.wal, [[3; 32].as_slice(), &[3]].concat());
    assert_eq!(
        store
            .positions(cell, ReplicaKind::Snapshot)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store.positions(cell, ReplicaKind::Wal).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn restore_uses_authoritative_root_instead_of_newest_listed_object() {
    let objects = Arc::new(InMemory::new());
    let store = BucketStore::new(objects.clone());
    let cell = CellId::from_bytes([9; 32]);
    let epoch = OwnershipEpoch::new(4);
    objects
        .put(
            &snapshot_key(cell, epoch, StorageRevision::new(1)),
            PutPayload::from_static(b"selected"),
        )
        .await
        .unwrap();
    objects
        .put(
            &wal_generation_key(cell, epoch, StorageRevision::new(1)),
            PutPayload::from(vec![1; 32]),
        )
        .await
        .unwrap();
    objects
        .put(
            &wal_key(cell, epoch, StorageRevision::new(2)),
            PutPayload::from_static(b"unselected"),
        )
        .await
        .unwrap();
    store
        .write_root(ReplicaRoot {
            cell,
            epoch,
            generation: StorageRevision::new(1),
            revision: StorageRevision::new(1),
            wal: Vec::new(),
        })
        .await
        .unwrap();

    let restored = store.restore(cell).await.unwrap().unwrap();

    assert_eq!(restored.epoch, epoch);
    assert_eq!(restored.generation, StorageRevision::new(1));
    assert_eq!(restored.revision, StorageRevision::new(1));
    assert_eq!(restored.database, b"selected");
    assert!(restored.wal.is_empty());
}

#[tokio::test]
async fn restores_the_released_snapshot_wal_and_header_layout() {
    let objects = Arc::new(InMemory::new());
    let store = BucketStore::new(objects.clone());
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/current/persistence");
    let cell = CellId::from_bytes(std::array::from_fn(|index| u8::try_from(index).unwrap()));
    let epoch = OwnershipEpoch::new(7);
    let snapshot = std::fs::read(fixture.join("snapshot.sqlite")).unwrap();
    let wal = std::fs::read(fixture.join("wal.bin")).unwrap();
    let (header, frames) = wal.split_at(32);
    objects
        .put(
            &snapshot_key(cell, epoch, StorageRevision::new(40)),
            PutPayload::from(snapshot.clone()),
        )
        .await
        .unwrap();
    objects
        .put(
            &wal_key(cell, epoch, StorageRevision::new(42)),
            PutPayload::from(frames.to_vec()),
        )
        .await
        .unwrap();
    objects
        .put(
            &wal_generation_key(cell, epoch, StorageRevision::new(40)),
            PutPayload::from(header.to_vec()),
        )
        .await
        .unwrap();

    let restored = store.restore(cell).await.unwrap().unwrap();
    assert_eq!(restored.epoch, epoch);
    assert_eq!(restored.generation, StorageRevision::new(40));
    assert_eq!(restored.revision, StorageRevision::new(42));
    assert_eq!(restored.database, snapshot);
    assert_eq!(restored.wal, wal);
}

#[tokio::test]
async fn checkpoint_starts_a_new_generation_and_prunes_the_old_chain() {
    let root = tempfile::tempdir().unwrap();
    let first_path = root.path().join("first.sqlite");
    let second_path = root.path().join("second.sqlite");
    let store = BucketStore::new(Arc::new(InMemory::new()));
    let cell_id = CellId::from_bytes([10; 32]);
    let lease = store.acquire(node(), cell_id).await.unwrap();
    let mut cell = WorkerCell::activate(
        &first_path,
        lease,
        store.clone(),
        bundle(),
        isolate(),
        peren_runtime::WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    cell.checkpoint().await.unwrap();
    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    cell.release().await.unwrap();

    let replica = store.restore(cell_id).await.unwrap().unwrap();
    assert_eq!(
        store
            .positions(cell_id, ReplicaKind::Snapshot)
            .await
            .unwrap()
            .len(),
        1
    );
    std::fs::write(&second_path, replica.database).unwrap();
    std::fs::write(format!("{}-wal", second_path.display()), replica.wal).unwrap();
    let lease = store.acquire(node(), cell_id).await.unwrap();
    let mut restored = WorkerCell::activate(
        &second_path,
        lease,
        store,
        bundle(),
        isolate(),
        peren_runtime::WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    assert_eq!(
        restored
            .dispatch_http(request("/read"), invocation())
            .await
            .unwrap()
            .body,
        b"2"
    );
    restored.release().await.unwrap();
}
