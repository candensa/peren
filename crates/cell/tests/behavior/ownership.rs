use super::*;

#[tokio::test]
async fn read_is_fenced_when_ownership_changes_during_dispatch() {
    let mut cell = WorkerCell::activate(
        Path::new(":memory:"),
        lease(2),
        repository(false),
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    assert!(matches!(
        cell.dispatch_http(request("/read"), invocation()).await,
        Err(CellError::Lease(_))
    ));
    assert_eq!(cell.state(), CellState::Fenced);
}

#[tokio::test]
async fn publication_failure_drains_and_rejects_further_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.sqlite");
    let mut cell = WorkerCell::activate(
        &path,
        lease(usize::MAX),
        repository(true),
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    assert!(matches!(
        cell.dispatch_http(request("/increment"), invocation())
            .await,
        Err(CellError::Repository(RepositoryError::Unavailable))
    ));
    assert_eq!(cell.last_commit(), None);
    assert_eq!(cell.state(), CellState::Draining);
    assert!(matches!(
        cell.dispatch_http(request("/read"), invocation()).await,
        Err(CellError::Inactive(CellState::Draining))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn response_is_rejected_when_ownership_fails_after_publication() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.sqlite");
    let repository = repository(false);
    let published = Arc::clone(&repository.image);
    let checks = Arc::new(AtomicUsize::new(0));
    let mut cell = WorkerCell::activate(
        &path,
        Lease {
            cell: CellId::from_bytes([1; 32]),
            owner: NodeId::from_uuid(Uuid::new_v4()),
            checks: Arc::clone(&checks),
            fail_at: 3,
            releases: None,
        },
        repository,
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    let result = cell
        .dispatch_http(request("/increment"), invocation())
        .await;
    assert!(matches!(result, Err(CellError::Lease(_))));

    assert_eq!(checks.load(Ordering::SeqCst), 3);
    assert_eq!(cell.state(), CellState::Fenced);
    assert!(published.lock().unwrap().is_some());
    assert!(matches!(
        cell.dispatch_http(request("/read"), invocation()).await,
        Err(CellError::Inactive(CellState::Fenced))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_prunes_old_replica_generations() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.sqlite");
    let repository = repository(false);
    let prunes = Arc::clone(&repository.prunes);
    let mut cell = WorkerCell::activate(
        &path,
        lease(usize::MAX),
        repository,
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    cell.checkpoint().await.unwrap();

    assert_eq!(prunes.load(Ordering::SeqCst), 1);
    cell.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_threshold_collapses_published_wal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.sqlite");
    let repository = repository(false);
    let prunes = Arc::clone(&repository.prunes);
    let mut cell = WorkerCell::activate(
        &path,
        lease(usize::MAX),
        repository,
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    assert!(cell.last_commit().unwrap().wal_bytes > 0);

    cell.checkpoint_if_wal_exceeds(1).await.unwrap();

    assert_eq!(cell.last_commit().unwrap().wal_bytes, 0);
    assert_eq!(prunes.load(Ordering::SeqCst), 1);
    cell.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_threshold_uses_wal_bytes_since_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.sqlite");
    let repository = repository(false);
    let prunes = Arc::clone(&repository.prunes);
    let mut cell = WorkerCell::activate(
        &path,
        lease(usize::MAX),
        repository,
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    let first_wal_bytes = cell.last_commit().unwrap().wal_bytes;
    cell.checkpoint_if_wal_exceeds(u64::try_from(first_wal_bytes).unwrap().saturating_add(1))
        .await
        .unwrap();
    assert_eq!(prunes.load(Ordering::SeqCst), 0);

    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    cell.checkpoint_if_wal_exceeds(u64::try_from(first_wal_bytes).unwrap().saturating_add(1))
        .await
        .unwrap();
    assert_eq!(cell.last_commit().unwrap().wal_bytes, 0);
    assert_eq!(prunes.load(Ordering::SeqCst), 1);
    cell.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn read_only_dispatch_does_not_publish_a_replica() {
    let repository = repository(false);
    let published = Arc::clone(&repository.image);
    let mut cell = WorkerCell::activate(
        Path::new(":memory:"),
        lease(usize::MAX),
        repository,
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    let response = cell
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(response.body, b"0");
    assert_eq!(cell.last_commit(), None);
    assert!(published.lock().unwrap().is_none());
    cell.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn delete_purges_local_state_and_releases_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cell.sqlite");
    let releases = Arc::new(AtomicUsize::new(0));
    let mut cell = WorkerCell::activate(
        &path,
        Lease {
            cell: CellId::from_bytes([1; 32]),
            owner: NodeId::from_uuid(Uuid::new_v4()),
            checks: Arc::new(AtomicUsize::new(0)),
            fail_at: usize::MAX,
            releases: Some(Arc::clone(&releases)),
        },
        repository(false),
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    cell.dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();

    cell.delete().await.unwrap();

    assert_eq!(releases.load(Ordering::SeqCst), 1);
    let storage = CellStorage::open(&path).unwrap();
    assert_eq!(storage.get("default", b"counter").unwrap(), None);
}

#[tokio::test(flavor = "current_thread")]
async fn failed_worker_transaction_rolls_back_without_publication() {
    let repository = repository(false);
    let published = Arc::clone(&repository.image);
    let mut cell = WorkerCell::activate(
        Path::new(":memory:"),
        lease(usize::MAX),
        repository,
        failing_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();

    assert!(matches!(
        cell.dispatch_http(request("/fail"), invocation()).await,
        Err(CellError::Engine(_))
    ));
    assert_eq!(cell.last_commit(), None);
    let response = cell
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(response.body, b"0");
    assert_eq!(cell.last_commit(), None);
    assert!(published.lock().unwrap().is_none());
    cell.release().await.unwrap();
}
