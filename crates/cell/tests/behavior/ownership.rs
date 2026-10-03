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

async fn activate_with_post_publish_fence(
    path: &Path,
    bundle: WorkerBundle,
    checks: Arc<AtomicUsize>,
) -> (
    WorkerCell<Lease, Repository>,
    Arc<Mutex<Option<ReplicaImage>>>,
) {
    let repository = repository(false);
    let image = Arc::clone(&repository.image);
    let cell = WorkerCell::activate(
        path,
        Lease {
            cell: CellId::from_bytes([1; 32]),
            owner: NodeId::from_uuid(Uuid::new_v4()),
            checks,
            fail_at: 3,
            releases: None,
        },
        repository,
        bundle,
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    (cell, image)
}

fn assert_post_publish_fence(
    checks: &AtomicUsize,
    state: CellState,
    image: &Arc<Mutex<Option<ReplicaImage>>>,
) {
    assert_eq!(checks.load(Ordering::SeqCst), 3);
    assert_eq!(state, CellState::Fenced);
    assert!(image.lock().unwrap().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn background_events_reject_completion_when_ownership_fails_after_publication() {
    let directory = tempfile::tempdir().unwrap();

    let alarm_checks = Arc::new(AtomicUsize::new(0));
    let (mut alarm, alarm_image) = activate_with_post_publish_fence(
        &directory.path().join("alarm.sqlite"),
        alarm_bundle(),
        Arc::clone(&alarm_checks),
    )
    .await;
    assert!(matches!(
        alarm.dispatch_alarm().await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(&alarm_checks, alarm.state(), &alarm_image);

    let scheduled_checks = Arc::new(AtomicUsize::new(0));
    let (mut scheduled, scheduled_image) = activate_with_post_publish_fence(
        &directory.path().join("scheduled.sqlite"),
        scheduled_bundle(),
        Arc::clone(&scheduled_checks),
    )
    .await;
    assert!(matches!(
        scheduled
            .dispatch_scheduled(ScheduledEvent {
                scheduled_time_ms: 7_000,
                cron: "*/5 * * * *".into(),
            })
            .await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(&scheduled_checks, scheduled.state(), &scheduled_image);

    let queue_checks = Arc::new(AtomicUsize::new(0));
    let (mut queue, queue_image) = activate_with_post_publish_fence(
        &directory.path().join("queue.sqlite"),
        queue_bundle(),
        Arc::clone(&queue_checks),
    )
    .await;
    assert!(matches!(
        queue
            .dispatch_queue(QueueEvent {
                metrics: QueueMetrics::default(),
                queue: "jobs".into(),
                messages: vec![peren_runtime::QueueMessage {
                    id: "message-1".into(),
                    body: vec![9],
                    attempts: 1,
                    timestamp: 1_700_000_000_000,
                }],
            })
            .await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(&queue_checks, queue.state(), &queue_image);

    let tail_checks = Arc::new(AtomicUsize::new(0));
    let (mut tail, tail_image) = activate_with_post_publish_fence(
        &directory.path().join("tail.sqlite"),
        tail_bundle(),
        Arc::clone(&tail_checks),
    )
    .await;
    assert!(matches!(
        tail.dispatch_tail(TailEvent {
            events: vec![peren_runtime::TailRecord {
                outcome: "ok".into(),
                script: "worker".into(),
                wall_time_ms: 11,
            }],
        })
        .await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(&tail_checks, tail.state(), &tail_image);

    let workflow_checks = Arc::new(AtomicUsize::new(0));
    let (mut workflow, workflow_image) = activate_with_post_publish_fence(
        &directory.path().join("workflow.sqlite"),
        workflow_bundle(),
        Arc::clone(&workflow_checks),
    )
    .await;
    assert!(matches!(
        workflow
            .dispatch_workflow(WorkflowEvent {
                instance: "instance-1".into(),
                payload: serde_json::json!({ "value": 5 }),
            })
            .await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(&workflow_checks, workflow.state(), &workflow_image);
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_events_reject_completion_when_ownership_fails_after_publication() {
    let directory = tempfile::tempdir().unwrap();

    let websocket_message_checks = Arc::new(AtomicUsize::new(0));
    let (mut websocket_message, websocket_message_image) = activate_with_post_publish_fence(
        &directory.path().join("websocket-message.sqlite"),
        websocket_bundle(),
        Arc::clone(&websocket_message_checks),
    )
    .await;
    assert!(matches!(
        websocket_message
            .dispatch_websocket_message(WebSocketMessageEvent {
                id: "socket-1".into(),
                message: "A".into(),
            })
            .await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(
        &websocket_message_checks,
        websocket_message.state(),
        &websocket_message_image,
    );

    let websocket_close_checks = Arc::new(AtomicUsize::new(0));
    let (mut websocket_close, websocket_close_image) = activate_with_post_publish_fence(
        &directory.path().join("websocket-close.sqlite"),
        websocket_bundle(),
        Arc::clone(&websocket_close_checks),
    )
    .await;
    assert!(matches!(
        websocket_close
            .dispatch_websocket_close(WebSocketCloseEvent {
                id: "socket-1".into(),
                code: 1007,
                reason: "done".into(),
                was_clean: true,
            })
            .await,
        Err(CellError::Lease(_))
    ));
    assert_post_publish_fence(
        &websocket_close_checks,
        websocket_close.state(),
        &websocket_close_image,
    );
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
