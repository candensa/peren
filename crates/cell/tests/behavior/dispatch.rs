use super::*;

#[tokio::test(flavor = "current_thread")]
async fn worker_write_is_published_before_response_returns() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let first_repository = repository(false);
    let replica = Arc::clone(&first_repository.image);

    let mut first = WorkerCell::activate(
        &first_path,
        lease(usize::MAX),
        first_repository,
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let written = first
        .dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    assert_eq!(written.body, b"1");

    restore_replica(&second_path, &replica);
    first.release().await.unwrap();

    let mut second = WorkerCell::activate(
        &second_path,
        lease(usize::MAX),
        repository(false),
        counter_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let restored = second
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(restored.body, b"1");
    second.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn alarm_write_is_published_before_completion() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let first_repository = repository(false);
    let replica = Arc::clone(&first_repository.image);

    let mut first = WorkerCell::activate(
        &first_path,
        lease(usize::MAX),
        first_repository,
        alarm_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    first.dispatch_alarm().await.unwrap();

    restore_replica(&second_path, &replica);
    first.release().await.unwrap();

    let mut second = WorkerCell::activate(
        &second_path,
        lease(usize::MAX),
        repository(false),
        alarm_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let restored = second
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(restored.body, b"1");
    second.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn tail_write_is_published_before_completion() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let first_repository = repository(false);
    let replica = Arc::clone(&first_repository.image);

    let mut first = WorkerCell::activate(
        &first_path,
        lease(usize::MAX),
        first_repository,
        tail_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    first
        .dispatch_tail(TailEvent {
            events: vec![peren_runtime::TailRecord {
                outcome: "ok".into(),
                script: "worker".into(),
                wall_time_ms: 11,
            }],
        })
        .await
        .unwrap();

    restore_replica(&second_path, &replica);
    first.release().await.unwrap();

    let mut second = WorkerCell::activate(
        &second_path,
        lease(usize::MAX),
        repository(false),
        tail_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let restored = second
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(restored.body, b"11");
    second.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn queue_write_is_published_before_completion() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let first_repository = repository(false);
    let replica = Arc::clone(&first_repository.image);

    let mut first = WorkerCell::activate(
        &first_path,
        lease(usize::MAX),
        first_repository,
        queue_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    first
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
        .await
        .unwrap();

    restore_replica(&second_path, &replica);
    first.release().await.unwrap();

    let mut second = WorkerCell::activate(
        &second_path,
        lease(usize::MAX),
        repository(false),
        queue_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let restored = second
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(restored.body, b"9");
    second.release().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn scheduled_write_is_published_before_completion() {
    let directory = tempfile::tempdir().unwrap();
    let first_path = directory.path().join("first.sqlite");
    let second_path = directory.path().join("second.sqlite");
    let first_repository = repository(false);
    let replica = Arc::clone(&first_repository.image);

    let mut first = WorkerCell::activate(
        &first_path,
        lease(usize::MAX),
        first_repository,
        scheduled_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    first
        .dispatch_scheduled(ScheduledEvent {
            scheduled_time_ms: 7_000,
            cron: "*/5 * * * *".into(),
        })
        .await
        .unwrap();

    restore_replica(&second_path, &replica);
    first.release().await.unwrap();

    let mut second = WorkerCell::activate(
        &second_path,
        lease(usize::MAX),
        repository(false),
        scheduled_bundle(),
        isolate(),
        WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let restored = second
        .dispatch_http(request("/read"), invocation())
        .await
        .unwrap();

    assert_eq!(restored.body, b"7");
    second.release().await.unwrap();
}
