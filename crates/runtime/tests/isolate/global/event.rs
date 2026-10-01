use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn add_event_listener_dispatches_worker_events() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "addEventListener('fetch', (event) => {
               event.respondWith(new Response('listener'));
             });
             addEventListener('scheduled', async (event) => {
               await Peren.storage.transaction(async (storage) => {
                 await storage.put('scheduled', new Uint8Array([event.scheduledTime / 1000, event.cron.length]));
               });
             });
             addEventListener('queue', async (event) => {
               await Peren.storage.transaction(async (storage) => {
                 const message = event.messages[0];
                 await storage.put('queue', new Uint8Array([message.body.charCodeAt(0), message.attempts]));
               });
             });
             addEventListener('tail', async (event) => {
               await Peren.storage.transaction(async (storage) => {
                 const record = event.events[0];
                 await storage.put('tail', new Uint8Array([record.wallTimeMs, record.outcome.length]));
               });
             });",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/listener".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    assert_eq!(response.body, b"listener");

    runtime
        .dispatch_scheduled(ScheduledEvent {
            scheduled_time_ms: 3_000,
            cron: "* * * * *".into(),
        })
        .await
        .unwrap();
    runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics::default(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "message-1".into(),
                timestamp: 1,
                body: vec![4],
                attempts: 2,
            }],
        })
        .await
        .unwrap();
    runtime
        .dispatch_tail(TailEvent {
            events: vec![TailRecord {
                outcome: "ok".into(),
                script: "worker".into(),
                wall_time_ms: 11,
            }],
        })
        .await
        .unwrap();

    assert_eq!(runtime.committed_revision(), StorageRevision::new(7));
}
