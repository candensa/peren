use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn exposes_durable_storage_to_worker_code() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default { async fetch() {
              const before = await Peren.storage.get('counter');
              await Peren.storage.transaction(async (storage) => {
                const value = await storage.get('counter');
                const next = value === undefined ? 1 : value[0] + 1;
                await storage.put('counter', new Uint8Array([next]));
              });
              const after = await Peren.storage.get('counter');
              return new Response(JSON.stringify({ before: before?.[0] ?? 0, after: after[0] }));
            } };",
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
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, br#"{"before":0,"after":1}"#);
    assert_eq!(runtime.committed_revision(), StorageRevision::new(7));
}

#[tokio::test(flavor = "current_thread")]
async fn storage_delete_all_deletes_scope_and_rolls_back_on_failure() {
    let storage = Arc::new(SqlHost::new());
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch() {
              await Peren.storage.transaction(async (storage) => {
                await storage.put('a', 'one');
                await storage.put('b', 'two');
                await storage.put('keep', 'other', { scope: 'kv' });
              });
              let failed = false;
              try {
                await Peren.storage.transaction(async (storage) => {
                  await storage.deleteAll();
                  throw new Error('rollback');
                });
              } catch (_error) {
                failed = true;
              }
              const afterRollback = await Peren.storage.get('a');
              const deleted = await Peren.storage.deleteAll();
              const missing = await Peren.storage.get('a');
              const other = await Peren.storage.get('keep', { scope: 'kv' });
              return new Response(`${failed}:${new TextDecoder().decode(afterRollback)}:${deleted}:${missing === undefined}:${new TextDecoder().decode(other)}`);
            } };",
        ),
        limits(),
        WorkerEnvironment::empty(),
        storage,
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/storage".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(
        String::from_utf8(response.body).unwrap(),
        "true:one:2:true:other"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn storage_mutation_replays_recorded_outcome_without_rerunning_callback() {
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async fetch() {
                const first = await Peren.storage.mutation('11111111-1111-4111-8111-111111111111', async (storage) => {
                  const current = await storage.get('counter');
                  const next = (current?.[0] ?? 0) + 1;
                  await storage.put('counter', new Uint8Array([next]));
                  return { next };
                });
                const second = await Peren.storage.mutation('11111111-1111-4111-8111-111111111111', async (storage) => {
                  await storage.put('counter', new Uint8Array([99]));
                  return { next: 99 };
                });
                const stored = await Peren.storage.get('counter');
                return Response.json({ first, second, stored: stored[0] });
              }
            };",
        ),
        limits(),
        Arc::new(SqlHost::new()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/mutation".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "first": { "next": 1 },
            "second": { "next": 1 },
            "stored": 1,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rolls_back_failed_storage_transactions() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default { async fetch(request) {
              const path = new URL(request.url).pathname;
              if (path === '/fail') {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('counter', new Uint8Array([9]));
                  throw new Error('abort transaction');
                });
              }
              const value = await Peren.storage.get('counter');
              return new Response(String(value === undefined ? 0 : `${value[0]}:${value[1]}`));
            } };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    assert!(matches!(
        runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "https://worker.invalid/fail".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                InvocationLimits::new(1024, 10),
            )
            .await,
        Err(EngineError::JavaScript(_))
    ));
    assert_eq!(runtime.committed_revision(), StorageRevision::default());

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/read".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    assert_eq!(response.body, b"0");
}

#[tokio::test(flavor = "current_thread")]
async fn dispatches_alarm_handlers_with_durable_storage() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async alarm() {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('alarm', new Uint8Array([3]));
                });
              },
              async fetch() {
                const value = await Peren.storage.get('alarm');
                return new Response(String(value?.[0] ?? 0));
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime.dispatch_alarm().await.unwrap();
    assert_eq!(runtime.committed_revision(), StorageRevision::new(7));

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/read".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    assert_eq!(response.body, b"3");
}

#[tokio::test(flavor = "current_thread")]
async fn missing_alarm_handler_is_a_noop() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle("export default { fetch() { return new Response('ok'); } };"),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime.dispatch_alarm().await.unwrap();

    assert_eq!(runtime.committed_revision(), StorageRevision::default());
}

#[tokio::test(flavor = "current_thread")]
async fn dispatches_scheduled_handlers_with_durable_storage() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async scheduled(event) {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('scheduled', new Uint8Array([event.scheduledTime / 1000, event.cron.length]));
                });
              },
              async fetch() {
                const value = await Peren.storage.get('scheduled');
                return new Response(String(value === undefined ? 0 : `${value[0]}:${value[1]}`));
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_scheduled(ScheduledEvent {
            scheduled_time_ms: 7_000,
            cron: "*/5 * * * *".into(),
        })
        .await
        .unwrap();
    assert_eq!(runtime.committed_revision(), StorageRevision::new(7));

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/read".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    assert_eq!(response.body, b"7:11");
}

#[tokio::test(flavor = "current_thread")]
async fn dispatches_queue_handlers_with_durable_storage() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async queue(event) {
                await Peren.storage.transaction(async (storage) => {
                  const message = event.messages[0];
                  await storage.put('queue', new Uint8Array([
                    message.body.charCodeAt(0),
                    message.attempts,
                    message.id.length,
                    message.timestamp / 1000000000000,
                  ]));
                });
              },
              async fetch() {
                const value = await Peren.storage.get('queue');
                return new Response(String(value === undefined ? 0 : `${value[0]}:${value[1]}:${value[2]}:${value[3]}`));
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics::default(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "message-1".into(),
                body: vec![9],
                attempts: 1,
                timestamp: 1_700_000_000_000,
            }],
        })
        .await
        .unwrap();
    assert_eq!(runtime.committed_revision(), StorageRevision::new(7));
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/read".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"9:1:9:1");
}

#[tokio::test(flavor = "current_thread")]
async fn dispatches_tail_handlers_with_durable_storage() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async tail(event) {
                await Peren.storage.transaction(async (storage) => {
                  const record = event.events[0];
                  await storage.put('tail', new Uint8Array([record.wallTimeMs, record.outcome.length, record.script.length]));
                });
              },
              async fetch() {
                const value = await Peren.storage.get('tail');
                return new Response(String(value === undefined ? 0 : `${value[0]}:${value[1]}:${value[2]}`));
              }
            };",
        ),
        limits(),
        host,
    )
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
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/read".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"11:2:6");
}

#[tokio::test(flavor = "current_thread")]
async fn missing_tail_handler_is_a_noop() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle("export default { async fetch() { return new Response('ok'); } };"),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_tail(TailEvent { events: Vec::new() })
        .await
        .unwrap();

    assert_eq!(runtime.committed_revision(), StorageRevision::default());
}

#[tokio::test(flavor = "current_thread")]
async fn queue_event_exposes_batch_metrics_and_bulk_settlement() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default { async queue(event) {
              if (event.metrics.ready !== 2 || event.metrics.oldestReadyTimestamp !== 41) throw new Error('missing metrics');
              event.ackAll();
            } };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let dispatch = runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics {
                ready: 2,
                delayed: 1,
                leased: 2,
                oldest_ready_timestamp: Some(41),
            },
            queue: "jobs".into(),
            messages: vec![
                QueueMessage {
                    id: "one".into(),
                    body: b"a".to_vec(),
                    attempts: 1,
                    timestamp: 41,
                },
                QueueMessage {
                    id: "two".into(),
                    body: b"b".to_vec(),
                    attempts: 1,
                    timestamp: 42,
                },
            ],
        })
        .await
        .unwrap();

    assert_eq!(dispatch.dispositions.len(), 2);
    assert!(
        dispatch
            .dispositions
            .iter()
            .all(|disposition| { disposition.outcome == QueueDispositionKind::Ack })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn queue_wait_until_drains_background_work_before_completion() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async queue(event, _env, ctx) {
                ctx.waitUntil(Peren.storage.transaction(async (storage) => {
                  await storage.put('queue-background', new Uint8Array([event.messages.length]));
                }));
              },
              async fetch() {
                const value = await Peren.storage.get('queue-background');
                return new Response(String(value?.[0] ?? 0));
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics::default(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "message-1".into(),
                body: vec![9],
                attempts: 1,
                timestamp: 1_700_000_000_000,
            }],
        })
        .await
        .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/read".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"1");
}

#[tokio::test(flavor = "current_thread")]
async fn missing_queue_handler_is_a_noop() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle("export default { async fetch() { return new Response('ok'); } };"),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics::default(),
            queue: "jobs".into(),
            messages: Vec::new(),
        })
        .await
        .unwrap();

    assert_eq!(runtime.committed_revision(), StorageRevision::default());
}

#[tokio::test(flavor = "current_thread")]
async fn missing_scheduled_handler_is_a_noop() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle("export default { fetch() { return new Response('ok'); } };"),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_scheduled(ScheduledEvent {
            scheduled_time_ms: 7_000,
            cron: "*/5 * * * *".into(),
        })
        .await
        .unwrap();

    assert_eq!(runtime.committed_revision(), StorageRevision::default());
}
