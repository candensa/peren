use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn non_retryable_error_is_available_to_queue_handlers() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async queue() { throw new NonRetryableError('bad payload'); },
              async fetch() { return new Response(String(typeof NonRetryableError)); }
            };",
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
                url: "https://worker.invalid/nonretryable".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    assert_eq!(response.body, b"function");

    let error = runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics::default(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "message-1".into(),
                timestamp: 1,
                body: b"bad".to_vec(),
                attempts: 1,
            }],
        })
        .await
        .unwrap_err();
    assert!(error.is_non_retryable());
}

#[tokio::test(flavor = "current_thread")]
async fn worker_utility_globals_are_available_inside_dispatch() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { async fetch() {
              let fired = 0;
              await new Promise((resolve) => setImmediate(() => { fired += 1; resolve(); }));
              const cancelled = setImmediate(() => { fired += 10; });
              clearImmediate(cancelled);
              await new Promise((resolve) => setTimeout(resolve, 0));
              console.log('visible to workers');
              return new Response(`${fired}:${typeof navigator.userAgent}:${navigator.userAgent.includes('Peren')}`);
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/utility".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"1:string:true");
}
