use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn unsupported_runtime_globals_are_not_exposed_to_worker_code() {
    let names = CAPABILITIES
        .iter()
        .filter(|capability| {
            capability.kind == CapabilityKind::Global
                && capability.status == CapabilityStatus::Unsupported
        })
        .map(|capability| capability.name)
        .collect::<Vec<_>>();
    let names = serde_json::to_string(&names).unwrap();
    let mut runtime = WorkerRuntime::load(
        bundle(&format!(
            "const names = {names};
             export default {{
               fetch() {{
                 const exposed = names.filter((name) => typeof globalThis[name] !== 'undefined');
                 return new Response(JSON.stringify(exposed));
               }}
             }};"
        )),
        limits(),
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
    let exposed = serde_json::from_slice::<Vec<String>>(&response.body).unwrap();

    assert!(
        exposed.is_empty(),
        "unsupported globals exposed: {exposed:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_an_oversized_body_before_worker_execution() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "let calls = 0; export default { fetch() { calls++; return new Response(String(calls)); } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let refused = runtime
        .dispatch_http(
            HttpRequest {
                method: "POST".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: vec![0; 11],
                mtls: None,
            },
            InvocationLimits::new(10, 10),
        )
        .await;
    assert!(matches!(
        refused,
        Err(EngineError::Limit(LimitError::RequestBytes {
            used: 11,
            limit: 10
        }))
    ));

    let accepted = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(10, 10),
        )
        .await
        .unwrap();
    assert_eq!(accepted.body, b"1");
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_an_oversized_response_body() {
    let mut runtime = WorkerRuntime::load(
        bundle("export default { fetch() { return new Response('too large'); } };"),
        limits(),
    )
    .await
    .unwrap();

    let refused = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(3, 10),
        )
        .await;

    assert!(matches!(
        refused,
        Err(EngineError::Limit(LimitError::ResponseBytes {
            used: 9,
            limit: 3
        }))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_an_oversized_streamed_response_body() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { fetch() {
              return new Response(new ReadableStream({
                start(controller) {
                  controller.enqueue(new TextEncoder().encode('ab'));
                  controller.enqueue(new TextEncoder().encode('cd'));
                  controller.close();
                }
              }));
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let refused = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/stream".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(3, 10),
        )
        .await;

    assert!(matches!(
        refused,
        Err(EngineError::Limit(LimitError::ResponseBytes {
            used: 4,
            limit: 3
        }))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn reports_heap_limit_when_worker_exhausts_memory() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { fetch() {
              const values = [];
              while (true) values.push(new Array(16_384).fill('peren-memory-pressure'));
            } };",
        ),
        IsolateLimits::new(8 * 1024 * 1024, Duration::from_secs(5)),
    )
    .await
    .unwrap();

    let result = runtime.dispatch(serde_json::Value::Null).await;

    assert!(matches!(result, Err(EngineError::HeapLimit)));
}

#[tokio::test(flavor = "current_thread")]
async fn accounts_invocation_cpu_time() {
    let mut runtime = WorkerRuntime::load(
        bundle("export default { fetch() { return new Response('too slow for zero budget'); } };"),
        limits(),
    )
    .await
    .unwrap();

    let result = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10).with_cpu_time(Duration::ZERO),
        )
        .await;

    assert!(matches!(
        result,
        Err(EngineError::Limit(LimitError::CpuTime { .. }))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn terminates_synchronous_worker_code_at_the_execution_deadline() {
    let mut runtime = WorkerRuntime::load(
        bundle("export default { fetch() { while (true) {} } };"),
        IsolateLimits::new(128 * 1024 * 1024, Duration::from_millis(30)),
    )
    .await
    .unwrap();

    let result = runtime.dispatch(serde_json::Value::Null).await;

    assert!(matches!(result, Err(EngineError::ExecutionTime)));
}

#[tokio::test(flavor = "current_thread")]
async fn applies_the_execution_deadline_during_module_activation() {
    let result = WorkerRuntime::load(
        bundle("while (true) {} export default { fetch() {} };"),
        IsolateLimits::new(128 * 1024 * 1024, Duration::from_millis(30)),
    )
    .await;

    assert!(matches!(result, Err(EngineError::ExecutionTime)));
}

#[tokio::test(flavor = "current_thread")]
async fn remains_usable_after_a_terminated_invocation() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { fetch(request) { if (request.loop) while (true) {} return { recovered: true }; } };",
        ),
        IsolateLimits::new(
            128 * 1024 * 1024,
            Duration::from_millis(30),
        ),
    )
    .await
    .unwrap();

    assert!(matches!(
        runtime.dispatch(serde_json::json!({ "loop": true })).await,
        Err(EngineError::ExecutionTime)
    ));
    assert_eq!(
        runtime
            .dispatch(serde_json::json!({ "loop": false }))
            .await
            .unwrap(),
        serde_json::json!({ "recovered": true })
    );
}
