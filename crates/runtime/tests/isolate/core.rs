use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn evaluates_once_and_dispatches_repeatedly() {
    let mut runtime = WorkerRuntime::load(bundle(
        "let count = 0; export default { async fetch(request) { count += request.by; return { count }; } };",
    ), limits())
    .await
    .unwrap();

    assert_eq!(
        runtime
            .dispatch(serde_json::json!({ "by": 2 }))
            .await
            .unwrap(),
        serde_json::json!({ "count": 2 })
    );
    assert_eq!(
        runtime
            .dispatch(serde_json::json!({ "by": 3 }))
            .await
            .unwrap(),
        serde_json::json!({ "count": 5 })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn passes_environment_to_fetch_and_queue_handlers() {
    let mut values = BTreeMap::new();
    values.insert("GREETING".to_string(), "hello".to_string());
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default {
              async fetch(_request, env) { return new Response(env.GREETING); },
              async queue(_event, env) { if (env.GREETING !== 'hello') throw new Error('missing env'); }
            };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(Host::default()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://example.com/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    runtime
        .dispatch_queue(QueueEvent {
            metrics: QueueMetrics::default(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "one".into(),
                timestamp: 1,
                body: b"{}".to_vec(),
                attempts: 1,
            }],
        })
        .await
        .unwrap();

    assert_eq!(response.body, b"hello");
}

#[tokio::test(flavor = "current_thread")]
async fn r2_binding_reads_writes_lists_and_deletes_objects() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"FILES":{"type":"r2","bucket":"uploads","prefix":"tenant/"}}"#.to_string(),
    );
    let r2 = Arc::new(R2Host::default());
    let mut runtime = WorkerRuntime::load_with_r2(
        bundle(
            "export default { async fetch(_request, env) {
                await env.FILES.put('a.txt', 'hello', { httpMetadata: { contentType: 'text/plain' }, customMetadata: { owner: 'api' } });
                await env.FILES.put('b.txt', new Uint8Array([2]));
                const object = await env.FILES.get('a.txt');
                const listed = await env.FILES.list({ limit: 1 });
                await env.FILES.delete(['a.txt', 'b.txt']);
                const missing = await env.FILES.get('a.txt');
                const missingB = await env.FILES.get('b.txt');
                return new Response(JSON.stringify({ text: await object.text(), type: object.httpMetadata.contentType, owner: object.customMetadata.owner, listed, missing, missingB, compareAndSet: typeof env.FILES.compareAndSet }));
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(Host::default()),
        r2,
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://example.com/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        response.body,
        br#"{"text":"hello","type":"text/plain","owner":"api","listed":{"objects":[{"key":"tenant/a.txt","size":5,"customMetadata":{"owner":"api"}}],"cursor":"tenant/a.txt","truncated":true,"delimitedPrefixes":[]},"missing":null,"missingB":null,"compareAndSet":"undefined"}"#
    );
}

#[tokio::test(flavor = "current_thread")]
async fn service_binding_fetches_through_host_capability() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"AUTH":{"type":"service","service":"auth-worker"}}"#.to_string(),
    );
    let service = Arc::new(ServiceHost::default());
    let mut runtime = WorkerRuntime::load_with_service(
        bundle(
            "export default { async fetch(_request, env) {
                const response = await env.AUTH.fetch('https://auth.invalid/session', { method: 'POST', body: 'login' });
                return new Response(`${response.status}:${response.headers.get('x-service')}:${await response.text()}`);
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(Host::default()),
        service.clone(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/service".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"207:auth:auth-worker:POST:login");
    let requests = service.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].service, "auth-worker");
}

#[tokio::test(flavor = "current_thread")]
async fn mtls_certificate_binding_is_opaque_to_worker_code() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"CLIENT_CERT":{"type":"mtls_certificate"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                return new Response(JSON.stringify({
                  marker: env.CLIENT_CERT.__perenMtlsBinding,
                  keys: Object.keys(env.CLIENT_CERT),
                  frozen: Object.isFrozen(env.CLIENT_CERT),
                  cert: env.CLIENT_CERT.cert,
                  key: env.CLIENT_CERT.key,
                }));
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(Host::default()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/mtls".into(),
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
            "marker": "CLIENT_CERT",
            "keys": ["__perenMtlsBinding"],
            "frozen": true,
        })
    );
}
