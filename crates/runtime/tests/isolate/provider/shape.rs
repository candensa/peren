use super::super::support::*;

fn provider_shape_worker() -> &'static str {
    "export default { async fetch(_request, env) {
        await env.CACHE.put('name', 'ada');
        await env.DB.prepare('CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT)').run();
        await env.DB.prepare('INSERT INTO records(name) VALUES (?)').bind('ada').run();
        await env.JOBS.send({ id: 1 });
        await env.FILES.put('hello.txt', 'world', { httpMetadata: { contentType: 'text/plain' }, customMetadata: { owner: 'ada' } });
        await env.LIMITER.limit({ key: 'tenant' });
        env.ANALYTICS.writeDataPoint({ blobs: ['request'], doubles: [1], indexes: ['tenant'] });
        const object = await env.FILES.get('hello.txt');
        const row = await env.DB.prepare('SELECT name FROM records WHERE id = ?').bind(1).first();
        return Response.json({
          kvGlobal: typeof KvNamespace, d1Global: typeof D1Database, statementGlobal: typeof D1PreparedStatement,
          queueGlobal: typeof Queue, r2Global: typeof R2Bucket, kv: env.CACHE instanceof KvNamespace,
          d1: env.DB instanceof D1Database, statement: env.DB.prepare('SELECT 1') instanceof D1PreparedStatement,
          queue: env.JOBS instanceof Queue, r2: env.FILES instanceof R2Bucket,
          provider: { kv: env.CACHE.provider.kind, d1: env.DB.provider.kind, queue: env.JOBS.provider.kind, r2: env.FILES.provider.kind, limiter: env.LIMITER.provider.kind, analytics: env.ANALYTICS.provider.kind },
          value: await env.CACHE.get('name'), row, text: await object.text(),
          contentType: object.httpMetadata.contentType, owner: object.customMetadata.owner,
        });
    } };"
}

fn provider_shape_environment() -> WorkerEnvironment {
    WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{"CACHE":{"type":"kv","scope":"cache"},"DB":{"type":"d1"},"JOBS":{"type":"queue","queue":"jobs"},"FILES":{"type":"r2","bucket":"files"},"LIMITER":{"type":"rate_limiter","limit":2,"periodSecs":60,"provider":{"kind":"memory"}},"ANALYTICS":{"type":"analytics_engine","dataset":"requests","provider":{"kind":"buffer","dataset":"requests"}}}"#.to_string(),
    )]))
}

fn expected_provider_shape() -> serde_json::Value {
    serde_json::json!({
        "kvGlobal": "function", "d1Global": "function", "statementGlobal": "function",
        "queueGlobal": "function", "r2Global": "function", "kv": true, "d1": true,
        "statement": true, "queue": true, "r2": true,
        "provider": { "kv": "native", "d1": "native_sqlite", "queue": "memory", "r2": "memory", "limiter": "memory", "analytics": "buffer" },
        "value": "ada", "row": { "name": "ada" }, "text": "world",
        "contentType": "text/plain", "owner": "ada",
    })
}

#[tokio::test]
async fn provider_bindings_expose_native_constructor_shapes() {
    let r2 = Arc::new(R2Host::default());
    let queue = Arc::new(QueueHost::default());
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(provider_shape_worker()),
        limits(),
        provider_shape_environment(),
        Capabilities {
            storage: Arc::new(SqlHost::new()),
            fetch: None,
            queue: Some(queue.clone()),
            r2: Some(r2),
            service: None,
            durable: None,
            cache: None,
            kv: None,
            ai: None,
        },
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/providers".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(8192, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        expected_provider_shape()
    );
    assert_eq!(queue.messages.lock().await.len(), 1);
}
