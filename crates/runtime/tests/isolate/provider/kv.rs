use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn native_kv_binding_reads_writes_and_deletes_values() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"CACHE":{"type":"kv","scope":"cache"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                await env.CACHE.put('alpha', 'one');
                const first = await env.CACHE.get('alpha');
                await env.CACHE.delete('alpha');
                const second = await env.CACHE.get('alpha');
                return new Response(`${first}:${second}`);
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
                url: "https://example.com/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"one:null");
}

#[tokio::test(flavor = "current_thread")]
async fn native_kv_binding_compares_and_sets_versions_atomically() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"CACHE":{"type":"kv","scope":"cache"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                const create = await env.CACHE.compareAndSet('counter', { missing: true }, 'one');
                const stale = await env.CACHE.compareAndSet('counter', { version: create.version - 1 }, 'bad');
                const update = await env.CACHE.compareAndSet('counter', { version: create.version }, 'two', { metadata: { ok: true } });
                const record = await env.CACHE.getWithMetadata('counter');
                return Response.json({ create, stale, update, record });
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
                url: "https://example.com/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();
    let body = serde_json::from_slice::<serde_json::Value>(&response.body).unwrap();

    assert_eq!(
        body["create"],
        serde_json::json!({ "ok": true, "version": 1 })
    );
    assert_eq!(
        body["stale"],
        serde_json::json!({ "ok": false, "version": 1 })
    );
    assert_eq!(
        body["update"],
        serde_json::json!({ "ok": true, "version": 2 })
    );
    assert_eq!(body["record"]["value"], serde_json::json!("two"));
    assert_eq!(
        body["record"]["metadata"],
        serde_json::json!({ "ok": true })
    );
    assert_eq!(body["record"]["version"], serde_json::json!(2));
}

#[tokio::test(flavor = "current_thread")]
async fn native_kv_binding_preserves_metadata_and_expiration() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"CACHE":{"type":"kv","scope":"cache"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                await env.CACHE.put('profile', JSON.stringify({ name: 'ada' }), { metadata: { type: 'user' } });
                const first = await env.CACHE.getWithMetadata('profile', { type: 'json' });
                await env.CACHE.put('short', 'gone', { expirationTtl: -1, metadata: { stale: true } });
                const expired = await env.CACHE.getWithMetadata('short');
                await env.CACHE.delete('profile');
                const deleted = await env.CACHE.getWithMetadata('profile');
                return Response.json({ first, expired, deleted });
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
                url: "https://example.com/".into(),
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
            "first": { "value": { "name": "ada" }, "metadata": { "type": "user" }, "version": 1 },
            "expired": { "value": null, "metadata": null, "version": null },
            "deleted": { "value": null, "metadata": null, "version": null }
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn native_kv_binding_lists_bounded_materialized_pages() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"CACHE":{"type":"kv","scope":"cache"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                await env.CACHE.put('app/one', '1');
                await env.CACHE.put('app/two', '2');
                await env.CACHE.put('sys/three', '3');
                const first = await env.CACHE.list({ prefix: 'app/', limit: 1 });
                const second = await env.CACHE.list({ prefix: 'app/', cursor: first.cursor, limit: 1 });
                return new Response(JSON.stringify({ first, second }));
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(SqlHost::new()),
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
        br#"{"first":{"keys":[{"name":"app/one"}],"cursor":"app/one","list_complete":false},"second":{"keys":[{"name":"app/two"}],"list_complete":true}}"#
    );
}
