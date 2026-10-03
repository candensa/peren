use super::support::*;

#[tokio::test]
async fn durable_object_stub_fetches_through_host_capability() {
    let bundle = bundle(
        r"export default { async fetch(_request, env) {
              const id = env.ROOMS.idFromName('lobby');
              const stub = env.ROOMS.get(id);
              const response = await stub.fetch('https://room.internal/message', { method: 'POST', body: 'hello' });
              return new Response(`${response.status}:${response.headers.get('x-durable-object')}:${await response.text()}`);
            } };",
    );
    let storage = Arc::new(Host::default());
    let durable = Arc::new(DurableHost {
        requests: Mutex::new(Vec::new()),
    });
    let env = WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{"ROOMS":{"type":"durable_object_namespace","className":"Room"}}"#.to_string(),
    )]));
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle,
        limits(),
        env,
        Capabilities {
            storage,
            fetch: None,
            queue: None,
            r2: None,
            service: None,
            durable: Some(durable.clone()),
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
                url: "https://worker.test/".into(),
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
        "209:Room:ROOMS:Room:POST:hello"
    );
    let requests = durable.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].namespace, "ROOMS");
    assert_eq!(requests[0].name.as_deref(), Some("lobby"));
    assert_eq!(requests[0].class_name, "Room");
    assert_eq!(requests[0].request.url, "https://room.internal/message");
}

#[tokio::test]
async fn durable_object_stub_fetch_carries_startup_props() {
    let bundle = bundle(
        r"export default { async fetch(_request, env) {
              const id = env.ROOMS.idFromName('lobby');
              const stub = env.ROOMS.get(id, { props: { region: 'eu', shard: 7 } });
              await stub.fetch('https://room.internal/message');
              return new Response('ok');
            } };",
    );
    let storage = Arc::new(Host::default());
    let durable = Arc::new(DurableHost {
        requests: Mutex::new(Vec::new()),
    });
    let env = WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{"ROOMS":{"type":"durable_object_namespace","className":"Room"}}"#.to_string(),
    )]));
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle,
        limits(),
        env,
        Capabilities {
            storage,
            fetch: None,
            queue: None,
            r2: None,
            service: None,
            durable: Some(durable.clone()),
            cache: None,
            kv: None,
            ai: None,
        },
    )
    .await
    .unwrap();

    runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.test/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    let requests = durable.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].props,
        serde_json::json!({ "region": "eu", "shard": 7 })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn durable_object_namespace_exposes_ids_and_stubs() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"ROOMS":{"type":"durable_object_namespace","className":"Room"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                const id = env.ROOMS.idFromName('lobby');
                const same = env.ROOMS.idFromString(id.toString());
                const stub = env.ROOMS.get(same);
                const unique = env.ROOMS.newUniqueId();
                return new Response(JSON.stringify({
                  global: typeof DurableObjectNamespace,
                  stable: id.toString() === same.toString(),
                  name: stub.name,
                  className: stub.className,
                  unique: unique.toString() !== id.toString(),
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
                url: "https://worker.invalid/do".into(),
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
            "global": "function",
            "stable": true,
            "name": "lobby",
            "className": "Room",
            "unique": true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn durable_object_state_exposes_id_props_exports_and_decoded_transactions() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default { async fetch() {
                const target = {
                  async add(left, right) { return left + right; }
                };
                const id = { name: 'room-a', toString() { return 'ROOMS:name:room-a'; } };
                const ctx = new DurableObjectState(Peren.storage, {}, {
                  id,
                  props: { region: 'eu' },
                  exports: target,
                });
                await ctx.storage.put('json', { count: 1 });
                await ctx.storage.transaction(async (storage) => {
                  const current = await storage.get('json');
                  await storage.put('json', { count: current.count + 1 });
                });
                const result = await ctx.storage.get('json');
                const sum = await ctx.exports.add(2, 3);
                return Response.json({
                  id: ctx.id.toString(),
                  name: ctx.id.name,
                  prop: ctx.props.region,
                  immutableProps: Object.isFrozen(ctx.props),
                  result,
                  sum,
                });
              } };",
        ),
        limits(),
        WorkerEnvironment::default(),
        Capabilities {
            storage: Arc::new(SqlHost::new()),
            fetch: None,
            queue: None,
            r2: None,
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
                url: "https://worker.invalid/do-state".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "id": "ROOMS:name:room-a",
            "name": "room-a",
            "prop": "eu",
            "immutableProps": true,
            "result": { "count": 2 },
            "sum": 5,
        })
    );
}

#[tokio::test]
async fn durable_object_facets_expose_scoped_rpc_storage_abort_and_delete() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "class Counter extends DurableObject {
                async increment(user) {
                    await this.ctx.storage.transaction(async (storage) => {
                        const current = await storage.get('count');
                        const next = current === undefined ? 1 : current[0] + 1;
                        await storage.put('count', new Uint8Array([next]));
                    });
                    const value = await this.ctx.storage.get('count');
                    return { user, count: value[0] };
                }
                async fetch(request) {
                    return new Response(new URL(request.url).pathname);
                }
            }
            export default { async fetch(_request) {
                const ctx = new DurableObjectState();
                const counter = ctx.facets.get('counter', () => Counter);
                const first = await counter.increment('ada');
                const second = await counter.increment('ada');
                const fetched = await counter.fetch('https://facet.internal/path');
                const aborted = ctx.facets.abort('counter');
                let refused = false;
                try { await counter.increment('ada'); } catch { refused = true; }
                const revived = ctx.facets.get('counter');
                const third = await revived.increment('ada');
                const deleted = await ctx.facets.delete('counter');
                const fresh = ctx.facets.get('counter', () => Counter);
                const afterDelete = await fresh.increment('ada');
                return Response.json({ first, second, third, afterDelete, fetched: await fetched.text(), aborted, refused, deleted });
            } };",
        ),
        limits(),
        WorkerEnvironment::default(),
        Capabilities {
            storage: Arc::new(SqlHost::new()),
            fetch: None,
            queue: None,
            r2: None,
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
                url: "https://worker.invalid/facet".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "first": { "user": "ada", "count": 1 },
            "second": { "user": "ada", "count": 2 },
            "third": { "user": "ada", "count": 3 },
            "afterDelete": { "user": "ada", "count": 1 },
            "fetched": "/path",
            "aborted": true,
            "refused": true,
            "deleted": true,
        })
    );
}
