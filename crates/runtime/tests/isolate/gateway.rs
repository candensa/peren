use super::support::*;

const PROVIDER_GATEWAY_WORKER: &str = "export default { async fetch(_request, env) {
    env.ANALYTICS.writeDataPoint({ blobs: ['deploy'], doubles: [1], indexes: ['prod'] });
    const limited = await env.LIMITER.limit({ key: 'deploy' });
    await env.VECTORS.upsert([{ id: 'doc', values: [1, 0], metadata: { title: 'Doc' } }]);
    const vector = await env.VECTORS.query([1, 0], { topK: 1, returnMetadata: true });
    const ai = await env.AI.run('embed', { text: 'hello' });
    return Response.json({
      shape: {
        ai: env.AI instanceof Ai,
        vector: env.VECTORS instanceof VectorizeIndex,
        limiter: env.LIMITER instanceof RateLimiter,
        analytics: env.ANALYTICS instanceof AnalyticsEngineDataset,
        hyperdrive: env.DB instanceof Hyperdrive,
      },
      globals: {
        ai: typeof Ai,
        vector: typeof VectorizeIndex,
        limiter: typeof RateLimiter,
        analytics: typeof AnalyticsEngineDataset,
        hyperdrive: typeof Hyperdrive,
      },
      provider: {
        ai: env.AI.provider.kind,
        vector: env.VECTORS.provider.kind,
        hyperdrive: env.DB.provider.kind,
      },
      limited,
      vector,
      ai,
      connection: env.DB.connectionString,
    });
} };";

fn provider_gateway_environment() -> WorkerEnvironment {
    WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{
          "AI":{"type":"ai","route":{"kind":"local","command":"echo"}},
          "VECTORS":{"type":"vectorize","index":"docs","route":{"kind":"local","index":"docs"}},
          "LIMITER":{"type":"rate_limiter","limit":1,"periodSecs":60},
          "ANALYTICS":{"type":"analytics_engine","dataset":"events"},
          "DB":{"type":"hyperdrive","connectionString":"postgres://redacted:redacted@db.internal/app"}
        }"#
        .to_string(),
    )]))
}

async fn provider_gateway_runtime() -> WorkerRuntime {
    WorkerRuntime::load_with_capabilities(
        bundle(PROVIDER_GATEWAY_WORKER),
        limits(),
        provider_gateway_environment(),
        Capabilities {
            storage: Arc::new(SqlHost::new()),
            fetch: None,
            queue: None,
            r2: None,
            service: None,
            durable: None,
            cache: None,
            kv: None,
            ai: Some(Arc::new(AiEcho)),
        },
    )
    .await
    .unwrap()
}

fn assert_provider_gateway_shape(value: &serde_json::Value) {
    assert_eq!(
        value["shape"],
        serde_json::json!({
            "ai": true,
            "vector": true,
            "limiter": true,
            "analytics": true,
            "hyperdrive": true,
        })
    );
    assert_eq!(
        value["globals"],
        serde_json::json!({
            "ai": "function",
            "vector": "function",
            "limiter": "function",
            "analytics": "function",
            "hyperdrive": "function",
        })
    );
    assert_eq!(
        value["provider"],
        serde_json::json!({ "ai": "local", "vector": "local", "hyperdrive": "pgcat" })
    );
    assert_eq!(
        value["connection"],
        "postgres://redacted:redacted@db.internal/app"
    );
}

fn assert_provider_gateway_behavior(value: &serde_json::Value) {
    assert_eq!(value["limited"]["success"], true);
    assert_eq!(value["limited"]["limit"], 1);
    assert_eq!(value["limited"]["remaining"], 0);
    assert!(value["limited"]["reset"].as_i64().unwrap() > 0);
    assert_eq!(
        value["vector"],
        serde_json::json!({
            "matches": [{ "id": "doc", "score": 1, "metadata": { "title": "Doc" } }],
            "count": 1
        })
    );
    assert_eq!(
        value["ai"],
        serde_json::json!({
            "model": "embed",
            "input": { "text": "hello" },
            "options": {}
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_gateway_bindings_expose_native_constructor_shapes() {
    let mut runtime = provider_gateway_runtime().await;
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/provider-shapes".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(8192, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    let value = serde_json::from_slice::<serde_json::Value>(&response.body).unwrap();
    assert_provider_gateway_shape(&value);
    assert_provider_gateway_behavior(&value);
}
