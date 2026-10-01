use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn local_ai_binding_runs_through_host_capability() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default {
              async fetch(_request, env) {
                const result = await env.AI.run('embed', { text: 'hello' }, { temperature: 0 });
                return new Response(`${result.model}:${result.input.text}:${result.options.temperature}:${typeof Ai}`);
              }
            };",
        ),
        limits(),
        WorkerEnvironment::new(BTreeMap::from([(
            "__perenBindings".to_string(),
            r#"{"AI":{"type":"ai","endpoint":"local","route":{"kind":"local","command":"echo"}}}"#
                .to_string(),
        )])),
        Capabilities {
            storage: Arc::new(Host::default()),
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
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/ai".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"embed:hello:0:function");
}

#[tokio::test(flavor = "current_thread")]
async fn ai_binding_exposes_native_helpers_through_host_capability() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default {
              async fetch(_request, env) {
                const embedded = await env.AI.embed('text-embedding-3-small', 'hello');
                const generated = await env.AI.generateText('gpt-4.1-mini', 'Write a title', { temperature: 0.2 });
                const chat = await env.AI.chat('claude-3-5-sonnet', [{ role: 'user', content: 'hello' }]);
                return Response.json({
                  shape: [typeof env.AI.run, typeof env.AI.embed, typeof env.AI.generateText, typeof env.AI.chat],
                  embedded,
                  generated,
                  chat,
                });
              }
            };",
        ),
        limits(),
        WorkerEnvironment::new(BTreeMap::from([(
            "__perenBindings".to_string(),
            r#"{"AI":{"type":"ai","endpoint":"local","route":{"kind":"local","command":"echo"}}}"#
                .to_string(),
        )])),
        Capabilities {
            storage: Arc::new(Host::default()),
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
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/ai".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(8192, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "shape": ["function", "function", "function", "function"],
            "embedded": {
                "model": "text-embedding-3-small",
                "input": { "text": "hello" },
                "options": {},
            },
            "generated": {
                "model": "gpt-4.1-mini",
                "input": { "prompt": "Write a title" },
                "options": { "temperature": 0.2 },
            },
            "chat": {
                "model": "claude-3-5-sonnet",
                "input": { "messages": [{ "role": "user", "content": "hello" }] },
                "options": {},
            },
        })
    );
}
