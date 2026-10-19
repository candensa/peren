use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn routes_outbound_fetch_through_host_capability() {
    let mut runtime = WorkerRuntime::load_with_hosts(
        bundle(
            "export default { async fetch() {
              const upstream = await fetch('https://api.invalid/items', {
                method: 'POST',
                headers: { 'content-type': 'text/plain' },
                body: 'abc',
              });
              return new Response(await upstream.text(), {
                status: upstream.status,
                headers: { 'x-upstream': upstream.headers.get('x-upstream') },
              });
            } };",
        ),
        limits(),
        Arc::new(Host::default()),
        Arc::new(FetchHost),
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

    assert_eq!(response.status, 202);
    assert_eq!(response.body, b"https://api.invalid/items:3");
    assert!(
        response
            .headers
            .contains(&("x-upstream".into(), "POST".into()))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn outbound_fetch_from_later_event_continuation_settles() {
    let mut runtime = WorkerRuntime::load_with_hosts(
        bundle(
            "let notify;
            let result = 'idle';
            export default { async fetch(request) {
              const url = new URL(request.url);
              if (url.pathname === '/start') {
                result = 'pending';
                new Promise((resolve) => { notify = resolve; })
                  .then(async () => {
                    const upstream = await fetch('https://api.invalid/items', {
                      method: 'POST',
                      body: 'abc',
                    });
                    result = `OK ${upstream.status}:${await upstream.text()}`;
                  }, (error) => {
                    result = `ERR ${error.message}`;
                  });
                return new Response('started');
              }
              if (url.pathname === '/notify') {
                notify();
                await Promise.resolve();
                await Promise.resolve();
                return new Response('notified');
              }
              return new Response(result);
            } };",
        ),
        limits(),
        Arc::new(Host::default()),
        Arc::new(FetchHost),
    )
    .await
    .unwrap();

    dispatch_path(&mut runtime, "/start").await;
    dispatch_path(&mut runtime, "/notify").await;

    let mut body = Vec::new();
    for _ in 0..5 {
        body = dispatch_path(&mut runtime, "/result").await;
        if body.starts_with(b"OK ") {
            break;
        }
    }

    assert_eq!(
        String::from_utf8(body).unwrap(),
        "OK 202:https://api.invalid/items:3"
    );
}

async fn dispatch_path(runtime: &mut WorkerRuntime, path: &str) -> Vec<u8> {
    runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: format!("https://worker.invalid{path}"),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap()
        .body
}

#[tokio::test(flavor = "current_thread")]
async fn outbound_fetch_carries_selected_mtls_binding_identity() {
    let fetch = Arc::new(MtlsFetchHost {
        requests: Mutex::new(Vec::new()),
    });
    let env = WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{"CLIENT_CERT":{"type":"mtls_certificate"}}"#.to_string(),
    )]));
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default { async fetch(_request, env) {
              const upstream = await Peren.fetch('https://api.invalid/secure', {
                method: 'POST',
                body: 'secret',
                cf: { mtlsCertificate: env.CLIENT_CERT },
              });
              return new Response(`${upstream.status}:${upstream.headers.get('x-mtls')}:${await upstream.text()}`);
            } };",
        ),
        limits(),
        env,
        Capabilities {
            storage: Arc::new(Host::default()),
            fetch: Some(fetch.clone()),
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
                url: "https://worker.invalid/".into(),
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
        "203:CLIENT_CERT:CLIENT_CERT"
    );
    let requests = fetch.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].mtls.as_deref(), Some("CLIENT_CERT"));
    assert_eq!(requests[0].body, b"secret");
}

#[tokio::test(flavor = "current_thread")]
async fn outbound_fetch_aborts_while_reading_streaming_body() {
    let fetch = Arc::new(MtlsFetchHost {
        requests: Mutex::new(Vec::new()),
    });
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default { async fetch() {
              const signal = AbortSignal.timeout(10);
              const body = new ReadableStream({
                start(controller) {
                  controller.enqueue(new Uint8Array([1]));
                },
                cancel(reason) {
                  globalThis.cancelled = reason.name;
                },
              });
              try {
                await fetch('https://api.invalid/hang', {
                  method: 'POST',
                  body,
                  duplex: 'half',
                  signal,
                });
                return new Response('resolved');
              } catch (error) {
                return Response.json({ name: error.name, cancelled: globalThis.cancelled });
              }
            } };",
        ),
        limits(),
        WorkerEnvironment::empty(),
        Capabilities {
            storage: Arc::new(Host::default()),
            fetch: Some(fetch.clone()),
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
                url: "https://worker.invalid/".into(),
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
        serde_json::json!({ "name": "TimeoutError", "cancelled": "TimeoutError" })
    );
    assert!(fetch.requests.lock().await.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_outbound_fetch_without_a_host_capability() {
    let mut runtime = WorkerRuntime::load(
        bundle("export default { async fetch() { await fetch('https://api.invalid/'); return new Response('unreachable'); } };"),
        limits(),
    )
    .await
    .unwrap();

    assert!(matches!(
        runtime
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
            .await,
        Err(EngineError::JavaScript(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn node_http_and_https_clients_route_through_host_gated_fetch() {
    let entry = ModuleName::parse("main.js").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(
                ModuleKind::JavaScript,
                br#"
                import http from "node:http";
                import https from "node:https";
                const read = (client, url) => new Promise((resolve, reject) => {
                  const request = client.request(url, { method: 'POST', headers: { 'content-type': 'text/plain' } }, (response) => {
                    const chunks = [];
                    response.on('data', (chunk) => chunks.push(chunk.toString()));
                    response.on('end', () => resolve(`${response.statusCode}:${response.headers['x-upstream']}:${chunks.join('')}`));
                  });
                  request.on('error', reject);
                  request.write('abc');
                  request.end();
                });
                export default { async fetch() {
                  const first = await read(http, 'https://api.invalid/items');
                  const second = await new Promise((resolve, reject) => {
                    https.get('https://api.invalid/next', (response) => {
                      const chunks = [];
                      response.on('data', (chunk) => chunks.push(chunk.toString()));
                      response.on('end', () => resolve(`${response.statusCode}:${chunks.join('')}`));
                    }).on('error', reject);
                  });
                  let serverRefused = false;
                  try { http.createServer(); } catch (error) { serverRefused = error.message.includes('Worker isolate'); }
                  return new Response(JSON.stringify({ first, second, serverRefused }));
                } };
                "#
                .as_slice(),
            )
            .unwrap(),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load_with_hosts(
        bundle,
        limits(),
        Arc::new(Host::default()),
        Arc::new(FetchHost),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/http".into(),
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
            "first": "202:POST:https://api.invalid/items:3",
            "second": "202:https://api.invalid/next:0",
            "serverRefused": true,
        })
    );
}
