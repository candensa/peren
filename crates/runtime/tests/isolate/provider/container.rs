use super::super::support::*;

struct ContainerFetchHost;

#[async_trait::async_trait]
impl OutboundFetchHost for ContainerFetchHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        let body = format!("{}:{}", request.url, request.body.len()).into_bytes();
        Ok(HttpResponse {
            status: 202,
            headers: Vec::new(),
            body,
            upgrade: false,
            websocket_id: None,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn container_binding_exposes_fetchable_local_port_bridge() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default {
              async fetch(_request, env) {
                const named = env.APP.get('build');
                const proxied = await named.fetch('https://container.invalid/app?x=1', { method: 'POST', body: 'abc' });
                let lifecycle;
                try {
                  await named.exec(['echo', 'ok'], { cwd: '/workspace' });
                } catch (error) {
                  lifecycle = String(error.message);
                }
                return Response.json({
                  shape: [typeof Container, env.APP.experimental, named.id, typeof named.status, typeof named.destroy],
                  proxied: await proxied.text(),
                  lifecycle,
                });
              }
            };",
        ),
        limits(),
        WorkerEnvironment::new(BTreeMap::from([(
            "__perenBindings".to_string(),
            r#"{"APP":{"type":"container","image":"demo:latest","port":8080,"memoryMb":256,"cpuMillis":250,"idleSleepSecs":30,"allowNetworkEgress":false}}"#.to_string(),
        )])),
        Capabilities {
            storage: Arc::new(Host::default()),
            fetch: Some(Arc::new(ContainerFetchHost)),
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
        body.get("shape"),
        Some(&serde_json::json!([
            "function", true, "build", "function", "function"
        ]))
    );
    assert_eq!(
        body.get("proxied"),
        Some(&serde_json::json!("http://127.0.0.1:8080/app?x=1:3"))
    );
    assert!(
        body.get("lifecycle")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| message.contains("requires a managed sandbox provider"))
    );
}
