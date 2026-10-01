use super::super::support::*;

struct EventSourceHost {
    requests: Mutex<Vec<HttpRequest>>,
}

#[async_trait::async_trait]
impl OutboundFetchHost for EventSourceHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        self.requests.lock().await.push(request);
        Ok(HttpResponse {
            status: 200,
            headers: vec![
                ("content-type".into(), "text/event-stream".into()),
                ("cache-control".into(), "no-cache".into()),
            ],
            body: b": comment\nid: 9\ndata: hello\ndata: world\n\nevent: deploy\ndata: ready\n\n"
                .to_vec(),
            upgrade: false,
            websocket_id: None,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn eventsource_parses_host_gated_server_sent_events() {
    let fetch = Arc::new(EventSourceHost {
        requests: Mutex::new(Vec::new()),
    });
    let mut runtime = WorkerRuntime::load_with_hosts(
        bundle(
            r"
            export default {
              async fetch() {
                const seen = [];
                const source = new EventSource('https://events.invalid/feed');
                const done = new Promise((resolve, reject) => {
                  source.onopen = () => seen.push(`open:${source.readyState}`);
                  source.onmessage = (event) => seen.push(`message:${event.data}:${event.lastEventId}:${event.origin}`);
                  source.addEventListener('deploy', (event) => {
                    seen.push(`deploy:${event.data}:${event.type}`);
                    source.close();
                    resolve();
                  });
                  source.onerror = (event) => reject(event.error ?? new Error('eventsource failed'));
                });
                await done;
                return new Response(JSON.stringify({
                  constructor: typeof EventSource,
                  instance: source instanceof EventSource,
                  constants: [EventSource.CONNECTING, EventSource.OPEN, EventSource.CLOSED],
                  state: source.readyState,
                  credentials: source.withCredentials,
                  seen,
                }));
              }
            };
            ",
        ),
        limits(),
        Arc::new(Host::default()),
        fetch.clone(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/events".into(),
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
            "constructor": "function",
            "instance": true,
            "constants": [0, 1, 2],
            "state": 2,
            "credentials": false,
            "seen": [
                "open:1",
                "message:hello\nworld:9:https://events.invalid",
                "deploy:ready:deploy"
            ]
        })
    );
    let requests = fetch.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].url, "https://events.invalid/feed");
    assert!(
        requests[0]
            .headers
            .iter()
            .any(|(name, value)| name == "accept" && value == "text/event-stream")
    );
}
