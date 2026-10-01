use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn broadcast_channel_delivers_messages_within_runtime() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          async fetch() {
            const received = [];
            const first = new BroadcastChannel('deployments');
            const second = new BroadcastChannel('deployments');
            const delivered = new Promise((resolve) => {
              second.onmessage = (event) => {
                received.push({ data: event.data, origin: event.origin, type: event.type });
                resolve();
              };
            });
            first.postMessage({ id: 7, status: 'ready' });
            await delivered;
            first.close();
            second.close();
            return Response.json({
              global: typeof BroadcastChannel,
              instance: first instanceof BroadcastChannel,
              name: first.name,
              received,
            });
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/broadcast".into(),
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
            "instance": true,
            "name": "deployments",
            "received": [{
                "data": { "id": 7, "status": "ready" },
                "origin": "http://127.0.0.1",
                "type": "message"
            }]
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn event_eventtarget_performance_and_websocket_globals_are_explicit() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          fetch() {
            const target = new EventTarget();
            const seen = [];
            const listener = (event) => {
              seen.push(`${event.type}:${event.bubbles}:${event.cancelable}`);
              event.preventDefault();
            };
            target.addEventListener('custom', listener);
            const event = new Event('custom', { bubbles: true, cancelable: true });
            const dispatched = target.dispatchEvent(event);
            target.removeEventListener('custom', listener);
            target.dispatchEvent(new Event('custom'));
            const pair = new WebSocketPair();
            return new Response(JSON.stringify({
              event: typeof Event,
              target: typeof EventTarget,
              seen,
              defaultPrevented: event.defaultPrevented,
              dispatched,
              removed: seen.length,
              performance: performance instanceof Performance && typeof Performance === 'function' && performance.now() >= 0,
              websocket: typeof WebSocket === 'function' && pair[0] instanceof WebSocket,
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/web-globals".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(2048, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "event": "function",
            "target": "function",
            "seen": ["custom:true:true"],
            "defaultPrevented": true,
            "dispatched": false,
            "removed": 1,
            "performance": true,
            "websocket": true,
        })
    );
}
