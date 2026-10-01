use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn message_channel_clones_and_delivers_messages_asynchronously() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { async fetch() {
              const { port1, port2 } = new MessageChannel();
              const received = new Promise((resolve) => { port2.onmessage = (event) => resolve(event.data); });
              const payload = { label: 'before', nested: { count: 1 } };
              port1.postMessage(payload);
              payload.label = 'after';
              payload.nested.count = 2;
              const data = await received;
              return new Response(JSON.stringify({ data, distinct: data !== payload }));
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/message".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "data": { "label": "before", "nested": { "count": 1 } }, "distinct": true })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn message_channel_transfers_ports_and_detaches_sender_reference() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { async fetch() {
              const outer = new MessageChannel();
              const inner = new MessageChannel();
              const received = new Promise((resolve) => {
                outer.port2.onmessage = (event) => {
                  const moved = event.ports[0];
                  const reply = new Promise((done) => { inner.port2.onmessage = (replyEvent) => done(replyEvent.data); });
                  moved.postMessage('from moved port');
                  resolve(reply.then((value) => ({ data: event.data, ports: event.ports.length, reply: value })));
                };
              });
              outer.port1.postMessage('take this', [inner.port1]);
              inner.port1.postMessage('after detach');
              return new Response(JSON.stringify(await received));
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/message-transfer".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "data": "take this", "ports": 1, "reply": "from moved port" })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn message_channel_queues_until_started_and_closes_cleanly() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { async fetch() {
              const { port1, port2 } = new MessageChannel();
              port1.postMessage('a');
              port1.postMessage('b');
              await Promise.resolve();
              const received = [];
              const done = new Promise((resolve) => {
                port2.addEventListener('message', (event) => {
                  received.push(event.data);
                  if (received.length === 2) resolve();
                });
              });
              await done;
              port1.close();
              port1.postMessage('c');
              await Promise.resolve();
              return new Response(JSON.stringify(received));
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/message-queue".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!(["a", "b"])
    );
}
