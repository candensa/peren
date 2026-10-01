use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn queue_binding_sends_messages_through_host_capability() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"JOBS":{"type":"queue","queue":"jobs"}}"#.to_string(),
    );
    let queue = Arc::new(QueueHost::default());
    let mut runtime = WorkerRuntime::load_with_queue(
        bundle(
            "export default { async fetch(_request, env) {
                await env.JOBS.send('one', { contentType: 'text/plain', delaySeconds: 3 });
                await env.JOBS.sendBatch([{ body: { id: 2 } }, { body: new Uint8Array([3]) }]);
                return new Response('queued');
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(Host::default()),
        queue.clone(),
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

    assert_eq!(response.body, b"queued");
    let messages = queue.messages.lock().await.clone();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].queue, "jobs");
    assert_eq!(messages[0].body, b"one");
    assert_eq!(messages[0].content_type.as_deref(), Some("text/plain"));
    assert_eq!(messages[0].delay_seconds, Some(3));
    assert_eq!(messages[1].body, br#"{"id":2}"#);
    assert_eq!(messages[2].body, vec![3]);
}

#[tokio::test(flavor = "current_thread")]
async fn queue_binding_rejects_oversized_messages_and_batches_before_host_send() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"JOBS":{"type":"queue","queue":"jobs"}}"#.to_string(),
    );
    let queue = Arc::new(QueueHost::default());
    let mut runtime = WorkerRuntime::load_with_queue(
        bundle(
            "export default { async fetch(_request, env) {
                const results = [];
                for (const run of [
                  () => env.JOBS.send('x'.repeat(128001)),
                  () => env.JOBS.send('ok', { delaySeconds: 86401 }),
                  () => env.JOBS.sendBatch(Array.from({ length: 101 }, () => ({ body: 'x' }))),
                  () => env.JOBS.sendBatch([{ body: 'x'.repeat(128000) }, { body: 'x'.repeat(128000) }, { body: 'x' }]),
                ]) {
                  try { await run(); results.push(false); } catch (_) { results.push(true); }
                }
                return Response.json(results);
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        Arc::new(Host::default()),
        queue.clone(),
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
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!([true, true, true, true])
    );
    assert!(queue.messages.lock().await.is_empty());
}
