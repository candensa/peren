use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn workflow_activity_handler_returns_serialized_result() {
    let source = "
        export default {
          async fetch() { return new Response('ok'); },
          async activity(event, env, ctx) {
            ctx.waitUntil(Promise.resolve());
            return {
              instance: event.instance,
              name: event.name,
              task: event.task,
              input: event.payload.value,
              env: env.FLAG
            };
          }
        }
    ";
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(source),
        limits(),
        WorkerEnvironment::new(BTreeMap::from([("FLAG".to_string(), "ready".to_string())])),
        Arc::new(Host::default()),
    )
    .await
    .unwrap();

    let result = runtime
        .dispatch_workflow_activity(peren_runtime::WorkflowActivityEvent {
            instance: "order-1".into(),
            name: "ship".into(),
            task: "fulfillment.ship".into(),
            payload: serde_json::json!({ "value": 42 }),
        })
        .await
        .unwrap();

    assert_eq!(
        result,
        serde_json::json!({
            "instance": "order-1",
            "name": "ship",
            "task": "fulfillment.ship",
            "input": 42,
            "env": "ready"
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn workflow_binding_tracks_instance_state() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"FLOW":{"type":"workflow"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                const instance = await env.FLOW.create({ id: 'order-1' });
                const running = await instance.status();
                await instance.terminate('done');
                const terminated = await env.FLOW.get('order-1').status();
                await instance.restart();
                const restarted = await instance.status();
                return new Response(JSON.stringify({
                  global: typeof Workflow,
                  provider: env.FLOW.provider.kind,
                  id: instance.id,
                  running: running.status,
                  terminated: terminated.status,
                  reason: terminated.reason,
                  restarted: restarted.status,
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
                url: "https://worker.invalid/workflow".into(),
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
            "provider": "native",
            "id": "order-1",
            "running": "running",
            "terminated": "terminated",
            "reason": "done",
            "restarted": "running",
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn workflow_sleep_replays_persisted_timer_without_waiting_again() {
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default {
              async fetch() {
                const calls = await Peren.storage.get('calls');
                const due = await Peren.storage.get('due');
                return new Response(`${new TextDecoder().decode(calls)}:${new TextDecoder().decode(due)}`);
              },
              async workflow(event, step) {
                const timer = await step.sleep('pause', 0);
                const bytes = await Peren.storage.get('calls');
                const calls = bytes === undefined ? 0 : Number(new TextDecoder().decode(bytes));
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('calls', String(calls + 1));
                  await storage.put('due', String(timer.delayMs));
                });
              }
            };",
        ),
        limits(),
        WorkerEnvironment::empty(),
        Arc::new(SqlHost::new()),
    )
    .await
    .unwrap();

    let event = WorkflowEvent {
        instance: "sleep-1".into(),
        payload: serde_json::json!({}),
    };
    runtime.dispatch_workflow(event.clone()).await.unwrap();
    runtime.dispatch_workflow(event).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/workflow-sleep".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(String::from_utf8(response.body).unwrap(), "2:0");
}

#[tokio::test(flavor = "current_thread")]
async fn workflow_event_replays_journaled_steps_without_rerunning_callbacks() {
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default {
              async fetch() { const calls = await Peren.storage.get('calls'); const last = await Peren.storage.get('last'); return new Response(`${new TextDecoder().decode(calls)}:${new TextDecoder().decode(last)}`); },
              async workflow(event, step) {
                const value = await step.do('charge', async () => {
                  const bytes = await Peren.storage.get('calls');
                  const calls = bytes === undefined ? 0 : Number(new TextDecoder().decode(bytes));
                  await Peren.storage.transaction(async (storage) => {
                    await storage.put('calls', String(calls + 1));
                  });
                  return { charged: event.payload.amount };
                });
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('last', String(value.charged));
                });
              }
            };",
        ),
        limits(),
        WorkerEnvironment::empty(),
        Arc::new(SqlHost::new()),
    )
    .await
    .unwrap();

    let event = WorkflowEvent {
        instance: "order-1".into(),
        payload: serde_json::json!({ "amount": 42 }),
    };
    runtime.dispatch_workflow(event.clone()).await.unwrap();
    runtime.dispatch_workflow(event).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/workflow".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"1:42");
}
