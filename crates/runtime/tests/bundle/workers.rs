use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn cloudflare_workers_module_exports_thin_base_classes() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(name("main.js"), javascript(r#"
            import { DurableObject, DurableObjectState, WorkerEntrypoint, RpcTarget, WebSocketRequestResponsePair } from "cloudflare:workers";
            class Counter extends DurableObject {}
            class Admin extends WorkerEntrypoint {}
            export default {
              fetch() {
                const ctx = { waitUntil() {} };
                const env = { VALUE: "ok" };
                const counter = new Counter(ctx, env);
                const admin = new Admin(ctx, env);
                const rpc = new RpcTarget();
                const state = new DurableObjectState();
                const pair = new WebSocketPair();
                state.acceptWebSocket(pair[1], ["room:a", "tenant:acme"]);
                const auto = new WebSocketRequestResponsePair("ping", "pong");
                state.setWebSocketAutoResponse(auto);
                return new Response(JSON.stringify({
                  durableObjectIsFunction: typeof DurableObject === "function",
                  workerEntrypointIsFunction: typeof WorkerEntrypoint === "function",
                  durableObjectStateIsFunction: typeof DurableObjectState === "function",
                  websocketPairIsFunction: typeof WebSocketRequestResponsePair === "function",
                  rpcTargetIsFunction: typeof RpcTarget === "function",
                  distinctClasses: DurableObject !== WorkerEntrypoint && WorkerEntrypoint !== RpcTarget,
                  counterCtx: counter.ctx === ctx,
                  counterEnv: counter.env.VALUE,
                  adminCtx: admin.ctx === ctx,
                  adminEnv: admin.env.VALUE,
                  rpcInstance: rpc instanceof RpcTarget,
                  hibernatedSockets: state.getWebSockets("room:a").length,
                  autoResponse: state.getWebSocketAutoResponse().response,
                  autoTimestamp: typeof state.getWebSocketAutoResponseTimestamp(),
                  globalDurableObject: globalThis.DurableObject === DurableObject,
                  globalRpcTarget: globalThis.RpcTarget === RpcTarget,
                }));
              }
            };
        "#))]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/cloudflare".into(),
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
        serde_json::json!({
            "durableObjectIsFunction": true,
            "workerEntrypointIsFunction": true,
            "durableObjectStateIsFunction": true,
            "websocketPairIsFunction": true,
            "rpcTargetIsFunction": true,
            "distinctClasses": true,
            "counterCtx": true,
            "counterEnv": "ok",
            "adminCtx": true,
            "adminEnv": "ok",
            "rpcInstance": true,
            "hibernatedSockets": 1,
            "autoResponse": "pong",
            "autoTimestamp": "number",
            "globalDurableObject": true,
            "globalRpcTarget": true,
        })
    );
}
