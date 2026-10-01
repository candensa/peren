use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_process_and_timers_modules_cover_common_worker_compatibility() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import process, { env, nextTick, hrtime } from "node:process";
            import { setImmediate, clearImmediate } from "node:timers";
            import { setTimeout as sleep } from "node:timers/promises";
            export default {
              async fetch() {
                const order = [];
                await new Promise((resolve) => nextTick(() => { order.push("tick"); resolve(); }));
                const cancelled = setImmediate(() => order.push("cancelled"));
                clearImmediate(cancelled);
                await sleep(0, "slept");
                const started = hrtime();
                return new Response(JSON.stringify({
                  envIsObject: typeof env === "object",
                  defaultMatchesNamed: process.env === env,
                  platform: process.platform,
                  order,
                  hrtimeShape: Array.isArray(started) && started.length === 2,
                }));
              }
            };
        "#,
            ),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/node".into(),
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
            "envIsObject": true,
            "defaultMatchesNamed": true,
            "platform": "linux",
            "order": ["tick"],
            "hrtimeShape": true,
        })
    );
}
