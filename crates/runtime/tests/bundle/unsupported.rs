use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn unsupported_node_builtins_import_and_refuse_operations_explicitly() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import dns from "node:dns";
            import dnsp from "node:dns/promises";
            import fs from "node:fs";
            import fsp from "node:fs/promises";
            import net from "node:net";
            import test from "node:test";
            import tls from "node:tls";
            export default {
              fetch() {
                const modules = [dns, dnsp, fs, fsp, net, test, tls];
                const names = ['lookup', 'lookup', 'readFile', 'readFile', 'connect', 'test', 'connect'];
                const results = modules.map((mod, index) => {
                  const fn = mod[names[index]];
                  if (typeof fn !== 'function') return 'missing';
                  try { fn('/tmp/x'); return 'miss'; } catch (error) { return error.message.includes('Worker isolate'); }
                });
                return new Response(JSON.stringify(results));
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
                url: "https://worker.invalid/node-unsupported".into(),
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
        serde_json::json!([true, true, true, true, true, true, true])
    );
}
