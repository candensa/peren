use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_module_exposes_builtin_metadata_and_limited_require() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import module, { builtinModules, createRequire, isBuiltin, syncBuiltinESMExports } from "node:module";
            export default {
              fetch() {
                const require = createRequire(import.meta.url);
                const buffer = require('node:buffer');
                const path = require('path');
                const fs = require('node:fs');
                let refused = false;
                try { require('./local.js'); } catch { refused = true; }
                let fsRefused = false;
                try { fs.readFile('/tmp/x'); } catch (error) { fsRefused = error.message.includes('Worker isolate'); }
                syncBuiltinESMExports();
                return new Response(JSON.stringify({
                  defaultMatches: module.builtinModules === builtinModules,
                  hasBuffer: builtinModules.includes('buffer'),
                  hasStreamWeb: builtinModules.includes('stream/web'),
                  hasStreamConsumers: builtinModules.includes('stream/consumers'),
                  hasFs: builtinModules.includes('fs'),
                  hasDnsPromises: builtinModules.includes('dns/promises'),
                  prefixed: isBuiltin('node:crypto'),
                  bare: isBuiltin('crypto'),
                  fsBuiltin: isBuiltin('fs') && isBuiltin('node:fs'),
                  absent: !isBuiltin('./local.js'),
                  buffer: typeof buffer.Buffer.from,
                  path: path.join('a', 'b'),
                  refused,
                  fsRefused,
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
                url: "https://worker.invalid/node-module".into(),
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
            "defaultMatches": true,
            "hasBuffer": true,
            "hasStreamWeb": true,
            "hasStreamConsumers": true,
            "hasFs": true,
            "hasDnsPromises": true,
            "prefixed": true,
            "bare": true,
            "fsBuiltin": true,
            "absent": true,
            "buffer": "function",
            "path": "a/b",
            "refused": true,
            "fsRefused": true,
        })
    );
}
