use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn resolves_relative_modules_without_exposing_deno() {
    let entry = ModuleName::parse("src/main.js").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    b"import { value } from '../lib/value.js'; export default { fetch() { return { value, deno: typeof Deno }; } };".as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("lib/value.js").unwrap(),
                Module::new(ModuleKind::JavaScript, b"export const value = 7;".as_slice()).unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    assert_eq!(
        runtime.dispatch(serde_json::Value::Null).await.unwrap(),
        serde_json::json!({ "value": 7, "deno": "undefined" })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn loads_commonjs_entry_modules() {
    let entry = ModuleName::parse("main.cjs").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(
                ModuleKind::CommonJs,
                b"module.exports = { fetch() { return new Response('commonjs'); } };".as_slice(),
            )
            .unwrap(),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"commonjs");
}

#[tokio::test(flavor = "current_thread")]
async fn resolves_commonjs_relative_require() {
    let entry = ModuleName::parse("src/main.cjs").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(
                    ModuleKind::CommonJs,
                    b"const value = require('../lib/value.cjs'); module.exports = { fetch() { return new Response(String(value.answer)); } };".as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("lib/value.cjs").unwrap(),
                Module::new(ModuleKind::CommonJs, b"exports.answer = 42;".as_slice()).unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"42");
}

#[tokio::test(flavor = "current_thread")]
async fn loader_binding_imports_only_bundled_relative_modules() {
    let entry = ModuleName::parse("src/main.js").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    br"export default { async fetch(_request, env) {
                        const module = await env.LOADER.import('../lib/value.js');
                        const rejections = [];
                        for (const specifier of ['', 'https://example.com/mod.js', 'node:fs', 'npm:left-pad', 'jsr:@std/path']) {
                            try { await env.LOADER.import(specifier); rejections.push(false); }
                            catch (_) { rejections.push(true); }
                        }
                        return Response.json({ value: module.value, loader: env.LOADER instanceof Loader, rejections });
                    } };".as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("lib/value.js").unwrap(),
                Module::new(ModuleKind::JavaScript, b"export const value = 'dynamic';".as_slice()).unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut environment = BTreeMap::new();
    environment.insert(
        "__perenBindings".to_string(),
        r#"{"LOADER":{"type":"loader"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle,
        limits(),
        WorkerEnvironment::new(environment),
        Arc::new(Host::default()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/loader".into(),
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
            "value": "dynamic",
            "loader": true,
            "rejections": [true, true, true, true, true],
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn loader_binding_resolves_entrypoints_and_durable_object_classes() {
    let entry = ModuleName::parse("src/main.js").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    br"export default { async fetch(_request, env) {
                        const worker = await env.LOADER.get('../lib/worker.js');
                        const entrypoint = worker.getEntrypoint();
                        const named = worker.getEntrypoint('named');
                        const Counter = worker.getDurableObjectClass('Counter');
                        const DirectCounter = await env.LOADER.getDurableObjectClass('Counter', '../lib/worker.js');
                        const directEntrypoint = await env.LOADER.getEntrypoint('../lib/worker.js');
                        const state = new DurableObjectState();
                        const counter = state.facets.get('counter', () => Counter);
                        const direct = state.facets.get('direct', () => DirectCounter);
                        const loaded = await entrypoint.fetch();
                        const directLoaded = await directEntrypoint.fetch();
                        const counted = await counter.increment();
                        const directCounted = await direct.increment();
                        return Response.json({
                          loaded: await loaded.text(),
                          directLoaded: await directLoaded.text(),
                          named: named.label,
                          counted,
                          directCounted,
                        });
                    } };".as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("lib/worker.js").unwrap(),
                Module::new(
                    ModuleKind::JavaScript,
                    br"export class Counter {
                        constructor(ctx) { this.ctx = ctx; }
                        async increment() {
                          const current = await this.ctx.storage.get('count');
                          const next = current === undefined ? 1 : current[0] + 1;
                          await this.ctx.storage.put('count', new Uint8Array([next]));
                          return next;
                        }
                      }
                      export const named = { label: 'named-entrypoint' };
                      export default { fetch() { return new Response('loaded-entrypoint'); } };".as_slice(),
                )
                .unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut environment = BTreeMap::new();
    environment.insert(
        "__perenBindings".to_string(),
        r#"{"LOADER":{"type":"loader"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle,
        limits(),
        WorkerEnvironment::new(environment),
        Capabilities {
            storage: Arc::new(SqlHost::new()),
            fetch: None,
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
                url: "https://worker.invalid/loader".into(),
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
            "loaded": "loaded-entrypoint",
            "directLoaded": "loaded-entrypoint",
            "named": "named-entrypoint",
            "counted": 1,
            "directCounted": 1,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn loader_binding_reports_missing_or_invalid_exports() {
    let entry = ModuleName::parse("src/main.js").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    br"export default { async fetch(_request, env) {
                        const worker = await env.LOADER.get('../lib/worker.js');
                        const failures = [];
                        for (const run of [
                          () => worker.getEntrypoint('missing'),
                          () => worker.getDurableObjectClass('missing'),
                          () => worker.getDurableObjectClass('NotClass'),
                          () => worker.getDurableObjectClass('ArrowCounter'),
                        ]) {
                          try { run(); failures.push(false); }
                          catch (error) { failures.push(error instanceof TypeError); }
                        }
                        const lookupRejections = [];
                        for (const run of [
                          () => env.LOADER.get('lib/worker.js'),
                          () => env.LOADER.getEntrypoint('lib/worker.js'),
                          () => env.LOADER.getDurableObjectClass('ArrowCounter', 'lib/worker.js'),
                        ]) {
                          try { await run(); lookupRejections.push(false); }
                          catch (error) { lookupRejections.push(error instanceof TypeError); }
                        }
                        const imported = await env.LOADER.import('lib/worker.js');
                        return Response.json({ failures, lookupRejections, imported: imported.value });
                    } };"
                        .as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("lib/worker.js").unwrap(),
                Module::new(
                    ModuleKind::JavaScript,
                    b"export const NotClass = { value: 1 }; export const ArrowCounter = () => {}; export const value = 7; export default {};".as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("src/lib/worker.js").unwrap(),
                Module::new(ModuleKind::JavaScript, b"export const value = 9;".as_slice()).unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut environment = BTreeMap::new();
    environment.insert(
        "__perenBindings".to_string(),
        r#"{"LOADER":{"type":"loader"}}"#.to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle,
        limits(),
        WorkerEnvironment::new(environment),
        Arc::new(Host::default()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/loader-errors".into(),
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
            "failures": [true, true, true, true],
            "lookupRejections": [true, true, true],
            "imported": 9,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn javascript_wasm_example_executes_with_runtime_loader() {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("examples/javascript/wasm");
    let worker = std::fs::read(examples.join("worker.js")).unwrap();
    let wasm = std::fs::read(examples.join("answer.wasm")).unwrap();
    let entry = ModuleName::parse("worker.js").unwrap();
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(ModuleKind::JavaScript, worker.as_slice()).unwrap(),
            ),
            (
                ModuleName::parse("answer.wasm").unwrap(),
                Module::new(ModuleKind::Wasm, wasm.as_slice()).unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "answer": 42 })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn imports_wasm_as_a_compiled_uninstantiated_module() {
    let entry = ModuleName::parse("main.js").unwrap();
    let wasm = [
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
        0x03, 0x02, 0x01, 0x00, 0x07, 0x0a, 0x01, 0x06, 0x61, 0x6e, 0x73, 0x77, 0x65, 0x72, 0x00,
        0x00, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
    ];
    let bundle = WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([
            (
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    b"import compiled from './answer.wasm'; const instance = new WebAssembly.Instance(compiled); export default { fetch() { return { answer: instance.exports.answer() }; } };".as_slice(),
                )
                .unwrap(),
            ),
            (
                ModuleName::parse("answer.wasm").unwrap(),
                Module::new(ModuleKind::Wasm, wasm.as_slice()).unwrap(),
            ),
        ]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    assert_eq!(
        runtime.dispatch(serde_json::Value::Null).await.unwrap(),
        serde_json::json!({ "answer": 42 })
    );
}
