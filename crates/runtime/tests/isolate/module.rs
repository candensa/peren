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
