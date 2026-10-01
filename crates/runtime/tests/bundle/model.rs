use super::support::*;

#[test]
fn digest_is_stable_across_insertion_order_and_sensitive_to_content() {
    let first = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([
            (name("main.js"), javascript("import './lib.js'")),
            (name("lib.js"), javascript("export const value = 1")),
        ]),
    )
    .unwrap();
    let same = WorkerBundle::new(
        name("main.js"),
        [
            (name("lib.js"), javascript("export const value = 1")),
            (name("main.js"), javascript("import './lib.js'")),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    let changed = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([
            (name("main.js"), javascript("import './lib.js'")),
            (name("lib.js"), javascript("export const value = 2")),
        ]),
    )
    .unwrap();

    assert_eq!(first.digest(), same.digest());
    assert_ne!(first.digest(), changed.digest());
}

#[test]
fn entry_must_be_present_javascript() {
    let missing = WorkerBundle::new(name("main.js"), BTreeMap::new()).unwrap_err();
    assert!(matches!(missing, BundleError::MissingEntry(_)));

    let wasm = Module::new(ModuleKind::Wasm, b"\0asm".as_slice()).unwrap();
    let invalid = WorkerBundle::new(
        name("main.wasm"),
        BTreeMap::from([(name("main.wasm"), wasm)]),
    )
    .unwrap_err();
    assert!(matches!(invalid, BundleError::WasmEntry(_)));
}

#[test]
fn module_names_cannot_escape_or_alias_the_bundle_namespace() {
    for invalid in [
        "",
        "/main.js",
        "./main.js",
        "../main.js",
        "a/../main.js",
        "a//b.js",
        "a\\b.js",
    ] {
        assert!(matches!(
            ModuleName::parse(invalid),
            Err(BundleError::ModuleName(_))
        ));
    }
}

#[test]
fn relative_imports_resolve_canonically_inside_the_bundle() {
    let bundle = WorkerBundle::new(
        name("src/main.js"),
        BTreeMap::from([
            (name("src/main.js"), javascript("")),
            (name("lib/value.js"), javascript("export default 1")),
        ]),
    )
    .unwrap();

    let (resolved, _) = bundle.resolve(bundle.entry(), "../lib/./value.js").unwrap();
    assert_eq!(resolved.as_ref(), "lib/value.js");
    assert!(matches!(
        bundle.resolve(bundle.entry(), "../../secret.js"),
        Err(BundleError::ImportEscape(_))
    ));
    assert!(bundle.resolve(bundle.entry(), "cloudflare:workers").is_ok());
    assert!(bundle.resolve(bundle.entry(), "node:fs").is_ok());
    assert!(matches!(
        bundle.resolve(bundle.entry(), "./missing.js"),
        Err(BundleError::MissingImport(_))
    ));
}

#[test]
fn built_in_imports_resolve_to_the_requested_module() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(name("main.js"), javascript(""))]),
    )
    .unwrap();

    let (workers, _) = bundle
        .resolve(bundle.entry(), "cloudflare:workers")
        .unwrap();
    let (assert, _) = bundle.resolve(bundle.entry(), "node:assert").unwrap();
    let (buffer, _) = bundle.resolve(bundle.entry(), "node:buffer").unwrap();
    let (console, _) = bundle.resolve(bundle.entry(), "node:console").unwrap();
    let (crypto, _) = bundle.resolve(bundle.entry(), "node:crypto").unwrap();
    let (events, _) = bundle.resolve(bundle.entry(), "node:events").unwrap();
    let (module, _) = bundle.resolve(bundle.entry(), "node:module").unwrap();
    let (fs, _) = bundle.resolve(bundle.entry(), "node:fs").unwrap();
    let (http, _) = bundle.resolve(bundle.entry(), "node:http").unwrap();
    let (os, _) = bundle.resolve(bundle.entry(), "node:os").unwrap();
    let (path, _) = bundle.resolve(bundle.entry(), "node:path").unwrap();
    let (perf, _) = bundle.resolve(bundle.entry(), "node:perf_hooks").unwrap();
    let (querystring, _) = bundle.resolve(bundle.entry(), "node:querystring").unwrap();
    let (decoder, _) = bundle
        .resolve(bundle.entry(), "node:string_decoder")
        .unwrap();
    let (stream, _) = bundle.resolve(bundle.entry(), "node:stream/web").unwrap();
    let (consumers, _) = bundle
        .resolve(bundle.entry(), "node:stream/consumers")
        .unwrap();
    let (timers, _) = bundle
        .resolve(bundle.entry(), "node:timers/promises")
        .unwrap();
    let (url, _) = bundle.resolve(bundle.entry(), "node:url").unwrap();
    let (util, _) = bundle.resolve(bundle.entry(), "node:util").unwrap();

    assert_eq!(workers.as_ref(), "cloudflare:workers");
    assert_eq!(assert.as_ref(), "node:assert");
    assert_eq!(buffer.as_ref(), "node:buffer");
    assert_eq!(console.as_ref(), "node:console");
    assert_eq!(crypto.as_ref(), "node:crypto");
    assert_eq!(events.as_ref(), "node:events");
    assert_eq!(module.as_ref(), "node:module");
    assert_eq!(fs.as_ref(), "node:fs");
    assert_eq!(http.as_ref(), "node:http");
    assert_eq!(os.as_ref(), "node:os");
    assert_eq!(path.as_ref(), "node:path");
    assert_eq!(perf.as_ref(), "node:perf_hooks");
    assert_eq!(querystring.as_ref(), "node:querystring");
    assert_eq!(decoder.as_ref(), "node:string_decoder");
    assert_eq!(stream.as_ref(), "node:stream/web");
    assert_eq!(consumers.as_ref(), "node:stream/consumers");
    assert_eq!(timers.as_ref(), "node:timers/promises");
    assert_eq!(url.as_ref(), "node:url");
    assert_eq!(util.as_ref(), "node:util");
}
