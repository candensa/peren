use std::{collections::BTreeMap, path::PathBuf, time::Duration};

#[path = "support/config.rs"]
mod config;
#[path = "support/env.rs"]
mod env;

use env::DataEnv;
use peren_config::{Assets, Binding, QueueConsumer, QueueDefaults, RunWorkerFirst};
use peren_node::{Process, ProcessError, TailLevel, TailLogEvent, TailRead, read_tail};
use peren_testkit::{
    eventually::eventually,
    http::{get, head as request_head, request, websocket_open_then_close, websocket_text},
    metrics as metric,
    worker::TestWorker,
};
use tokio::net::TcpListener;

struct AssetsDir {
    root: tempfile::TempDir,
}

impl AssetsDir {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("index.html"), "home").unwrap();
        std::fs::write(root.path().join("app.js"), "console.log('peren');").unwrap();
        Self { root }
    }

    fn path(&self) -> PathBuf {
        self.root.path().to_path_buf()
    }
}

#[tokio::test]
async fn public_listener_dispatches_to_worker_runtime() {
    let worker = TestWorker::from_source(
            "export default { async fetch(request) {
                await Peren.storage.transaction(async (storage) => {
                    const current = await storage.get('counter');
                    const next = current === undefined ? 1 : current[0] + 1;
                    await storage.put('counter', new Uint8Array([next]));
                });
                const value = await Peren.storage.get('counter');
                return new Response(String(value[0]), { status: 201, headers: { 'x-worker': new URL(request.url).pathname } });
            } };",
        );
    let environment = DataEnv::new();
    let process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();
    let address = process.listeners()["public"];

    let first = get(address, "/room").await;
    let second = get(address, "/room").await;
    let other = get(address, "/other").await;

    assert!(first.starts_with("HTTP/1.1 201"), "{first}");
    assert!(first.contains("x-worker: /room"), "{first}");
    assert!(first.ends_with('1'), "{first}");
    assert!(second.ends_with('2'), "{second}");
    assert!(other.ends_with('1'), "{other}");
    let tail = std::fs::read_to_string(environment.path().join("tail/events.jsonl")).unwrap();
    assert!(tail.contains(r#""service":"api""#), "{tail}");
    assert!(tail.contains(r#""path":"/room""#), "{tail}");
    assert!(tail.contains(r#""status":201"#), "{tail}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn service_can_opt_in_to_stable_node_id_env() {
    let worker = TestWorker::from_source(
        "export default { fetch(_request, env) { return new Response(env.PEREN_NODE_ID ?? 'missing'); } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].expose_node_id = true;
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/").await;

    assert!(
        response.ends_with("00000000-0000-0000-0000-000000000001"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_captures_worker_console_logs_with_severity() {
    let worker = TestWorker::from_source(
        "export default { async fetch() {
            console.debug('debug');
            console.log('log', 1);
            console.info('info', { ok: true });
            console.warn('warn');
            console.error(new Error('boom'));
            return new Response('failed', { status: 500 });
        } };",
    );
    let environment = DataEnv::new();
    let config = config::worker(worker.path());
    let tail_config = config::worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    let response = request(
        address,
        &format!(
            "GET /logs HTTP/1.1\r\nhost: localhost\r\ntraceparent: {traceparent}\r\nconnection: close\r\n\r\n"
        ),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 500"), "{response}");
    let all = read_all_tail(&tail_config, &environment);
    let console = console_events(&all);
    assert_console_levels(&console, &all);
    assert_request_context(&all, &console, traceparent);
    assert_warn_filter(&tail_config, &environment);
    process.shutdown().await.unwrap();
}

fn read_all_tail(
    config: &peren_config::ValidatedConfig,
    environment: &DataEnv,
) -> peren_node::TailReport {
    read_tail(
        config,
        environment,
        &TailRead {
            service: "api".into(),
            level: None,
        },
    )
    .unwrap()
}

fn console_events(report: &peren_node::TailReport) -> Vec<&peren_node::TailConsoleEvent> {
    report
        .events
        .iter()
        .filter_map(|event| match event {
            TailLogEvent::Console(event) => Some(event),
            TailLogEvent::Request(_) => None,
        })
        .collect()
}

fn assert_console_levels(console: &[&peren_node::TailConsoleEvent], all: &peren_node::TailReport) {
    assert_eq!(console.len(), 5, "{all:?}");
    assert!(console.iter().any(
        |event| event.level == peren_node::TailConsoleLevel::Debug && event.message == "debug"
    ));
    assert!(
        console
            .iter()
            .any(|event| event.level == peren_node::TailConsoleLevel::Info
                && event.message == "log 1")
    );
    assert!(
        console
            .iter()
            .any(|event| event.level == peren_node::TailConsoleLevel::Info
                && event.message == r#"info {"ok":true}"#)
    );
    assert!(
        console
            .iter()
            .any(|event| event.level == peren_node::TailConsoleLevel::Warn
                && event.message == "warn")
    );
    assert!(
        console
            .iter()
            .any(|event| event.level == peren_node::TailConsoleLevel::Error
                && event.message.contains("Error: boom"))
    );
}

fn assert_request_context(
    all: &peren_node::TailReport,
    console: &[&peren_node::TailConsoleEvent],
    traceparent: &str,
) {
    let request = all
        .events
        .iter()
        .find_map(|event| match event {
            TailLogEvent::Request(request) if request.status == 500 => Some(request),
            TailLogEvent::Request(_) | TailLogEvent::Console(_) => None,
        })
        .expect("request event is present");
    let request_id = request
        .request_id
        .as_deref()
        .expect("request id is present");
    let dispatch_id = request
        .dispatch_id
        .as_deref()
        .expect("dispatch id is present");
    assert!(!request_id.is_empty());
    assert!(!dispatch_id.is_empty());
    assert_eq!(request.traceparent.as_deref(), Some(traceparent));
    assert!(
        console
            .iter()
            .all(|event| event.request_id.as_deref() == Some(request_id))
    );
}

fn assert_warn_filter(config: &peren_config::ValidatedConfig, environment: &DataEnv) {
    let warnings = read_tail(
        config,
        environment,
        &TailRead {
            service: "api".into(),
            level: Some(TailLevel::Warn),
        },
    )
    .unwrap();
    assert!(warnings.events.iter().any(|event| matches!(event, TailLogEvent::Console(event) if event.level == peren_node::TailConsoleLevel::Warn)));
    assert!(warnings.events.iter().any(|event| matches!(event, TailLogEvent::Console(event) if event.level == peren_node::TailConsoleLevel::Error)));
    assert!(!warnings.events.iter().any(|event| matches!(event, TailLogEvent::Console(event) if event.level == peren_node::TailConsoleLevel::Info)));
    assert!(
        warnings
            .events
            .iter()
            .any(|event| matches!(event, TailLogEvent::Request(request) if request.status == 500))
    );
}

#[tokio::test]
async fn public_listener_replays_storage_mutation_outcome() {
    let worker = TestWorker::from_source(
            "export default { async fetch() {
                const first = await Peren.storage.mutation('22222222-2222-4222-8222-222222222222', async (storage) => {
                  const current = await storage.get('counter');
                  const next = (current?.[0] ?? 0) + 1;
                  await storage.put('counter', new Uint8Array([next]));
                  return { next };
                });
                const second = await Peren.storage.mutation('22222222-2222-4222-8222-222222222222', async (storage) => {
                  await storage.put('counter', new Uint8Array([99]));
                  return { next: 99 };
                });
                const stored = await Peren.storage.get('counter');
                return Response.json({ first, second, stored: stored[0] });
            } };",
        );
    let environment = DataEnv::new();
    let process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/mutation").await;

    assert!(
        response.ends_with(r#"{"first":{"next":1},"second":{"next":1},"stored":1}"#),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_persists_websocket_attachment_metadata() {
    let worker = TestWorker::from_source(
        "export default { async fetch() {
                const pair = new WebSocketPair();
                const socket = pair[1];
                await socket.serializeAttachment({ room: 'chat', cursor: 7 });
                const first = await socket.deserializeAttachment();
                await socket.deleteAttachment();
                const second = await socket.deserializeAttachment();
                return Response.json({ first, missing: second === undefined });
            } };",
    );
    let environment = DataEnv::new();
    let process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/attachment").await;

    assert!(
        response.ends_with(r#"{"first":{"room":"chat","cursor":7},"missing":true}"#),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_accepts_worker_websocket_upgrade_handshake() {
    let worker = TestWorker::from_source(
        "export default { async fetch() {
                const pair = new WebSocketPair();
                pair[1].accept();
                const response = new Response(null, { headers: { 'x-worker': 'socket' } });
                response.webSocket = pair[0];
                return response;
            } };",
    );
    let environment = DataEnv::new();
    let process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();
    let address = process.listeners()["public"];

    let response = request_head(
            address,
            "GET /chat HTTP/1.1\r\nhost: localhost\r\nconnection: keep-alive, Upgrade\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        )
        .await;

    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    assert!(response.contains("connection: Upgrade"), "{response}");
    assert!(response.contains("upgrade: websocket"), "{response}");
    assert!(
        response.contains("sec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="),
        "{response}"
    );
    assert!(response.contains("x-worker: socket"), "{response}");
    assert!(
        response.contains("x-peren-websocket-session: socket-"),
        "{response}"
    );

    let metrics = get(address, "/metrics").await;
    assert!(
        metrics.contains("peren_websocket_sessions_total 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains("peren_websocket_sessions_registered 1"),
        "{metrics}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_preserves_requested_websocket_subprotocol() {
    let worker = TestWorker::from_source(
        "export default {
            async fetch(request) {
                const pair = new WebSocketPair();
                pair[1].accept();
                const requested = request.headers.get('sec-websocket-protocol') ?? '';
                const selected = requested.split(',').map(value => value.trim()).find(value => value === 'peren.rpc');
                const response = new Response(null, { headers: selected ? { 'sec-websocket-protocol': selected } : {} });
                response.webSocket = pair[0];
                return response;
            }
        };",
    );
    let environment = DataEnv::new();
    let process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();
    let address = process.listeners()["public"];

    let response = request_head(
        address,
        "GET /rpc HTTP/1.1\r\nhost: localhost\r\nconnection: keep-alive, Upgrade\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-protocol: chat.v1, peren.rpc\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    assert!(
        response.contains("sec-websocket-protocol: peren.rpc"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_bridges_websocket_message_frames_to_worker() {
    let worker = TestWorker::from_source(
        "export default {
            async fetch() {
                const pair = new WebSocketPair();
                pair[1].accept();
                const response = new Response(null, { headers: { 'x-worker': 'socket' } });
                response.webSocket = pair[0];
                return response;
            },
            async webSocketMessage(socket, message) {
                console.warn('socket message', message);
                socket.send(`echo:${message}`);
            }
        };",
    );
    let environment = DataEnv::new();
    let config = config::worker(worker.path());
    let tail_config = config::worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let (head, frame) = websocket_text(address, "/chat", "hello").await;

    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert!(head.contains("x-worker: socket"), "{head}");
    assert_eq!(frame, "echo:hello");

    let tail = read_tail(
        &tail_config,
        &environment,
        &TailRead {
            service: "api".into(),
            level: Some(TailLevel::Warn),
        },
    )
    .unwrap();
    assert!(
        tail.events.iter().any(|event| matches!(
            event,
            TailLogEvent::Console(console)
                if console.event == "websocket_message"
                    && console.level == peren_node::TailConsoleLevel::Warn
                    && console.message == "socket message hello"
        )),
        "{tail:?}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_unregisters_websocket_session_after_close() {
    let worker = TestWorker::from_source(
        "export default {
            async fetch() {
                const pair = new WebSocketPair();
                pair[1].accept();
                const response = new Response(null);
                response.webSocket = pair[0];
                return response;
            }
        };",
    );
    let environment = DataEnv::new();
    let process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();
    let address = process.listeners()["public"];

    let head = websocket_open_then_close(address, "/chat").await;
    let metrics = eventually(
        "closed WebSocket session leaves the host registry",
        Duration::from_secs(2),
        Duration::from_millis(20),
        || get(address, "/metrics"),
        |metrics| metric::value(metrics, "peren_websocket_sessions_registered") == 0,
    )
    .await;

    assert!(head.starts_with("HTTP/1.1 101"), "{head}");
    assert_eq!(
        metric::value(&metrics, "peren_websocket_sessions_registered"),
        0
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_serves_configured_static_assets_before_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch() { return new Response('worker'); } };",
    );
    let assets = AssetsDir::new();
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].assets = Some(Assets {
        directory: assets.path(),
        run_worker_first: None,
    });
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let root = get(address, "/").await;
    let script = get(address, "/app.js").await;
    let missing = get(address, "/missing").await;
    let escaped = get(address, "/../secret").await;

    assert!(root.starts_with("HTTP/1.1 200"), "{root}");
    assert!(
        root.contains("content-type: text/html; charset=utf-8"),
        "{root}"
    );
    assert!(root.ends_with("home"), "{root}");
    assert!(
        script.contains("content-type: text/javascript; charset=utf-8"),
        "{script}"
    );
    assert!(script.ends_with("console.log('peren');"), "{script}");
    assert!(missing.ends_with("worker"), "{missing}");
    assert!(escaped.starts_with("HTTP/1.1 400"), "{escaped}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn run_worker_first_assets_route_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(request) { return new Response(new URL(request.url).pathname); } };",
    );
    let assets = AssetsDir::new();
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].assets = Some(Assets {
        directory: assets.path(),
        run_worker_first: Some(RunWorkerFirst::Patterns(vec!["/app*".into()])),
    });
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let home = get(address, "/").await;
    let app = get(address, "/app.js").await;

    assert!(home.ends_with("home"), "{home}");
    assert!(app.ends_with("/app.js"), "{app}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_service_environment_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                return new Response(`${env.PUBLIC}:${env.TOKEN}:${env.STORE_TOKEN}`);
            } };",
    );
    let environment = DataEnv::new()
        .with("TOKEN_ENV", "secret")
        .with("STORE_TOKEN_ENV", "stored");
    let mut config = config::worker(worker.path());
    config.raw.services[0]
        .vars
        .insert("PUBLIC".into(), "visible".into());
    config.raw.services[0]
        .secrets
        .insert("TOKEN".into(), "TOKEN_ENV".into());
    config
        .raw
        .secrets_store
        .insert("STORE_TOKEN".into(), "STORE_TOKEN_ENV".into());
    config.raw.services[0]
        .secrets_store_refs
        .insert("STORE_TOKEN".into(), "STORE_TOKEN".into());
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/env").await;

    assert!(response.ends_with("visible:secret:stored"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_resolves_worker_secrets_from_local_store() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                return new Response(`${env.TOKEN}:${env.STORE_TOKEN}`);
            } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0]
        .secrets
        .insert("TOKEN".into(), "TOKEN_ENV".into());
    config
        .raw
        .secrets_store
        .insert("STORE_TOKEN".into(), "STORE_TOKEN_ENV".into());
    config.raw.services[0]
        .secrets_store_refs
        .insert("STORE_TOKEN".into(), "STORE_TOKEN".into());

    peren_node::rotate_secret(
        &config,
        &environment,
        peren_node::SecretRotate {
            name: "TOKEN".into(),
            value: "local-worker-secret".into(),
        },
    )
    .unwrap();
    peren_node::rotate_secret(
        &config,
        &environment,
        peren_node::SecretRotate {
            name: "STORE_TOKEN".into(),
            value: "local-store-secret".into(),
        },
    )
    .unwrap();

    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/env").await;

    assert!(
        response.ends_with("local-worker-secret:local-store-secret"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_request_is_rejected_before_worker_dispatch() {
    let worker = TestWorker::from_source(
        "export default { async fetch() { return new Response('should-not-run'); } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.limits.request_body_bytes = 3;
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = request(
            address,
            "POST /room HTTP/1.1\r\nhost: localhost\r\ncontent-length: 4\r\nconnection: close\r\n\r\n1234",
        )
        .await;

    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    assert!(
        response.contains("request body exceeds the configured limit"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn process_bundle_loader_preserves_relative_module_paths() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("src");
    let lib = root.path().join("lib");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::create_dir_all(&lib).unwrap();
    let entry = source.join("main.js");
    let module = lib.join("value.js");
    std::fs::write(
        &entry,
        "import { value } from '../lib/value.js'; export default { fetch() { return new Response(value); } };",
    )
    .unwrap();
    std::fs::write(&module, "export const value = 'loaded-relative-module';").unwrap();
    let environment = DataEnv::new();
    let mut config = config::worker(&entry);
    config.raw.services[0]
        .additional_modules
        .insert(String::new(), module);
    let process = Process::start(config, &environment).await.unwrap();
    let response = get(process.listeners()["public"], "/room").await;

    assert!(response.ends_with("loaded-relative-module"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn process_bundle_loader_supports_explicit_commonjs_and_wasm_modules() {
    let root = tempfile::tempdir().unwrap();
    let entry = root.path().join("main.js");
    let common = root.path().join("value.cjs");
    let wasm = root.path().join("answer.wasm");
    let wasm_bytes = [
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
        0x03, 0x02, 0x01, 0x00, 0x07, 0x0a, 0x01, 0x06, 0x61, 0x6e, 0x73, 0x77, 0x65, 0x72, 0x00,
        0x00, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
    ];
    std::fs::write(
        &entry,
        "import compiled from './answer.wasm';
         import value from './value.cjs';
         const instance = new WebAssembly.Instance(compiled);
         export default { fetch() { return new Response(value.prefix + ':' + instance.exports.answer()); } };",
    )
    .unwrap();
    std::fs::write(&common, "exports.prefix = 'mixed';").unwrap();
    std::fs::write(&wasm, wasm_bytes).unwrap();
    let environment = DataEnv::new();
    let mut config = config::worker(&entry);
    config.raw.services[0]
        .additional_modules
        .insert("value.cjs".into(), common);
    config.raw.services[0]
        .additional_modules
        .insert("answer.wasm".into(), wasm);
    let process = Process::start(config, &environment).await.unwrap();
    let response = get(process.listeners()["public"], "/room").await;

    assert!(response.ends_with("mixed:42"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn binds_every_listener_before_reporting_ready() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut process = Process::start(config::worker(worker.path()), &environment)
        .await
        .unwrap();

    assert!(process.ready());
    assert_eq!(process.listeners().len(), 2);
    for address in process.listeners().values() {
        assert!(get(*address, "/healthz").await.starts_with("HTTP/1.1 200"));
        assert!(get(*address, "/readyz").await.starts_with("HTTP/1.1 200"));
    }
    process.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_refuses_top_level_work_when_node_is_saturated() {
    let worker = TestWorker::from_source(
        "export default { async fetch() {
                await new Promise(resolve => setTimeout(resolve, 400));
                return new Response('slow');
            } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.limits.isolates = 1;
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let first = tokio::spawn(async move { get(address, "/first").await });
    tokio::time::sleep(Duration::from_millis(75)).await;
    let second = get(address, "/second").await;
    let first = first.await.unwrap();
    let metrics = get(address, "/metrics").await;

    assert!(first.starts_with("HTTP/1.1 200"), "{first}");
    assert!(second.starts_with("HTTP/1.1 503"), "{second}");
    assert!(
        metrics.contains("peren_admission_refused_total 1"),
        "{metrics}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_worker_bundle_fails_before_binding_listeners() {
    let worker = std::env::temp_dir().join(format!("missing-worker-{}.js", uuid::Uuid::new_v4()));
    let mut config = config::worker(&worker);
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_address = peer.local_addr().unwrap();
    let public_address = public.local_addr().unwrap();
    drop(peer);
    drop(public);
    config.raw.node.listen = peer_address.to_string();
    config.peer_listen = peer_address;
    config.raw.sockets[0].listen = public_address.to_string();
    config.sockets.insert("public".into(), public_address);
    let environment = DataEnv::new();

    let result = Process::start(config, &environment).await;

    assert!(matches!(
        result,
        Err(ProcessError::BundleFile { path, .. }) if path == worker
    ));
    let rebound_peer = TcpListener::bind(peer_address).await.unwrap();
    let rebound_public = TcpListener::bind(public_address).await.unwrap();
    drop(rebound_peer);
    drop(rebound_public);
}

#[tokio::test]
async fn a_bind_failure_rolls_back_earlier_listeners() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_address = occupied.local_addr().unwrap();
    let mut config = config::worker(worker.path());
    config.sockets.insert("public".into(), occupied_address);
    let environment = DataEnv::new();

    let result = Process::start(config, &environment).await;

    assert!(matches!(result, Err(ProcessError::Listen(_))));
}

#[tokio::test]
async fn serves_an_inherited_listener_without_binding_its_configured_address() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let inherited = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = inherited.local_addr().unwrap();
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config::worker(worker.path());
    config
        .sockets
        .insert("public".into(), occupied.local_addr().unwrap());
    let environment = DataEnv::new();
    let mut process = Process::start_with_listeners(
        config,
        &environment,
        BTreeMap::from([("public".into(), inherited)]),
    )
    .await
    .unwrap();

    assert!(process.ready());
    assert_eq!(process.listeners()["public"], address);
    assert!(get(address, "/readyz").await.starts_with("HTTP/1.1 200"));
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_unknown_inherited_listeners() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let inherited = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let environment = DataEnv::new();
    let result = Process::start_with_listeners(
        config::worker(worker.path()),
        &environment,
        BTreeMap::from([("admin".into(), inherited)]),
    )
    .await;

    assert!(matches!(
        result,
        Err(ProcessError::UnknownInheritedListener(name)) if name == "admin"
    ));
}

#[tokio::test]
async fn public_listener_resolves_stable_store_secret_declaration() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                return new Response(env.STRIPE_SECRET_KEY);
            } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].secrets.insert(
        "STRIPE_SECRET_KEY".into(),
        peren_config::Secret::Ref(peren_config::SecretRef {
            source: peren_config::SecretSource::Store,
            name: "services/api/STRIPE_SECRET_KEY".into(),
        }),
    );

    peren_node::rotate_secret(
        &config,
        &environment,
        peren_node::SecretRotate {
            name: "services/api/STRIPE_SECRET_KEY".into(),
            value: "local-store-secret".into(),
        },
    )
    .unwrap();

    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/env").await;

    assert!(response.ends_with("local-store-secret"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn metrics_endpoint_reports_safe_process_state() {
    let worker = TestWorker::from_source(
        "export default {
            async fetch(request, env) {
                await Peren.storage.transaction(async (storage) => {
                    await storage.put('observed', new Uint8Array([1]));
                });
                if (new URL(request.url).pathname === '/enqueue') {
                    await env.JOBS.send('alpha');
                }
                return new Response('ok');
            },
            async queue(event) {
                await Peren.storage.transaction(async (storage) => {
                    await storage.put('queue-count', String(event.messages.length));
                });
            }
        };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "JOBS".into(),
        Binding::Queue {
            queue_name: "jobs".into(),
        },
    );
    config.raw.services[0].consumes_queues = vec![QueueConsumer::Name("jobs".into())];
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::Memory,
        nats_url: None,
        file_path: None,
        cell_path: None,
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: QueueDefaults::default(),
    });
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let served = get(address, "/observed").await;
    let enqueued = get(address, "/enqueue").await;
    let response = get(address, "/metrics").await;

    assert!(served.starts_with("HTTP/1.1 200"), "{served}");
    assert!(enqueued.starts_with("HTTP/1.1 200"), "{enqueued}");
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("content-type: text/plain; version=0.0.4; charset=utf-8"));
    assert!(response.contains("peren_ready{listener=\"public\"} 1"));
    assert!(response.contains("peren_listener_worker{listener=\"public\"} 1"));
    assert!(response.contains("peren_configured_services 1"));
    assert!(response.contains("peren_configured_queue_consumers 1"));
    assert_eq!(metric::value(&response, "peren_http_errors_total"), 0);
    assert!(
        metric::value(&response, "peren_admission_active") <= 1,
        "{response}"
    );
    assert_eq!(metric::value(&response, "peren_admission_refused_total"), 0);
    assert!(
        metric::value(&response, "peren_http_requests_total") >= 2,
        "{response}"
    );
    assert!(
        metric::value(&response, "peren_storage_commits_total") >= 2,
        "{response}"
    );
    assert!(
        metric::value(&response, "peren_queue_sends_total") >= 1,
        "{response}"
    );
    assert!(
        metric::value(&response, "peren_queue_ticks_total") >= 1,
        "{response}"
    );
    assert!(
        response.contains("peren_http_duration_ms_total"),
        "{response}"
    );
    assert!(
        response.contains("peren_storage_commit_duration_ms_total"),
        "{response}"
    );
    assert!(!response.contains("ca.pem"));
    assert!(!response.contains("key.pem"));

    process.shutdown().await.unwrap();
}
