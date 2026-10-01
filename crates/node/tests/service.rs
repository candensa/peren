#[path = "support/env.rs"]
mod env;

use env::DataEnv;
use peren_config::FleetConfig;
use peren_node::Process;
use peren_testkit::{http::get, worker::TestWorker};

#[tokio::test]
async fn service_binding_routes_to_target_worker() {
    let api = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            const response = await env.AUTH.fetch('https://auth.internal/session', { method: 'POST', body: 'login' });
            return new Response(`${response.status}:${await response.text()}`);
        } };",
    );
    let auth = TestWorker::from_source(
        "export default { async fetch(request) {
            return new Response(`${new URL(request.url).pathname}:${request.method}:${await request.text()}`, { status: 207 });
        } };",
    );
    let environment = DataEnv::new();
    let config = FleetConfig::from_toml(&format!(
        r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.AUTH]
type = "service"
entrypoint = "auth"
[[services]]
name = "auth"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
"#,
        api.path().display(),
        auth.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/service").await;

    assert!(response.ends_with("207:/session:POST:login"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_object_stub_fetch_routes_to_object_service_cell() {
    let api = TestWorker::from_source(
        "export default { async fetch(request, env) {
            const name = new URL(request.url).pathname.slice(1) || 'lobby';
            const id = env.ROOMS.idFromName(name);
            const stub = env.ROOMS.get(env.ROOMS.idFromString(id.toString()));
            const response = await stub.fetch('https://room.internal/increment', { method: 'POST', body: name });
            return new Response(`${stub.name}:${stub.className}:${response.status}:${await response.text()}:${typeof DurableObjectNamespace}`);
        } };",
    );
    let room = TestWorker::from_source(
        "export default { async fetch(request) {
            await Peren.storage.transaction(async (storage) => {
                const current = await storage.get('count');
                const next = current === undefined ? 1 : current[0] + 1;
                await storage.put('count', new Uint8Array([next]));
            });
            const value = await Peren.storage.get('count');
            return new Response(`${new URL(request.url).pathname}:${request.method}:${await request.text()}:${value[0]}`, { status: 208 });
        } };",
    );
    let environment = DataEnv::new();
    let config = FleetConfig::from_toml(&format!(
        r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.ROOMS]
type = "durable_object_namespace"
class_name = "Room"
unique_key = "rooms"
[[services]]
name = "room"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.entrypoint]
kind = "durable_object"
class_name = "Room"
unique_key = "rooms"
id_from = {{ source = "first_path_segment" }}
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
"#,
        api.path().display(),
        room.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let first = get(address, "/lobby").await;
    let second = get(address, "/lobby").await;
    let other = get(address, "/other").await;

    assert!(
        first.ends_with("lobby:Room:208:/increment:POST:lobby:1:function"),
        "{first}"
    );
    assert!(
        second.ends_with("lobby:Room:208:/increment:POST:lobby:2:function"),
        "{second}"
    );
    assert!(
        other.ends_with("other:Room:208:/increment:POST:other:1:function"),
        "{other}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn service_binding_invokes_exported_rpc_method() {
    let api = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            const result = await env.AUTH.session('ada', { role: 'admin' });
            return Response.json(result);
        } };",
    );
    let auth = TestWorker::from_source(
        "export default {
            async session(name, details) {
                return { user: name, role: details.role, issued: true };
            }
        };",
    );
    let environment = DataEnv::new();
    let config = FleetConfig::from_toml(&format!(
        r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.AUTH]
type = "service"
entrypoint = "auth"
[[services]]
name = "auth"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
"#,
        api.path().display(),
        auth.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/rpc").await;

    assert!(
        response.contains(r#"{"user":"ada","role":"admin","issued":true}"#),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_object_stub_invokes_exported_rpc_method_on_object_cell() {
    let api = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            const id = env.ROOMS.idFromName('lobby');
            const room = env.ROOMS.get(id);
            const first = await room.increment('ada');
            const second = await room.increment('ada');
            return Response.json({ name: room.name, className: room.className, first, second });
        } };",
    );
    let room = TestWorker::from_source(
        "export default {
            async increment(user) {
                await Peren.storage.transaction(async (storage) => {
                    const current = await storage.get('count');
                    const next = current === undefined ? 1 : current[0] + 1;
                    await storage.put('count', new Uint8Array([next]));
                });
                const value = await Peren.storage.get('count');
                return { user, count: value[0] };
            }
        };",
    );
    let environment = DataEnv::new();
    let config = FleetConfig::from_toml(&format!(
        r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.ROOMS]
type = "durable_object_namespace"
class_name = "Room"
unique_key = "rooms"
[[services]]
name = "room"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.entrypoint]
kind = "durable_object"
class_name = "Room"
unique_key = "rooms"
id_from = {{ source = "first_path_segment" }}
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
"#,
        api.path().display(),
        room.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/rpc").await;

    assert!(response.contains(r#""name":"lobby""#), "{response}");
    assert!(response.contains(r#""className":"Room""#), "{response}");
    assert!(
        response.contains(r#""first":{"user":"ada","count":1}"#),
        "{response}"
    );
    assert!(
        response.contains(r#""second":{"user":"ada","count":2}"#),
        "{response}"
    );
    process.shutdown().await.unwrap();
}
