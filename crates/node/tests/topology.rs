#[path = "support/env.rs"]
mod env;

use env::DataEnv;
use peren_config::FleetConfig;
use peren_node::Process;
use peren_testkit::{http::get, worker::TestWorker};

#[tokio::test]
async fn sockets_dispatch_to_their_configured_services() {
    let api = TestWorker::from_source(
        "export default { async fetch() {
            await Peren.storage.transaction(async (storage) => {
                const value = await storage.get('count');
                await storage.put('count', new Uint8Array([value === undefined ? 1 : value[0] + 1]));
            });
            const value = await Peren.storage.get('count');
            return new Response('api:' + value[0]);
        } };",
    );
    let admin = TestWorker::from_source(
        "export default { async fetch() {
            await Peren.storage.transaction(async (storage) => {
                const value = await storage.get('count');
                await storage.put('count', new Uint8Array([value === undefined ? 1 : value[0] + 1]));
            });
            const value = await Peren.storage.get('count');
            return new Response('admin:' + value[0]);
        } };",
    );
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
[[services]]
name = "admin"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
[[sockets]]
name = "admin"
listen = "127.0.0.1:0"
service = "admin"
"#,
        api.path().display(),
        admin.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let environment = DataEnv::new();
    let process = Process::start(config, &environment).await.unwrap();

    let public = process.listeners()["public"];
    let admin = process.listeners()["admin"];
    let public_first = get(public, "/same").await;
    let public_second = get(public, "/same").await;
    let admin_first = get(admin, "/same").await;

    assert!(public_first.ends_with("api:1"), "{public_first}");
    assert!(public_second.ends_with("api:2"), "{public_second}");
    assert!(admin_first.ends_with("admin:1"), "{admin_first}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn service_environment_keeps_tenant_secrets_isolated() {
    let source = "export default { async fetch(_request, env) {
            return new Response(`${env.PUBLIC}:${env.TOKEN ?? 'missing'}:${env.OTHER_TOKEN ?? 'missing'}`);
        } };";
    let acme = TestWorker::from_source(source);
    let beta = TestWorker::from_source(source);
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
[[tenants]]
id = "acme"
cell_quota = 10
[[tenants]]
id = "beta"
cell_quota = 10
[[services]]
name = "acme"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
tenant_id = "acme"
vars = {{ PUBLIC = "acme" }}
secrets = {{ TOKEN = "ACME_TOKEN" }}
[[services]]
name = "beta"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
tenant_id = "beta"
vars = {{ PUBLIC = "beta" }}
secrets = {{ TOKEN = "BETA_TOKEN" }}
[[sockets]]
name = "acme"
listen = "127.0.0.1:0"
service = "acme"
[[sockets]]
name = "beta"
listen = "127.0.0.1:0"
service = "beta"
"#,
        acme.path().display(),
        beta.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let environment = DataEnv::new()
        .with("ACME_TOKEN", "acme-secret")
        .with("BETA_TOKEN", "beta-secret");
    let process = Process::start(config, &environment).await.unwrap();

    let acme_response = get(process.listeners()["acme"], "/").await;
    let beta_response = get(process.listeners()["beta"], "/").await;

    assert!(
        acme_response.ends_with("acme:acme-secret:missing"),
        "{acme_response}"
    );
    assert!(
        beta_response.ends_with("beta:beta-secret:missing"),
        "{beta_response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn service_bindings_keep_tenant_storage_isolated() {
    let acme = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.CACHE.put('shared', env.NAME);
            const value = await env.CACHE.get('shared');
            return new Response(`${env.NAME}:${value ?? 'missing'}`);
        } };",
    );
    let beta = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            const value = await env.CACHE.get('shared');
            return new Response(`${env.NAME}:${value ?? 'missing'}`);
        } };",
    );
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
[[tenants]]
id = "acme"
cell_quota = 10
[[tenants]]
id = "beta"
cell_quota = 10
[[services]]
name = "acme"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
tenant_id = "acme"
vars = {{ NAME = "acme" }}
[services.bindings.CACHE]
type = "kv"
namespace = "tenant/acme/cache"
unique_key = "tenant-acme-cache"
backend = {{ kind = "native" }}
[[services]]
name = "beta"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
tenant_id = "beta"
vars = {{ NAME = "beta" }}
[services.bindings.CACHE]
type = "kv"
namespace = "tenant/beta/cache"
unique_key = "tenant-beta-cache"
backend = {{ kind = "native" }}
[[sockets]]
name = "acme"
listen = "127.0.0.1:0"
service = "acme"
[[sockets]]
name = "beta"
listen = "127.0.0.1:0"
service = "beta"
"#,
        acme.path().display(),
        beta.path().display()
    ))
    .unwrap()
    .validate()
    .unwrap();
    let environment = DataEnv::new();
    let process = Process::start(config, &environment).await.unwrap();

    let acme_write = get(process.listeners()["acme"], "/").await;
    let beta_read = get(process.listeners()["beta"], "/").await;
    let acme_read = get(process.listeners()["acme"], "/").await;

    assert!(acme_write.ends_with("acme:acme"), "{acme_write}");
    assert!(beta_read.ends_with("beta:missing"), "{beta_read}");
    assert!(acme_read.ends_with("acme:acme"), "{acme_read}");
    process.shutdown().await.unwrap();
}
