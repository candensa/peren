#[path = "support/config.rs"]
mod config;
#[path = "support/env.rs"]
mod env;

use env::DataEnv;
use peren_config::{Binding, D1Backend, KvBackend};
use peren_node::Process;
use peren_testkit::{http::get, worker::TestWorker};

#[tokio::test]
async fn public_listener_passes_native_kv_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.CACHE.put('alpha', 'one', { metadata: { owner: 'ada' } });
            const value = await env.CACHE.get('alpha');
            const record = await env.CACHE.getWithMetadata('alpha');
            await env.CACHE.put('short', 'gone', { expirationTtl: -1, metadata: { stale: true } });
            const expired = await env.CACHE.getWithMetadata('short');
            return Response.json({ provider: env.CACHE.provider.kind, value, record, expired });
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "CACHE".into(),
        Binding::Kv {
            namespace: "cache".into(),
            unique_key: "cache-key".into(),
            backend: KvBackend::Native,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/kv").await;

    assert!(
        response.ends_with(
            r#"{"provider":"native","value":"one","record":{"value":"one","metadata":{"owner":"ada"},"version":1},"expired":{"value":null,"metadata":null,"version":null}}"#
        ),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_bucket_kv_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.CACHE.put('user:1', JSON.stringify({ name: 'Ada' }), { metadata: { role: 'user' } });
            await env.CACHE.put('admin:1', 'Grace');
            await env.CACHE.put('short', 'gone', { expirationTtl: -1, metadata: { stale: true } });
            const value = await env.CACHE.get('user:1', { type: 'json' });
            const record = await env.CACHE.getWithMetadata('user:1', { type: 'json' });
            const expired = await env.CACHE.getWithMetadata('short');
            const page = await env.CACHE.list({ prefix: 'user:' });
            await env.CACHE.delete('user:1');
            const missing = await env.CACHE.get('user:1');
            return Response.json({
                provider: env.CACHE.provider.kind,
                leak: env.CACHE.provider.secret_access_key_env,
                value,
                record,
                expired,
                keys: page.keys.map((key) => key.name),
                complete: page.list_complete,
                missing,
            });
        } };",
    );
    let environment = DataEnv::new()
        .with("KV_ACCESS_KEY", "test-access")
        .with("KV_SECRET_KEY", "test-secret");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "CACHE".into(),
        Binding::Kv {
            namespace: "cache".into(),
            unique_key: "cache-key".into(),
            backend: KvBackend::Bucket {
                endpoint: "memory://kv".into(),
                bucket: "kv".into(),
                prefix: "workers".into(),
                access_key_id_env: "KV_ACCESS_KEY".into(),
                secret_access_key_env: "KV_SECRET_KEY".into(),
                allow_http: false,
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/kv-provider").await;

    assert!(
        response.ends_with(
            r#"{"provider":"bucket","value":{"name":"Ada"},"record":{"value":{"name":"Ada"},"metadata":{"role":"user"},"version":null},"expired":{"value":null,"metadata":null,"version":null},"keys":["user:1"],"complete":true,"missing":null}"#
        ),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_native_d1_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.DB.prepare('CREATE TABLE records(id INTEGER PRIMARY KEY, label TEXT)').run();
            await env.DB.prepare('INSERT INTO records(label) VALUES (?)').bind('alpha').run();
            const row = await env.DB.prepare('SELECT label FROM records WHERE id=?').bind(1).first();
            return new Response(`${env.DB.provider.kind}:${row.label}`);
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "DB".into(),
        Binding::D1Database {
            database_name: "db".into(),
            unique_key: "db-key".into(),
            backend: D1Backend::NativeSqlite,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/d1").await;

    assert!(response.ends_with("native_sqlite:alpha"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_opaque_mtls_certificate_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.CLIENT_CERT.__perenMtlsBinding}:${env.CLIENT_CERT.cert === undefined}:${env.CLIENT_CERT.key === undefined}`);
        } };",
    );
    let environment = DataEnv::new()
        .with("CLIENT_CERT_PEM", "certificate")
        .with("CLIENT_KEY_PEM", "private-key");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "CLIENT_CERT".into(),
        Binding::MtlsCertificate {
            cert_pem_env: "CLIENT_CERT_PEM".into(),
            key_pem_env: "CLIENT_KEY_PEM".into(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/mtls").await;

    assert!(response.ends_with("CLIENT_CERT:true:true"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_mtls_certificate_material_fails_before_listeners_open() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new().with("CLIENT_CERT_PEM", "certificate");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "CLIENT_CERT".into(),
        Binding::MtlsCertificate {
            cert_pem_env: "CLIENT_CERT_PEM".into(),
            key_pem_env: "CLIENT_KEY_PEM".into(),
        },
    );

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started with missing binding material");
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("CLIENT_KEY_PEM"), "{error}");
}

#[tokio::test]
async fn public_listener_passes_secrets_store_secret_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.API_KEY}:${Object.prototype.toString.call(env.API_KEY)}`);
        } };",
    );
    let environment = DataEnv::new().with("SECRET_VALUE", "secret-value");
    let mut config = config::worker(worker.path());
    config
        .raw
        .secrets_store
        .insert("prod-api-key".into(), "SECRET_VALUE".into());
    config.raw.services[0].bindings.insert(
        "API_KEY".into(),
        Binding::SecretsStoreSecret {
            secret_name: "prod-api-key".into(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/secret").await;

    assert!(
        response.ends_with("secret-value:[object String]"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_secrets_store_secret_binding_fails_before_listeners_open() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config
        .raw
        .secrets_store
        .insert("prod-api-key".into(), "MISSING_SECRET".into());
    config.raw.services[0].bindings.insert(
        "API_KEY".into(),
        Binding::SecretsStoreSecret {
            secret_name: "prod-api-key".into(),
        },
    );

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started with missing binding material");
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("MISSING_SECRET"), "{error}");
}

#[tokio::test]
async fn public_listener_sanitizes_cache_provider_metadata() {
    let worker = TestWorker::from_source(
        "export default { async fetch() {
            return new Response(`${caches.default.provider.kind}:${caches.default.provider.url_env}:${caches.default.provider.secret_access_key_env}`);
        } };",
    );
    let environment = DataEnv::new().with("REDIS_URL", "redis://secret@example.com");
    let mut config = config::worker(worker.path());
    config.raw.cache = peren_config::Cache::Redis {
        url_env: "REDIS_URL".into(),
    };
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/cache").await;

    assert!(
        response.ends_with("redis:undefined:undefined"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_provider_credentials_are_required_at_startup() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.cache = peren_config::Cache::Redis {
        url_env: "REDIS_URL".into(),
    };

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started with missing binding material");
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("REDIS_URL"), "{error}");
}

#[tokio::test]
async fn public_listener_uses_bucket_backed_cache() {
    let worker = TestWorker::from_source(
        "export default { async fetch() {
            const request = new Request('https://cache.local/item');
            await caches.default.put(request, new Response('cached', { headers: { 'content-type': 'text/plain' } }));
            const response = await caches.default.match(request);
            return new Response(`${await response.text()}:${response.headers.get('content-type')}`);
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.cache = peren_config::Cache::Bucket {
        endpoint: "memory://cache".into(),
        bucket: "cache".into(),
        prefix: "edge".into(),
        access_key_id_env: "CACHE_ACCESS_KEY".into(),
        secret_access_key_env: "CACHE_SECRET_KEY".into(),
        allow_http: true,
    };
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/cache").await;

    assert!(response.ends_with("cached:text/plain"), "{response}");
    process.shutdown().await.unwrap();
}
