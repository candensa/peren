#[path = "support/config.rs"]
mod config;
#[path = "support/env.rs"]
mod env;

use std::collections::BTreeMap;

use env::DataEnv;
use peren_config::Binding;
use peren_node::Process;
use peren_testkit::{http::get, worker::TestWorker};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn public_listener_passes_images_binding_to_worker() {
    let worker = TestWorker::from_source(
            "export default { async fetch(_request, env) {
                return new Response(`${env.IMAGES.provider.kind}:${env.IMAGES.provider.authorization}:${typeof env.IMAGES.input}:${typeof ImagesBinding}`);
            } };",
        );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "IMAGES".into(),
        Binding::Images {
            provider: peren_config::ImageProvider::Local,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/images").await;

    assert!(
        response.ends_with("local:undefined:function:function"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_container_binding_to_worker() {
    let worker = TestWorker::from_source(
            "export default { async fetch(_request, env) {
                const named = env.APP.get('build');
                return new Response(`${env.APP.provider.kind}:${env.APP.image}:${env.APP.port}:${env.APP.experimental}:${named.provider.kind}:${named.id}:${typeof named.exec}:${typeof named.readFile}:${typeof env.APP.fetch}:${typeof Container}`);
            } };",
        );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.containers = Some(peren_config::Containers {
        docker_socket: None,
        max_instances_per_node: 4,
    });
    config.raw.services[0].bindings.insert(
        "APP".into(),
        Binding::Container {
            image: "ghcr.io/candensa/peren-demo:latest".into(),
            default_port: 8080,
            env: BTreeMap::new(),
            memory_mb: Some(256),
            cpu_millis: Some(250),
            idle_sleep_secs: 30,
            allow_network_egress: false,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/container").await;

    assert!(
        response.ends_with(
            "local:ghcr.io/candensa/peren-demo:latest:8080:true:local:build:function:function:function:function"
        ),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_loader_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                return new Response(`${typeof env.LOADER.import}:${typeof Loader}`);
            } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0]
        .bindings
        .insert("LOADER".into(), Binding::Loader);
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/loader").await;

    assert!(response.ends_with("function:function"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_dispatcher_binding_to_worker() {
    let api = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            const stub = env.DISPATCH.get('tenant-a');
            const response = await stub.fetch('https://tenant.internal/work', { method: 'PUT', body: 'job' });
            return new Response(`${env.DISPATCH.namespace}:${env.DISPATCH.scripts.join(',')}:${typeof stub.fetch}:${typeof DispatchNamespace}:${response.status}:${await response.text()}`);
        } };",
    );
    let tenant = TestWorker::from_source(
        "export default { async fetch(request) {
            return new Response(`${new URL(request.url).pathname}:${request.method}:${await request.text()}`, { status: 209 });
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(api.path());
    config
        .raw
        .dispatch_namespaces
        .push(peren_config::DispatchNamespace {
            name: "tenants".into(),
            scripts: vec![peren_config::DispatchScript {
                name: "tenant-a".into(),
                worker_bundle_path: tenant.path().to_path_buf(),
                compatibility_date: "2026-01-01".into(),
                compatibility_flags: Vec::new(),
                cell_quota: 100,
                deployment: peren_config::DispatchDeployment::default(),
            }],
        });
    config.raw.services[0].bindings.insert(
        "DISPATCH".into(),
        Binding::Dispatcher {
            namespace: "tenants".into(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/dispatch").await;

    assert!(
        response.ends_with("tenants:tenant-a:function:function:209:/work:PUT:job"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_hyperdrive_binding_to_worker() {
    let worker = TestWorker::from_source(
            "export default { async fetch(_request, env) {
                return new Response(`${env.DB.provider.kind}:${env.DB.connectionString}:${env.DB.poolMaxConnections}:${env.DB.cachingDisabled}:${typeof Hyperdrive}`);
            } };",
        );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "DB".into(),
        Binding::Hyperdrive {
            pgcat_endpoint: "postgres://peren:secret@127.0.0.1:6432/app".into(),
            credential_scope: "pgcat".into(),
            caching_disabled: true,
            max_age_secs: 15,
            stale_while_revalidate_secs: 30,
            pool_max_connections: 8,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/db").await;

    assert!(
        response.ends_with("pgcat:postgres://redacted:redacted@127.0.0.1:6432/app:8:true:function"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_rate_limiter_binding_to_worker() {
    let worker = TestWorker::from_source(
            "export default { async fetch(request, env) {
                const key = new URL(request.url).pathname;
                const first = await env.LIMITER.limit({ key });
                const second = await env.LIMITER.limit({ key });
                return new Response(`${env.LIMITER.provider.kind}:${first.success}:${first.remaining}:${second.success}:${second.remaining}:${typeof RateLimiter}`);
            } };",
        );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "LIMITER".into(),
        Binding::RateLimiter {
            limit: 1,
            period_secs: 60,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/tenant-a").await;

    assert!(
        response.ends_with("memory:true:0:false:0:function"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_analytics_engine_binding_to_worker() {
    let worker = TestWorker::from_source(
            "export default { async fetch(_request, env) {
                env.ANALYTICS.writeDataPoint({ blobs: ['request'], doubles: [1.0], indexes: ['tenant'] });
                return new Response(`${env.ANALYTICS.provider.kind}:${env.ANALYTICS.provider.dataset}:${typeof env.ANALYTICS.writeDataPoint}:${typeof AnalyticsEngineDataset}`);
            } };",
        );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "ANALYTICS".into(),
        Binding::AnalyticsEngine {
            dataset: "requests".into(),
            credential_scope: "analytics".into(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/analytics").await;

    assert!(response.ends_with("function:function"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_workflow_binding_to_worker() {
    let worker = TestWorker::from_source(
            "export default { async fetch(_request, env) {
                const instance = await env.FLOW.create({ id: 'deploy-1' });
                await instance.terminate('complete');
                const state = await env.FLOW.get('deploy-1').status();
                return new Response(`${env.FLOW.provider.kind}:${instance.id}:${state.status}:${state.reason}:${typeof Workflow}`);
            } };",
        );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "FLOW".into(),
        Binding::Workflow {
            class_name: "DeployWorkflow".into(),
            unique_key: "deploy-workflow".into(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/workflow").await;

    assert!(
        response.ends_with("native:deploy-1:terminated:complete:function"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_routes_http_images_provider_output() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let mut request = vec![0; 2048];
        let read = stream.read(&mut request).await.unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("POST /transform HTTP/1.1"), "{request}");
        assert!(
            request.contains("authorization: Bearer secret-images"),
            "{request}"
        );
        assert!(
            request.contains(r#"{"source":"avatar","transforms":{"width":64}}"#),
            "{request}"
        );
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 7\r\nconnection: close\r\n\r\nresized",
            )
            .await
            .unwrap();
    });
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                const output = await env.IMAGES.input('avatar').transform({ width: 64 }).output();
                return new Response(`${output.status}:${output.headers.get('content-type')}:${await output.text()}:${env.IMAGES.provider.authorization}`);
            } };",
    );
    let environment = DataEnv::new().with("IMAGES_TOKEN", "secret-images");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "IMAGES".into(),
        Binding::Images {
            provider: peren_config::ImageProvider::Http {
                url: format!("http://{upstream_address}"),
                token_env: Some("IMAGES_TOKEN".into()),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/images").await;

    assert!(
        response.ends_with("200:text/plain:resized:undefined"),
        "{response}"
    );
    assert!(!response.contains("secret-images"), "{response}");
    upstream_task.await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_sanitizes_http_images_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                return new Response(`${env.IMAGES.provider.kind}:${env.IMAGES.provider.endpoint}:${env.IMAGES.provider.authorization}`);
            } };",
    );
    let environment = DataEnv::new().with("IMAGES_TOKEN", "secret-images");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "IMAGES".into(),
        Binding::Images {
            provider: peren_config::ImageProvider::Http {
                url: "https://images.internal".into(),
                token_env: Some("IMAGES_TOKEN".into()),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/images").await;

    assert!(
        response.ends_with("http:https://images.internal:undefined"),
        "{response}"
    );
    assert!(!response.contains("secret-images"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn images_provider_credentials_are_required_at_startup() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "IMAGES".into(),
        Binding::Images {
            provider: peren_config::ImageProvider::Http {
                url: "https://images.internal".into(),
                token_env: Some("IMAGES_TOKEN".into()),
            },
        },
    );

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started without IMAGES_TOKEN");
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("IMAGES_TOKEN"), "{error}");
}

#[tokio::test]
async fn public_listener_routes_allowed_outbound_binding_fetch() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let mut request = vec![0; 1024];
        let read = stream.read(&mut request).await.unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("POST /items HTTP/1.1"), "{request}");
        stream
            .write_all(
                b"HTTP/1.1 202 Accepted\r\nx-upstream: peren\r\ncontent-length: 8\r\nconnection: close\r\n\r\naccepted",
            )
            .await
            .unwrap();
    });
    let worker = TestWorker::from_source(&format!(
        "export default {{ async fetch(_request, env) {{
                const response = await env.OUT.fetch('http://{upstream_address}/items', {{ method: 'POST', body: 'payload' }});
                return new Response(`${{response.status}}:${{response.headers.get('x-upstream')}}:${{await response.text()}}:${{env.OUT.allowedHosts.join(',')}}`);
            }} }};"
    ));
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "OUT".into(),
        Binding::Outbound {
            allowed_hosts: vec![upstream_address.to_string()],
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/outbound").await;

    assert!(
        response.ends_with(&format!("202:peren:accepted:{upstream_address}")),
        "{response}"
    );
    upstream_task.await.unwrap();
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_decompresses_gzip_outbound_fetch() {
    const GZIP: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 75, 206, 207, 45, 40, 74, 45, 46, 78, 77, 81, 200, 72,
        205, 201, 201, 7, 0, 182, 1, 153, 70, 16, 0, 0, 0,
    ];

    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let mut request = vec![0; 1024];
        let read = stream.read(&mut request).await.unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("GET /gzip HTTP/1.1"), "{request}");
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    GZIP.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.write_all(GZIP).await.unwrap();
    });

    let worker = TestWorker::from_source(&format!(
        "export default {{ async fetch(_request, env) {{
                const response = await env.OUT.fetch('http://{upstream_address}/gzip');
                return new Response(`${{response.status}}:${{response.headers.get('content-encoding')}}:${{await response.text()}}`);
            }} }};"
    ));
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "OUT".into(),
        Binding::Outbound {
            allowed_hosts: vec![upstream_address.to_string()],
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/outbound").await;

    assert!(
        response.ends_with("200:null:compressed hello"),
        "{response}"
    );
    upstream_task.await.unwrap();
    process.shutdown().await.unwrap();
}
#[tokio::test]
async fn public_listener_rejects_unlisted_outbound_binding_host() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
                try {
                  await env.OUT.fetch('http://127.0.0.1:9/items');
                  return new Response('unreachable');
                } catch (error) {
                  return new Response(`${error.name}:${error.message.includes('not allowed')}`);
                }
            } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "OUT".into(),
        Binding::Outbound {
            allowed_hosts: vec!["api.internal".into()],
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/outbound").await;

    assert!(response.ends_with("TypeError:true"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_signs_aws_binding_fetch_without_exposing_credentials() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.unwrap();
        let mut request = vec![0; 4096];
        let read = stream.read(&mut request).await.unwrap();
        let request = String::from_utf8_lossy(&request[..read]);
        assert!(request.starts_with("POST /model HTTP/1.1"), "{request}");
        assert!(
            request.contains("authorization: AWS4-HMAC-SHA256"),
            "{request}"
        );
        assert!(request.contains("Credential=test-access/"), "{request}");
        assert!(request.contains("SignedHeaders="), "{request}");
        assert!(request.contains("x-amz-date:"), "{request}");
        assert!(request.contains("x-amz-content-sha256:"), "{request}");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 6\r\nconnection: close\r\n\r\nsigned")
            .await
            .unwrap();
    });
    let worker = TestWorker::from_source(&format!(
        "export default {{ async fetch(_request, env) {{
                const response = await env.AWS.fetch('http://{upstream_address}/model', {{ method: 'POST', body: 'payload' }});
                return new Response(`${{response.status}}:${{await response.text()}}:${{env.AWS.region}}:${{env.AWS.service}}:${{env.AWS.allowedHosts.join(',')}}:${{typeof env.AWS.accessKeyId}}:${{typeof env.AWS.secretAccessKey}}`);
            }} }};"
    ));
    let environment = DataEnv::new()
        .with("AWS_ACCESS_KEY_ID", "test-access")
        .with("AWS_SECRET_ACCESS_KEY", "test-secret");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AWS".into(),
        Binding::AwsSigv4 {
            credential_source: peren_config::CredentialsSource::Environment,
            region: "us-east-1".into(),
            service: "bedrock".into(),
            allowed_hosts: vec![upstream_address.to_string()],
            access_key_env: Some("AWS_ACCESS_KEY_ID".into()),
            secret_key_env: Some("AWS_SECRET_ACCESS_KEY".into()),
            token_env: None,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/aws").await;

    assert!(
        response.ends_with(&format!(
            "200:signed:us-east-1:bedrock:{upstream_address}:undefined:undefined"
        )),
        "{response}"
    );
    upstream_task.await.unwrap();
    process.shutdown().await.unwrap();
}
