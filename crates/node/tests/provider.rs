#[path = "support/config.rs"]
mod config;
#[path = "support/env.rs"]
mod env;

use std::path::Path;

use env::DataEnv;
use peren_config::Binding;
use peren_node::Process;
use peren_testkit::{http::get, worker::TestWorker};

#[cfg(unix)]
fn executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions).unwrap();
}

#[cfg(not(unix))]
fn executable(_path: &Path) {}

#[tokio::test]
async fn public_listener_passes_ai_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.AI.endpoint}:${typeof env.AI.run}:${typeof Ai}`);
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "https://ai.internal".into(),
            credential_scope: "ai".into(),
            provider: peren_config::AiProvider::Http,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/ai").await;

    assert!(
        response.ends_with("https://ai.internal:function:function"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_sanitizes_ai_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.AI.provider.kind}:${env.AI.provider.endpoint}:${env.AI.provider.authorization}:${env.AI.provider.xApiKey}:${env.AI.provider.apiKey}`);
        } };",
    );
    let environment = DataEnv::new().with("ANTHROPIC_API_KEY", "secret-anthropic");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "unused".into(),
            credential_scope: "anthropic".into(),
            provider: peren_config::AiProvider::Anthropic {
                api_key_env: "ANTHROPIC_API_KEY".into(),
                base_url: "https://api.anthropic.com/v1".into(),
                version: "2023-06-01".into(),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/ai").await;

    assert!(
        response.ends_with("anthropic:https://api.anthropic.com/v1:undefined:undefined:undefined"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn ai_provider_credentials_are_required_at_startup() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "unused".into(),
            credential_scope: "google".into(),
            provider: peren_config::AiProvider::Gemini {
                api_key_env: "GEMINI_API_KEY".into(),
                base_url: "https://generativelanguage.googleapis.com/v1beta".into(),
            },
        },
    );

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started without GEMINI_API_KEY");
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("GEMINI_API_KEY"), "{error}");
}

#[tokio::test]
async fn public_listener_redacts_workers_ai_provider_metadata() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.AI.provider.kind}:${env.AI.endpoint}:${env.AI.provider.endpoint}:${env.AI.provider.authorization}`);
        } };",
    );
    let environment = DataEnv::new()
        .with("CLOUDFLARE_ACCOUNT_ID", "account-secret")
        .with("CLOUDFLARE_API_TOKEN", "token-secret");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "unused".into(),
            credential_scope: "cloudflare".into(),
            provider: peren_config::AiProvider::WorkersAi {
                account_id_env: "CLOUDFLARE_ACCOUNT_ID".into(),
                api_token_env: "CLOUDFLARE_API_TOKEN".into(),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/ai").await;

    assert!(
        response.ends_with("workers_ai:https://api.cloudflare.com/client/v4/accounts/redacted/ai/run:https://api.cloudflare.com/client/v4/accounts/redacted/ai/run:undefined"),
        "{response}"
    );
    assert!(!response.contains("account-secret"), "{response}");
    assert!(!response.contains("token-secret"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_accepts_gemini_ai_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.AI.provider.kind}:${env.AI.provider.endpoint}:${env.AI.provider.apiKey}`);
        } };",
    );
    let environment = DataEnv::new().with("GEMINI_API_KEY", "secret-gemini");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "unused".into(),
            credential_scope: "google".into(),
            provider: peren_config::AiProvider::Gemini {
                api_key_env: "GEMINI_API_KEY".into(),
                base_url: "https://generativelanguage.googleapis.com/v1beta".into(),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/ai").await;

    assert!(
        response.ends_with("gemini:https://generativelanguage.googleapis.com/v1beta:undefined"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_runs_local_ai_provider_command() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            const result = await env.AI.run('summary', { text: 'hello' }, { trace: true });
            return new Response(`${result.model}:${result.input.text}:${result.options.trace}:${typeof Ai}`);
        } };",
    );
    let environment = DataEnv::new();
    let command = environment.path().join("ai");
    std::fs::create_dir_all(environment.path()).unwrap();
    std::fs::write(&command, "#!/usr/bin/env sh\ncat\n").unwrap();
    executable(&command);
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "local".into(),
            credential_scope: "ai".into(),
            provider: peren_config::AiProvider::Local {
                command: command.display().to_string(),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/ai").await;

    assert!(
        response.ends_with("summary:hello:true:function"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_redacts_openai_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.AI.provider.kind}:${env.AI.provider.endpoint}:${env.AI.provider.authorization}`);
        } };",
    );
    let environment = DataEnv::new().with("OPENAI_API_KEY", "secret-openai");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "AI".into(),
        Binding::Ai {
            endpoint: "unused".into(),
            credential_scope: "openai".into(),
            provider: peren_config::AiProvider::OpenAi {
                api_key_env: "OPENAI_API_KEY".into(),
                base_url: "https://api.openai.com/v1".into(),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/ai").await;

    assert!(
        response.ends_with("open_ai:https://api.openai.com/v1:undefined"),
        "{response}"
    );
    assert!(!response.contains("secret-openai"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_vectorize_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.VECTORS.upsert([
              { id: 'a', values: [1, 0], metadata: { color: 'red' } },
              { id: 'b', values: [0, 1], metadata: { color: 'blue' } }
            ]);
            const result = await env.VECTORS.query([1, 0], { topK: 1, returnMetadata: true, returnValues: true });
            const fetched = await env.VECTORS.getByIds(['a']);
            await env.VECTORS.deleteByIds(['b']);
            return new Response(`${result.matches[0].id}:${result.matches[0].score}:${fetched[0].metadata.color}:${typeof VectorizeIndex}`);
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "VECTORS".into(),
        Binding::Vectorize {
            endpoint: "local-search".into(),
            credential_scope: "vectors".into(),
            provider: peren_config::VectorProvider::Local,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/vectors").await;

    assert!(response.ends_with("a:1:red:function"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_applies_vectorize_filters_and_fetches_ids() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.VECTORS.upsert([
              { id: 'intro', values: [1, 0], metadata: { section: 'guide' } },
              { id: 'draft', values: [0, 1], metadata: { section: 'draft' } }
            ]);
            const result = await env.VECTORS.query([1, 0], { topK: 5, returnMetadata: true, returnValues: true, filter: { section: 'guide' } });
            const fetched = await env.VECTORS.getByIds(['intro']);
            return Response.json({ ids: result.matches.map((match) => match.id), section: fetched[0].metadata.section });
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "VECTORS".into(),
        Binding::Vectorize {
            endpoint: "local-search".into(),
            credential_scope: "vectors".into(),
            provider: peren_config::VectorProvider::Local,
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/vectors").await;

    assert!(
        response.ends_with(r#"{"ids":["intro"],"section":"guide"}"#),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_sanitizes_vector_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.VECTORS.provider.kind}:${env.VECTORS.provider.endpoint}:${env.VECTORS.provider.apiKey}:${env.VECTORS.provider.authorization}`);
        } };",
    );
    let environment = DataEnv::new().with("PINECONE_API_KEY", "secret-pinecone");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "VECTORS".into(),
        Binding::Vectorize {
            endpoint: "articles".into(),
            credential_scope: "vectors".into(),
            provider: peren_config::VectorProvider::Pinecone {
                url: "https://pinecone.internal".into(),
                index: "articles".into(),
                api_key_env: "PINECONE_API_KEY".into(),
                namespace: Some("docs".into()),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/vectors").await;

    assert!(
        response.ends_with("pinecone:https://pinecone.internal:undefined:undefined"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn vector_provider_credentials_are_required_at_startup() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "VECTORS".into(),
        Binding::Vectorize {
            endpoint: "articles".into(),
            credential_scope: "vectors".into(),
            provider: peren_config::VectorProvider::Pinecone {
                url: "https://pinecone.internal".into(),
                index: "articles".into(),
                api_key_env: "PINECONE_API_KEY".into(),
                namespace: None,
            },
        },
    );

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started without PINECONE_API_KEY");
        }
        Err(error) => error,
    };

    assert!(error.to_string().contains("PINECONE_API_KEY"), "{error}");
}

#[tokio::test]
async fn public_listener_sanitizes_qdrant_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.VECTORS.provider.kind}:${env.VECTORS.provider.endpoint}:${env.VECTORS.provider.collection}:${env.VECTORS.provider.apiKey}`);
        } };",
    );
    let environment = DataEnv::new().with("QDRANT_API_KEY", "secret-qdrant");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "VECTORS".into(),
        Binding::Vectorize {
            endpoint: "articles".into(),
            credential_scope: "vectors".into(),
            provider: peren_config::VectorProvider::Qdrant {
                url: "https://qdrant.internal".into(),
                collection: "articles".into(),
                api_key_env: Some("QDRANT_API_KEY".into()),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/vectors").await;

    assert!(
        response.ends_with("qdrant:https://qdrant.internal:articles:undefined"),
        "{response}"
    );
    assert!(!response.contains("secret-qdrant"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_sanitizes_weaviate_provider_credentials() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.VECTORS.provider.kind}:${env.VECTORS.provider.endpoint}:${env.VECTORS.provider.className}:${env.VECTORS.provider.apiKey}`);
        } };",
    );
    let environment = DataEnv::new().with("WEAVIATE_API_KEY", "secret-weaviate");
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "VECTORS".into(),
        Binding::Vectorize {
            endpoint: "articles".into(),
            credential_scope: "vectors".into(),
            provider: peren_config::VectorProvider::Weaviate {
                url: "https://weaviate.internal".into(),
                class_name: "Article".into(),
                api_key_env: Some("WEAVIATE_API_KEY".into()),
            },
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/vectors").await;

    assert!(
        response.ends_with("weaviate:https://weaviate.internal:Article:undefined"),
        "{response}"
    );
    assert!(!response.contains("secret-weaviate"), "{response}");
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn remote_r2_binding_requires_explicit_credentials_at_startup() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "FILES".into(),
        Binding::R2Bucket {
            endpoint: "https://objects.example".into(),
            bucket: "uploads".into(),
            credential_scope: "objects".into(),
            region: None,
            access_key_env: None,
            secret_key_env: None,
            token_env: None,
            allow_http: false,
            prefix: None,
            notifications: Vec::new(),
        },
    );

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started without R2 credentials");
        }
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains(r#"R2 bucket "uploads" requires access_key_env"#),
        "{error}"
    );
}

#[tokio::test]
async fn public_listener_exposes_safe_r2_provider_metadata() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            return new Response(`${env.BUCKET.provider.kind}:${env.BUCKET.provider.access_key_env}:${env.BUCKET.provider.secret_key_env}`);
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "BUCKET".into(),
        Binding::R2Bucket {
            endpoint: "memory://objects".into(),
            bucket: "objects".into(),
            credential_scope: "objects".into(),
            region: None,
            access_key_env: None,
            secret_key_env: None,
            token_env: None,
            allow_http: false,
            prefix: None,
            notifications: Vec::new(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/r2").await;

    assert!(
        response.ends_with("memory:undefined:undefined"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_listener_passes_r2_binding_to_worker() {
    let worker = TestWorker::from_source(
        "export default { async fetch(_request, env) {
            await env.FILES.put('avatar.txt', 'hello', { httpMetadata: { contentType: 'text/plain' }, customMetadata: { owner: 'ada' } });
            const object = await env.FILES.get('avatar.txt');
            const page = await env.FILES.list();
            await env.FILES.delete('avatar.txt');
            const missing = await env.FILES.get('avatar.txt');
            return new Response(`${await object.text()}:${object.httpMetadata.contentType}:${object.customMetadata.owner}:${page.objects.length}:${missing === null}`);
        } };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.services[0].bindings.insert(
        "FILES".into(),
        Binding::R2Bucket {
            endpoint: "memory://objects".into(),
            bucket: "uploads".into(),
            credential_scope: "objects".into(),
            region: None,
            access_key_env: None,
            secret_key_env: None,
            token_env: None,
            allow_http: false,
            prefix: None,
            notifications: Vec::new(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/r2").await;

    assert!(
        response.ends_with("hello:text/plain:ada:1:true"),
        "{response}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_notifications_enqueue_filtered_queue_events() {
    let worker = TestWorker::from_source(
        "export default {
            async fetch(request, env) {
                const path = new URL(request.url).pathname;
                if (path === '/write') {
                    await env.FILES.put('images/a.txt', 'created');
                    await env.FILES.put('logs/a.txt', 'ignored');
                    await env.FILES.delete('images/a.txt');
                    return new Response('written');
                }
                return new Response('ok');
            }
        };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    let queue_path = environment.path().join("queues/state.json");
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::File,
        nats_url: None,
        file_path: Some(queue_path.to_string_lossy().into_owned()),
        cell_path: None,
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: peren_config::QueueDefaults::default(),
    });
    config.raw.services[0].bindings.insert(
        "FILES".into(),
        Binding::R2Bucket {
            endpoint: "memory://objects".into(),
            bucket: "uploads".into(),
            credential_scope: "objects".into(),
            region: None,
            access_key_env: None,
            secret_key_env: None,
            token_env: None,
            allow_http: false,
            prefix: None,
            notifications: vec![peren_config::R2Notification {
                queue_name: "changes".into(),
                event_types: vec![
                    peren_config::R2EventType::ObjectCreate,
                    peren_config::R2EventType::ObjectDelete,
                ],
                prefix: Some("images/".into()),
                suffix: Some(".txt".into()),
            }],
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let written = get(address, "/write").await;
    assert!(written.ends_with("written"), "{written}");

    let broker = peren_queues::FileBroker::open(&queue_path).unwrap();
    let messages = broker.inspect("changes", 10);
    assert_eq!(messages.len(), 2);
    let events = messages
        .iter()
        .map(|message| serde_json::from_slice::<serde_json::Value>(&message.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        vec![
            serde_json::json!({
                "source": "peren.r2",
                "type": "object_create",
                "bucket": "uploads",
                "key": "images/a.txt",
                "eventTime": events[0]["eventTime"],
            }),
            serde_json::json!({
                "source": "peren.r2",
                "type": "object_delete",
                "bucket": "uploads",
                "key": "images/a.txt",
                "eventTime": events[1]["eventTime"],
            })
        ]
    );
    process.shutdown().await.unwrap();
}
