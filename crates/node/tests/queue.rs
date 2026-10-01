#[path = "support/config.rs"]
mod config;
#[path = "support/env.rs"]
mod env;

use std::{sync::LazyLock, time::Duration};

use env::DataEnv;
use peren_config::{Binding, QueueConsumer, QueueDefaults};
use peren_node::{Process, ProcessError, TailLevel, TailLogEvent, TailRead, read_tail};
use peren_testkit::{eventually::eventually, http::get, worker::TestWorker};
use tokio::time::sleep;

static QUEUE_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_listener_drains_queue_consumers() {
    let _guard = QUEUE_TEST_LOCK.lock().await;
    let worker = TestWorker::from_source(
        "export default {
            async fetch(request, env) {
                if (new URL(request.url).pathname === '/enqueue') {
                    await env.JOBS.send('alpha');
                }
                const value = await Peren.storage.get('seen');
                const seen = value === undefined ? 'pending' : new TextDecoder().decode(value);
                return new Response(`${env.JOBS.provider.kind}:${seen}`);
            },
            async queue(event) {
                console.warn('queue delivered', event.messages[0].body);
                await Peren.storage.transaction(async (storage) => {
                    await storage.put('seen', event.messages[0].body);
                });
            }
        };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::Memory,
        nats_url: None,
        file_path: None,
        cell_path: None,
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: QueueDefaults {
            max_batch_timeout_secs: 0,
            ..QueueDefaults::default()
        },
    });
    config.raw.services[0].bindings.insert(
        "JOBS".into(),
        Binding::Queue {
            queue_name: "memory-jobs".into(),
        },
    );
    config.raw.services[0].consumes_queues = vec![QueueConsumer::Name("memory-jobs".into())];
    let tail_config = config::worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let first = get(address, "/enqueue").await;
    assert!(first.ends_with("memory:pending"), "{first}");
    sleep(Duration::from_millis(750)).await;

    let observed = eventually(
        "memory queue consumer persists the message",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || get(address, "/__queue/memory-jobs"),
        |response| response.ends_with("memory:alpha"),
    )
    .await;

    assert!(observed.ends_with("memory:alpha"), "{observed}");
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
                if console.event == "queue"
                    && console.level == peren_node::TailConsoleLevel::Warn
                    && console.message == "queue delivered alpha"
        )),
        "{tail:?}"
    );
    process.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_listener_sends_queue_batch_from_worker_binding() {
    let _guard = QUEUE_TEST_LOCK.lock().await;
    let worker = TestWorker::from_source(
        "export default {
            async fetch(_request, env) {
                await env.JOBS.sendBatch([
                    { body: 'alpha' },
                    { body: { id: 2 }, contentType: 'application/json' }
                ]);
                return new Response(`${env.JOBS.provider.kind}:queued`);
            }
        };",
    );
    let environment = DataEnv::new();
    let path = environment.path().join("queues/batch.json");
    let mut config = config::worker(worker.path());
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::File,
        nats_url: None,
        file_path: Some(path.to_string_lossy().into_owned()),
        cell_path: None,
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: QueueDefaults::default(),
    });
    config.raw.services[0].bindings.insert(
        "JOBS".into(),
        Binding::Queue {
            queue_name: "batch-jobs".into(),
        },
    );
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let response = get(address, "/enqueue").await;
    assert!(response.ends_with("file:queued"), "{response}");

    let broker = peren_queues::FileBroker::open(&path).unwrap();
    let messages = broker.inspect("batch-jobs", 10);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].body, b"alpha");
    assert_eq!(messages[1].body, br#"{"id":2}"#);
    assert_eq!(
        messages[1].content_type.as_deref(),
        Some("application/json")
    );
    process.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_listener_drains_file_backed_queue_consumers() {
    let _guard = QUEUE_TEST_LOCK.lock().await;
    let worker = TestWorker::from_source(
        "export default {
            async fetch(request, env) {
                if (new URL(request.url).pathname === '/enqueue') {
                    await env.JOBS.send('file-backed');
                }
                const value = await Peren.storage.get('seen');
                const seen = value === undefined ? 'pending' : new TextDecoder().decode(value);
                return new Response(`${env.JOBS.provider.kind}:${seen}`);
            },
            async queue(event) {
                await Peren.storage.transaction(async (storage) => {
                    await storage.put('seen', event.messages[0].body);
                });
            }
        };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    let path = environment.path().join("queues/state.json");
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::File,
        nats_url: None,
        file_path: Some(path.to_string_lossy().into_owned()),
        cell_path: None,
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: QueueDefaults {
            max_batch_timeout_secs: 0,
            ..QueueDefaults::default()
        },
    });
    config.raw.services[0].bindings.insert(
        "JOBS".into(),
        Binding::Queue {
            queue_name: "jobs".into(),
        },
    );
    config.raw.services[0].consumes_queues = vec![QueueConsumer::Name("jobs".into())];
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let first = get(address, "/enqueue").await;
    assert!(first.ends_with("pending"), "{first}");
    sleep(Duration::from_millis(750)).await;

    let second = eventually(
        "file queue consumer persists the message",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || get(address, "/__queue/jobs"),
        |response| response.ends_with("file-backed"),
    )
    .await;

    assert!(second.ends_with("file-backed"), "{second}");
    assert!(path.exists());
    process.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_listener_drains_cell_backed_queue_consumers() {
    let _guard = QUEUE_TEST_LOCK.lock().await;
    let worker = TestWorker::from_source(
        "export default {
            async fetch(request, env) {
                if (new URL(request.url).pathname === '/enqueue') {
                    await env.JOBS.send('cell-backed');
                }
                const value = await Peren.storage.get('seen');
                const seen = value === undefined ? 'pending' : new TextDecoder().decode(value);
                return new Response(`${env.JOBS.provider.kind}:${seen}`);
            },
            async queue(event) {
                await Peren.storage.transaction(async (storage) => {
                    await storage.put('seen', event.messages[0].body);
                });
            }
        };",
    );
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    let path = environment.path().join("queues/cell.sqlite");
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::Cell,
        nats_url: None,
        file_path: None,
        cell_path: Some(path.to_string_lossy().into_owned()),
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: QueueDefaults {
            max_batch_timeout_secs: 0,
            ..QueueDefaults::default()
        },
    });
    config.raw.services[0].bindings.insert(
        "JOBS".into(),
        Binding::Queue {
            queue_name: "jobs".into(),
        },
    );
    config.raw.services[0].consumes_queues = vec![QueueConsumer::Name("jobs".into())];
    let process = Process::start(config, &environment).await.unwrap();
    let address = process.listeners()["public"];

    let first = get(address, "/enqueue").await;
    assert!(first.ends_with("pending"), "{first}");
    sleep(Duration::from_millis(750)).await;

    let second = eventually(
        "cell queue consumer persists the message",
        Duration::from_secs(10),
        Duration::from_millis(100),
        || get(address, "/__queue/jobs"),
        |response| response.ends_with("cell-backed"),
    )
    .await;

    assert!(second.ends_with("cell-backed"), "{second}");
    assert!(path.exists());
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn queue_admin_uses_the_configured_file_backed_broker() {
    let _guard = QUEUE_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queues.json");
    let config = queue_config(&path);
    let now = chrono::Utc::now();
    {
        let mut broker = peren_queues::FileBroker::open(&path).unwrap();
        broker.send(queue_send("dead", b"work"), now).unwrap();
        broker.send(queue_send("trash", b"drop"), now).unwrap();
    }

    assert!(
        peren_node::queue_pause(
            &config,
            &peren_node::QueuePause {
                queue: "dead".into()
            }
        )
        .await
        .unwrap()
        .changed
    );
    assert!(
        peren_node::queue_depth(
            &config,
            &peren_node::QueueDepth {
                queue: "dead".into()
            }
        )
        .await
        .unwrap()
        .paused
    );
    assert!(
        peren_node::queue_resume(
            &config,
            &peren_node::QueueResume {
                queue: "dead".into()
            }
        )
        .await
        .unwrap()
        .changed
    );

    let redrive = peren_node::queue_redrive(
        &config,
        &peren_node::QueueRedrive {
            source: "dead".into(),
            target: "jobs".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(redrive.moved, 1);
    assert_eq!(
        peren_node::queue_depth(
            &config,
            &peren_node::QueueDepth {
                queue: "jobs".into()
            }
        )
        .await
        .unwrap()
        .ready,
        1
    );

    let purge = peren_node::queue_purge(
        &config,
        &peren_node::QueuePurge {
            queue: "trash".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(purge.queued, 1);
    assert_eq!(
        peren_node::queue_depth(
            &config,
            &peren_node::QueueDepth {
                queue: "trash".into()
            }
        )
        .await
        .unwrap()
        .ready,
        0
    );
}

fn queue_send(queue: &str, body: &[u8]) -> peren_queues::Send {
    peren_queues::Send {
        queue: queue.into(),
        body: body.to_vec(),
        content_type: None,
        partition: None,
        delay: chrono::Duration::zero(),
        dedup_id: None,
    }
}

fn queue_config(path: &std::path::Path) -> peren_config::ValidatedConfig {
    peren_config::FleetConfig::from_toml(&format!(
        r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[queues]
broker = "file"
file_path = "{}"
[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
        path.display()
    ))
    .unwrap()
    .validate()
    .unwrap()
}

#[tokio::test]
async fn external_queue_broker_requires_its_endpoint() {
    let _guard = QUEUE_TEST_LOCK.lock().await;
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let mut config = config::worker(worker.path());
    config.raw.queues = Some(peren_config::Queues {
        broker: peren_config::QueueBroker::Kafka,
        nats_url: None,
        file_path: None,
        cell_path: None,
        amqp_url: None,
        kafka_bootstrap_servers: None,
        consumer_defaults: QueueDefaults::default(),
    });

    let error = match Process::start(config, &environment).await {
        Ok(process) => {
            process.shutdown().await.unwrap();
            panic!("process started without Kafka bootstrap servers");
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ProcessError::QueueAdapter(peren_queues::QueueError::MissingEndpoint(
            peren_queues::Adapter::Kafka
        ))
    ));
}
