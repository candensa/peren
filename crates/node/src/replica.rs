use crate::*;
use peren_cell::WorkerCell;
use peren_fleet::{RecoveryObservation, RecoveryPolicy};
use peren_primitives::OwnershipEpoch;
use peren_runtime::{Module, ModuleKind, ModuleName};
use std::time::Duration;
use std::{collections::BTreeMap, collections::BTreeSet, num::NonZeroUsize};
use uuid::Uuid;

pub(crate) fn recovery(owner: NodeId, epoch: OwnershipEpoch) -> peren_fleet::RecoveryEvidence {
    RecoveryPolicy::new(NonZeroUsize::new(2).unwrap(), 100)
        .authorize(&RecoveryObservation::new(
            owner,
            epoch,
            1_000,
            1_100,
            BTreeSet::from([
                NodeId::from_uuid(Uuid::new_v4()),
                NodeId::from_uuid(Uuid::new_v4()),
            ]),
        ))
        .unwrap()
}

pub(crate) fn bundle(source: &str) -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(ModuleKind::JavaScript, source.as_bytes()).unwrap(),
        )]),
    )
    .unwrap()
}

pub(crate) fn counter_bundle() -> WorkerBundle {
    bundle(
        "export default { async fetch(request) {
          if (new URL(request.url).pathname === '/increment') {
            await Peren.storage.transaction(async (storage) => {
              const current = await storage.get('counter');
              const next = current === undefined ? 1 : current[0] + 1;
              await storage.put('counter', new Uint8Array([next]));
            });
          }
          const value = await Peren.storage.get('counter');
          return new Response(String(value?.[0] ?? 0));
        } };",
    )
}

pub(crate) fn alarm_bundle() -> WorkerBundle {
    bundle(
        "export default {
          async alarm() {
            await Peren.storage.transaction(async (storage) => {
              const current = await storage.get('counter');
              const next = current === undefined ? 1 : current[0] + 1;
              await storage.put('counter', new Uint8Array([next]));
            });
          },
          async fetch() {
            const value = await Peren.storage.get('counter');
            return new Response(String(value?.[0] ?? 0));
          }
        };",
    )
}

pub(crate) fn slow_alarm_bundle() -> WorkerBundle {
    bundle(
        "export default {
          async alarm() {
            await new Promise(resolve => setTimeout(resolve, 100));
            await Peren.storage.transaction(async (storage) => {
              const current = await storage.get('counter');
              const next = current === undefined ? 1 : current[0] + 1;
              await storage.put('counter', new Uint8Array([next]));
            });
          },
          async fetch() {
            const value = await Peren.storage.get('counter');
            return new Response(String(value?.[0] ?? 0));
          }
        };",
    )
}

pub(crate) fn scheduled_bundle() -> WorkerBundle {
    bundle(
        "export default {
          async scheduled(event) {
            await Peren.storage.transaction(async (storage) => {
              await storage.put('counter', new Uint8Array([event.scheduledTime / 1000]));
            });
          },
          async fetch() {
            const value = await Peren.storage.get('counter');
            return new Response(String(value?.[0] ?? 0));
          }
        };",
    )
}

pub(crate) fn queue_bundle() -> WorkerBundle {
    bundle(
        "export default {
          async queue(event) {
            await Peren.storage.transaction(async (storage) => {
              await storage.put('counter', new Uint8Array([event.messages[0].body.charCodeAt(0)]));
            });
          },
          async fetch() {
            const value = await Peren.storage.get('counter');
            return new Response(String(value?.[0] ?? 0));
          }
        };",
    )
}

pub(crate) fn tail_bundle() -> WorkerBundle {
    bundle(
        "export default {
          async tail(event) {
            await Peren.storage.transaction(async (storage) => {
              await storage.put('counter', new Uint8Array([event.events[0].wallTimeMs]));
            });
          },
          async fetch() {
            const value = await Peren.storage.get('counter');
            return new Response(String(value?.[0] ?? 0));
          }
        };",
    )
}

pub(crate) fn workflow_bundle() -> WorkerBundle {
    bundle(
        "export default {
          async workflow(event, step) {
            const value = await step.do('charge', async () => {
              const current = await Peren.storage.get('counter');
              const next = current === undefined ? 1 : current[0] + 1;
              await Peren.storage.transaction(async (storage) => {
                await storage.put('counter', new Uint8Array([next]));
              });
              return event.payload.amount;
            });
            await Peren.storage.transaction(async (storage) => {
              await storage.put('last', new Uint8Array([value]));
            });
          },
          async fetch() {
            const counter = await Peren.storage.get('counter');
            const last = await Peren.storage.get('last');
            return new Response(`${counter?.[0] ?? 0}:${last?.[0] ?? 0}`);
          }
        };",
    )
}

pub(crate) fn request(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("https://worker.invalid{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

pub(crate) fn isolate() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

pub(crate) fn invocation() -> InvocationLimits {
    InvocationLimits::new(1024, 10)
}

#[cfg(test)]
mod event {
    use super::*;
    use object_store::memory::InMemory;
    use peren_runtime::{QueueMetrics, WorkerLogLevel};
    use std::time::Duration;
    use std::{fs, sync::Arc};

    #[tokio::test]
    async fn direct_event_dispatch_exposes_console_logs() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let node = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("node"),
            MemoryStore::default(),
        );
        let cell = CellId::from_bytes([21; 32]);
        let bundle = event_log_bundle();

        let alarm = node
            .dispatch_alarm_with_logs(cell, bundle.clone(), isolate())
            .await
            .unwrap();
        assert_log(&alarm.logs, WorkerLogLevel::Warn, "alarm fired");

        let scheduled = node
            .dispatch_scheduled_with_logs(
                cell,
                ScheduledEvent {
                    scheduled_time_ms: 7_000,
                    cron: "*/5 * * * *".into(),
                },
                bundle.clone(),
                isolate(),
            )
            .await
            .unwrap();
        assert_log(
            &scheduled.logs,
            WorkerLogLevel::Info,
            "scheduled */5 * * * *",
        );

        let tail = node
            .dispatch_tail_with_logs(
                cell,
                TailEvent {
                    events: vec![peren_runtime::TailRecord {
                        outcome: "ok".into(),
                        script: "worker".into(),
                        wall_time_ms: 11,
                    }],
                },
                bundle.clone(),
                isolate(),
            )
            .await
            .unwrap();
        assert_log(&tail.logs, WorkerLogLevel::Debug, "tail ok");

        let workflow = node
            .dispatch_workflow_with_logs(
                cell,
                WorkflowEvent {
                    instance: "order-1".into(),
                    payload: serde_json::json!({ "amount": 42 }),
                },
                bundle,
                isolate(),
            )
            .await
            .unwrap();
        assert_log(&workflow.logs, WorkerLogLevel::Error, "workflow order-1");

        fs::remove_dir_all(root).unwrap();
    }

    fn event_log_bundle() -> WorkerBundle {
        bundle(
            "export default {
              async alarm() { console.warn('alarm fired'); },
              async scheduled(event) { console.info('scheduled', event.cron); },
              async tail(event) { console.debug('tail', event.events[0].outcome); },
              async workflow(event) { console.error('workflow', event.instance); }
            };",
        )
    }

    fn assert_log(logs: &[peren_runtime::WorkerLogEvent], level: WorkerLogLevel, message: &str) {
        assert!(
            logs.iter()
                .any(|log| log.level == level && log.message == message),
            "{logs:?}"
        );
    }

    #[tokio::test]
    async fn alarm_write_survives_activation_on_another_node() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let cell = CellId::from_bytes([12; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("b"), store);

        first
            .dispatch_alarm(cell, alarm_bundle(), isolate())
            .await
            .unwrap();
        let restored = second
            .dispatch_http(
                cell,
                request("/read"),
                alarm_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(restored.body, b"1");
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn overlapping_alarm_dispatch_is_refused_for_the_same_cell() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let cell = CellId::from_bytes([17; 32]);
        let node = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("node"), store);

        let running = node.dispatch_alarm(cell, slow_alarm_bundle(), isolate());
        let overlapping = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            node.dispatch_alarm(cell, slow_alarm_bundle(), isolate())
                .await
        };

        let (running, overlap) = tokio::join!(running, overlapping);

        assert!(
            matches!(overlap.unwrap_err(), NodeError::AlarmOverlap(blocked) if blocked == cell)
        );
        running.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn tail_write_survives_activation_on_another_node() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let cell = CellId::from_bytes([15; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("b"), store);

        first
            .dispatch_tail(
                cell,
                TailEvent {
                    events: vec![peren_runtime::TailRecord {
                        outcome: "ok".into(),
                        script: "worker".into(),
                        wall_time_ms: 11,
                    }],
                },
                tail_bundle(),
                isolate(),
            )
            .await
            .unwrap();
        let restored = second
            .dispatch_http(
                cell,
                request("/read"),
                tail_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(restored.body, b"11");
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn queue_write_survives_activation_on_another_node() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let cell = CellId::from_bytes([14; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("b"), store);

        first
            .dispatch_queue(
                cell,
                QueueEvent {
                    metrics: QueueMetrics::default(),
                    queue: "jobs".into(),
                    messages: vec![peren_runtime::QueueMessage {
                        id: "message-1".into(),
                        body: vec![9],
                        attempts: 1,
                        timestamp: 1_700_000_000_000,
                    }],
                },
                queue_bundle(),
                isolate(),
            )
            .await
            .unwrap();
        let restored = second
            .dispatch_http(
                cell,
                request("/read"),
                queue_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(restored.body, b"9");
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn workflow_step_journal_survives_activation_on_another_node() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let cell = CellId::from_bytes([16; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("b"), store);
        let event = WorkflowEvent {
            instance: "order-1".into(),
            payload: serde_json::json!({ "amount": 42 }),
        };

        first
            .dispatch_workflow(cell, event.clone(), workflow_bundle(), isolate())
            .await
            .unwrap();
        second
            .dispatch_workflow(cell, event, workflow_bundle(), isolate())
            .await
            .unwrap();
        let restored = second
            .dispatch_http(
                cell,
                request("/read"),
                workflow_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(restored.body, b"1:42");
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn scheduled_write_survives_activation_on_another_node() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let cell = CellId::from_bytes([13; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("b"), store);

        first
            .dispatch_scheduled(
                cell,
                ScheduledEvent {
                    scheduled_time_ms: 7_000,
                    cron: "*/5 * * * *".into(),
                },
                scheduled_bundle(),
                isolate(),
            )
            .await
            .unwrap();
        let restored = second
            .dispatch_http(
                cell,
                request("/read"),
                scheduled_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(restored.body, b"7");
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod receipt {
    use super::*;
    use peren_cell::CellState;
    use std::fs;

    #[tokio::test]
    async fn acknowledged_write_survives_on_another_node() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let cell = CellId::from_bytes([7; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("b"),
            store.clone(),
        );

        let written = first
            .dispatch_http(
                cell,
                request("/increment"),
                counter_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();
        let restored = second
            .dispatch_http(
                cell,
                request("/read"),
                counter_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(written.body, b"1");
        assert_eq!(restored.body, b"1");
        assert_eq!(store.replica(cell).await.unwrap().0, OwnershipEpoch::new(1));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn recovery_fences_the_stale_owner() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let cell_id = CellId::from_bytes([9; 32]);
        let first_id = NodeId::from_uuid(Uuid::new_v4());
        let second_id = NodeId::from_uuid(Uuid::new_v4());
        let first_path = root.join("a.sqlite");
        let second_path = root.join("b.sqlite");
        fs::create_dir_all(&root).unwrap();

        let first_lease = store.acquire(first_id, cell_id).await.unwrap();
        let mut stale = WorkerCell::activate(
            &first_path,
            first_lease,
            store.clone(),
            counter_bundle(),
            isolate(),
            WorkerEnvironment::empty(),
        )
        .await
        .unwrap();
        stale
            .dispatch_http(request("/increment"), invocation())
            .await
            .unwrap();

        let second_lease = store
            .recover(
                second_id,
                cell_id,
                recovery(first_id, OwnershipEpoch::new(1)),
            )
            .await
            .unwrap();
        let replica = store.restore(cell_id).await.unwrap().unwrap();
        fs::write(&second_path, replica.database).unwrap();
        if !replica.wal.is_empty() {
            fs::write(format!("{}-wal", second_path.display()), replica.wal).unwrap();
        }
        let mut recovered = WorkerCell::activate(
            &second_path,
            second_lease,
            store.clone(),
            counter_bundle(),
            isolate(),
            WorkerEnvironment::empty(),
        )
        .await
        .unwrap();

        assert_eq!(
            recovered
                .dispatch_http(request("/read"), invocation())
                .await
                .unwrap()
                .body,
            b"1"
        );
        assert!(
            stale
                .dispatch_http(request("/increment"), invocation())
                .await
                .is_err()
        );
        assert_eq!(stale.state(), CellState::Fenced);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn dispatch_http_after_receipt_refuses_unsatisfied_replica() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let node_id = NodeId::from_uuid(Uuid::new_v4());
        let cell = CellId::from_bytes([43; 32]);
        let node = Node::new(node_id, root.clone(), store.clone());
        store.acquire(node_id, cell).await.unwrap();
        store
            .publish_through(
                cell,
                OwnershipEpoch::new(1),
                peren_primitives::StorageRevision::new(4),
                &peren_replication::ReplicaPayload {
                    generation: peren_primitives::StorageRevision::new(0),
                    database: Vec::new(),
                    wal_header: None,
                    wal_frames: Vec::new(),
                },
            )
            .await
            .unwrap();

        let error = node
            .dispatch_http_after_receipt(
                DurableReceipt::new(
                    cell,
                    OwnershipEpoch::new(1),
                    peren_primitives::StorageRevision::new(0),
                    peren_primitives::StorageRevision::new(5),
                ),
                request("/read"),
                counter_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, NodeError::ReceiptUnsatisfied));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn dispatch_http_after_receipt_serves_receipt_satisfying_state() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let cell = CellId::from_bytes([44; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("first"),
            store.clone(),
        );
        let second = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("second"),
            store,
        );

        first
            .dispatch_http(
                cell,
                request("/increment"),
                counter_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();
        let receipt = DurableReceipt::new(
            cell,
            OwnershipEpoch::new(1),
            peren_primitives::StorageRevision::new(0),
            peren_primitives::StorageRevision::new(1),
        );

        let response = second
            .dispatch_http_after_receipt(
                receipt,
                request("/read"),
                counter_bundle(),
                isolate(),
                invocation(),
            )
            .await
            .unwrap();

        assert_eq!(response.body, b"1");
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn node_restore_receipt_writes_only_receipt_satisfying_replica() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let node_id = NodeId::from_uuid(Uuid::new_v4());
        let cell = CellId::from_bytes([41; 32]);
        let node = Node::new(node_id, root.clone(), store.clone());
        store.acquire(node_id, cell).await.unwrap();
        store
            .publish_through(
                cell,
                OwnershipEpoch::new(1),
                peren_primitives::StorageRevision::new(4),
                &peren_replication::ReplicaPayload {
                    generation: peren_primitives::StorageRevision::new(0),
                    database: b"database".to_vec(),
                    wal_header: None,
                    wal_frames: Vec::new(),
                },
            )
            .await
            .unwrap();

        let restored = node
            .restore_receipt(DurableReceipt::new(
                cell,
                OwnershipEpoch::new(1),
                peren_primitives::StorageRevision::new(0),
                peren_primitives::StorageRevision::new(4),
            ))
            .await
            .unwrap();

        assert!(restored);
        assert_eq!(
            fs::read(root.join(format!("{cell}.sqlite"))).unwrap(),
            b"database"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn node_restore_receipt_refuses_stale_or_different_generation() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = MemoryStore::default();
        let node_id = NodeId::from_uuid(Uuid::new_v4());
        let cell = CellId::from_bytes([42; 32]);
        let node = Node::new(node_id, root.clone(), store.clone());
        store.acquire(node_id, cell).await.unwrap();
        store
            .publish_through(
                cell,
                OwnershipEpoch::new(1),
                peren_primitives::StorageRevision::new(4),
                &peren_replication::ReplicaPayload {
                    generation: peren_primitives::StorageRevision::new(0),
                    database: b"database".to_vec(),
                    wal_header: None,
                    wal_frames: Vec::new(),
                },
            )
            .await
            .unwrap();

        for receipt in [
            DurableReceipt::new(
                cell,
                OwnershipEpoch::new(1),
                peren_primitives::StorageRevision::new(0),
                peren_primitives::StorageRevision::new(5),
            ),
            DurableReceipt::new(
                cell,
                OwnershipEpoch::new(1),
                peren_primitives::StorageRevision::new(1),
                peren_primitives::StorageRevision::new(4),
            ),
            DurableReceipt::new(
                cell,
                OwnershipEpoch::new(2),
                peren_primitives::StorageRevision::new(0),
                peren_primitives::StorageRevision::new(4),
            ),
        ] {
            assert!(!node.restore_receipt(receipt).await.unwrap());
        }
        assert!(!root.join(format!("{cell}.sqlite")).exists());
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod restore {
    use super::*;
    use object_store::memory::InMemory;
    use std::{fs, sync::Arc};

    #[tokio::test]
    async fn object_store_backed_nodes_restore_acknowledged_state() {
        let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
        let store = BucketStore::new(Arc::new(InMemory::new()));
        let cell = CellId::from_bytes([11; 32]);
        let first = Node::new(
            NodeId::from_uuid(Uuid::new_v4()),
            root.join("a"),
            store.clone(),
        );
        let second = Node::new(NodeId::from_uuid(Uuid::new_v4()), root.join("b"), store);

        assert_eq!(
            first
                .dispatch_http(
                    cell,
                    request("/increment"),
                    counter_bundle(),
                    isolate(),
                    invocation()
                )
                .await
                .unwrap()
                .body,
            b"1"
        );
        assert_eq!(
            second
                .dispatch_http(
                    cell,
                    request("/read"),
                    counter_bundle(),
                    isolate(),
                    invocation()
                )
                .await
                .unwrap()
                .body,
            b"1"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
