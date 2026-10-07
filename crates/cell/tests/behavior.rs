use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use peren_cell::{CellError, CellState, LeaseError, OwnershipLease, WorkerCell};
use peren_primitives::{CellId, NodeId, OwnershipEpoch};
use peren_replication::{ReplicaImage, ReplicaPayload, ReplicaRepository, RepositoryError};
use peren_runtime::{
    HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName, QueueEvent,
    QueueMetrics, ScheduledEvent, TailEvent, WebSocketCloseEvent, WebSocketMessageEvent,
    WorkerBundle, WorkerEnvironment, WorkflowEvent,
};
use peren_storage::CellStorage;
use uuid::Uuid;

struct Lease {
    cell: CellId,
    owner: NodeId,
    checks: Arc<AtomicUsize>,
    fail_at: usize,
    releases: Option<Arc<AtomicUsize>>,
}

impl OwnershipLease for Lease {
    fn cell(&self) -> CellId {
        self.cell
    }

    fn owner(&self) -> NodeId {
        self.owner
    }

    fn epoch(&self) -> OwnershipEpoch {
        OwnershipEpoch::new(1)
    }

    async fn verify(&self) -> Result<(), LeaseError> {
        let check = self.checks.fetch_add(1, Ordering::SeqCst) + 1;
        (check < self.fail_at).then_some(()).ok_or(LeaseError)
    }

    async fn release(self) -> Result<(), LeaseError> {
        if let Some(releases) = self.releases {
            releases.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

struct Repository {
    reject_publication: bool,
    reject_checkpoint: bool,
    image: Arc<Mutex<Option<ReplicaImage>>>,
    prunes: Arc<AtomicUsize>,
}

impl ReplicaRepository for Repository {
    async fn publish_through(
        &self,
        _cell: CellId,
        epoch: OwnershipEpoch,
        revision: peren_primitives::StorageRevision,
        payload: &ReplicaPayload,
    ) -> Result<(), RepositoryError> {
        if self.reject_publication {
            Err(RepositoryError::Unavailable)
        } else {
            let mut wal = Vec::new();
            if let Some(header) = payload.wal_header {
                wal.extend_from_slice(&header);
                wal.extend_from_slice(&payload.wal_frames);
            }
            *self.image.lock().unwrap() = Some(ReplicaImage {
                epoch,
                generation: payload.generation,
                revision,
                database: payload.database.clone(),
                wal,
            });
            Ok(())
        }
    }

    async fn restore(&self, _cell: CellId) -> Result<Option<ReplicaImage>, RepositoryError> {
        Ok(self
            .image
            .lock()
            .unwrap()
            .as_ref()
            .map(|image| ReplicaImage {
                epoch: image.epoch,
                generation: image.generation,
                revision: image.revision,
                database: image.database.clone(),
                wal: image.wal.clone(),
            }))
    }

    async fn checkpoint(
        &self,
        _cell: CellId,
        _epoch: OwnershipEpoch,
        _revision: peren_primitives::StorageRevision,
        _database: &[u8],
    ) -> Result<(), RepositoryError> {
        if self.reject_checkpoint {
            Err(RepositoryError::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn prune(&self, _cell: CellId, _retain: NonZeroUsize) -> Result<usize, RepositoryError> {
        self.prunes.fetch_add(1, Ordering::SeqCst);
        Ok(0)
    }
}

fn lease(fail_at: usize) -> Lease {
    Lease {
        cell: CellId::from_bytes([1; 32]),
        owner: NodeId::from_uuid(Uuid::new_v4()),
        checks: Arc::new(AtomicUsize::new(0)),
        fail_at,
        releases: None,
    }
}

fn repository(reject_publication: bool) -> Repository {
    Repository {
        reject_publication,
        reject_checkpoint: false,
        image: Arc::new(Mutex::new(None)),
        prunes: Arc::new(AtomicUsize::new(0)),
    }
}

fn repository_rejecting_checkpoint() -> Repository {
    Repository {
        reject_publication: false,
        reject_checkpoint: true,
        image: Arc::new(Mutex::new(None)),
        prunes: Arc::new(AtomicUsize::new(0)),
    }
}

fn bundle(source: &str) -> WorkerBundle {
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

fn counter_bundle() -> WorkerBundle {
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

fn failing_bundle() -> WorkerBundle {
    bundle(
        "export default { async fetch(request) {
              if (new URL(request.url).pathname === '/fail') {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('counter', new Uint8Array([9]));
                  throw new Error('abort transaction');
                });
              }
              const value = await Peren.storage.get('counter');
              return new Response(String(value?.[0] ?? 0));
            } };",
    )
}

fn alarm_bundle() -> WorkerBundle {
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

fn scheduled_bundle() -> WorkerBundle {
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

fn queue_bundle() -> WorkerBundle {
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

fn tail_bundle() -> WorkerBundle {
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

fn workflow_bundle() -> WorkerBundle {
    bundle(
        "export default {
              async workflow(event) {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('counter', new Uint8Array([event.payload.value]));
                });
              },
              async fetch() {
                const value = await Peren.storage.get('counter');
                return new Response(String(value?.[0] ?? 0));
              }
            };",
    )
}

fn websocket_bundle() -> WorkerBundle {
    bundle(
        "export default {
              async webSocketMessage(_socket, message) {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('counter', new Uint8Array([message.charCodeAt(0)]));
                });
                return { outbound: [] };
              },
              async webSocketClose(_socket, code) {
                await Peren.storage.transaction(async (storage) => {
                  await storage.put('counter', new Uint8Array([code - 1000]));
                });
                return { outbound: [] };
              },
              async fetch() {
                const value = await Peren.storage.get('counter');
                return new Response(String(value?.[0] ?? 0));
              }
            };",
    )
}

fn request(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("https://worker.invalid{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

fn isolate() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

fn invocation() -> InvocationLimits {
    InvocationLimits::new(1024, 10)
}

fn restore_replica(path: &Path, image: &Arc<Mutex<Option<ReplicaImage>>>) {
    let replica = image.lock().unwrap().clone().unwrap();
    std::fs::write(path, replica.database).unwrap();
    if !replica.wal.is_empty() {
        std::fs::write(format!("{}-wal", path.display()), replica.wal).unwrap();
    }
}

#[path = "behavior/dispatch.rs"]
mod dispatch;
#[path = "behavior/ownership.rs"]
mod ownership;
