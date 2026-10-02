use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use peren_config::{QueueConsumer, QueueDefaults, Service};
use peren_primitives::CellId;
use peren_queues::{BatchWindow, Conclusion, Lease as QueueLease, Outcome, Policy, Shard};
use peren_runtime::{
    IsolateLimits, QueueDispatch, QueueDispositionKind, QueueEvent, QueueMessage, QueueMetrics,
    WorkerLogEvent,
};

use crate::{
    Node, Repository, Shutdown, Supervisor, TaskError, admission::Admission, metrics::Telemetry,
    tail, trace,
};

use crate::host::RoutedHost;

use super::{
    CacheStore, Limits, ObjectRegistry, ProcessContext, ProcessHost, QueueBrokerState,
    ServiceTarget, cell, turso_routes,
};

struct QueueDrainer {
    node: Node<Repository>,
    queues: QueueBrokerState,
    service: String,
    plan: QueueConsumerPlan,
    target: ServiceTarget,
    limits: Limits,
    telemetry: Arc<Telemetry>,
    admission: Admission,
    tail: PathBuf,
    trace: PathBuf,
    cache: CacheStore,
    services: Arc<BTreeMap<String, ServiceTarget>>,
    objects: Arc<BTreeMap<String, String>>,
    registry: ObjectRegistry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct QueueConsumerPlan {
    pub(super) queue: String,
    pub(super) limit: usize,
    pub(super) policy: Policy,
    pub(super) timeout: chrono::Duration,
    pub(super) concurrency: u16,
    pub(super) shard: Option<Shard>,
}

pub(super) fn queue_consumers(
    services: &[Service],
    defaults: Option<&QueueDefaults>,
) -> BTreeMap<String, Vec<QueueConsumerPlan>> {
    let defaults = defaults.cloned().unwrap_or_default();
    services
        .iter()
        .map(|service| {
            let plans = service
                .consumes_queues
                .iter()
                .map(|consumer| queue_consumer(consumer, &defaults))
                .collect();
            (service.name.clone(), plans)
        })
        .collect()
}

fn queue_consumer(consumer: &QueueConsumer, defaults: &QueueDefaults) -> QueueConsumerPlan {
    let (queue, settings) = match consumer {
        QueueConsumer::Name(queue) => (queue.clone(), defaults.clone()),
        QueueConsumer::Settings(settings) => (settings.queue.clone(), settings.resolve(defaults)),
    };
    QueueConsumerPlan {
        queue,
        limit: usize::from(settings.max_batch_size),
        timeout: chrono::Duration::seconds(
            settings
                .max_batch_timeout_secs
                .try_into()
                .unwrap_or(i64::MAX),
        ),
        concurrency: settings.max_concurrency,
        shard: None,
        policy: Policy {
            max_retries: settings.max_retries,
            retry_delay: chrono::Duration::seconds(
                settings.retry_delay_secs.try_into().unwrap_or(i64::MAX),
            ),
            dead_letter: settings.dead_letter_queue,
        },
    }
}

pub(super) fn consumer_shards(plan: &QueueConsumerPlan) -> Vec<QueueConsumerPlan> {
    if plan.concurrency <= 1 {
        return vec![plan.clone()];
    }
    (0..plan.concurrency)
        .map(|index| QueueConsumerPlan {
            shard: Some(
                Shard::new(index, plan.concurrency)
                    .expect("validated queue concurrency produces valid shard"),
            ),
            concurrency: 1,
            ..plan.clone()
        })
        .collect()
}

pub(super) fn spawn_queue_consumers(
    supervisor: &mut Supervisor,
    context: &ProcessContext,
    services: &Arc<BTreeMap<String, ServiceTarget>>,
    objects: &Arc<BTreeMap<String, String>>,
    registry: &ObjectRegistry,
) {
    for (service, consumers) in &context.consumers {
        let Some(target) = services.get(service).cloned() else {
            continue;
        };
        for plan in consumers.clone() {
            let shards = consumer_shards(&plan);
            for plan in shards {
                let suffix = plan
                    .shard
                    .map(|shard| format!("-{}-of-{}", shard.index + 1, shard.total))
                    .unwrap_or_default();
                let name = format!("queue-{service}-{}{suffix}", plan.queue);
                let node = Node::new(
                    context.node,
                    context.data.join("cells"),
                    context.repository.clone(),
                );
                let queues = context.queues.clone();
                let service = service.clone();
                let limits = context.limits;
                let target = target.clone();
                let telemetry = Arc::clone(&context.telemetry);
                let admission = context.admission.clone();
                let tail = context.data.join("tail");
                let trace = context.data.join("traces");
                let drainer = QueueDrainer {
                    node,
                    queues,
                    service,
                    plan,
                    target,
                    limits,
                    telemetry,
                    admission,
                    tail,
                    trace,
                    cache: context.cache.clone(),
                    services: Arc::clone(services),
                    objects: Arc::clone(objects),
                    registry: Arc::clone(registry),
                };
                supervisor.spawn(name, false, move |shutdown| async move {
                    consume_queue(shutdown, drainer).await
                });
            }
        }
    }
}

async fn consume_queue(shutdown: Shutdown, drainer: QueueDrainer) -> Result<(), TaskError> {
    tokio::task::spawn_blocking(move || queue_thread(shutdown, drainer))
        .await
        .map_err(|error| TaskError::new(error.to_string()))?
}

fn queue_thread(shutdown: Shutdown, drainer: QueueDrainer) -> Result<(), TaskError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| TaskError::new(error.to_string()))?;
    runtime.block_on(queue_loop(
        shutdown,
        drainer.queues,
        drainer.node,
        drainer.service,
        drainer.plan,
        drainer.target,
        drainer.limits,
        drainer.telemetry,
        drainer.admission,
        drainer.tail,
        drainer.trace,
        drainer.cache,
        drainer.services,
        drainer.objects,
        drainer.registry,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "queue loop keeps process dependencies explicit"
)]
async fn queue_loop(
    mut shutdown: Shutdown,
    queues: QueueBrokerState,
    node: Node<Repository>,
    service: String,
    plan: QueueConsumerPlan,
    target: ServiceTarget,
    limits: Limits,
    telemetry: Arc<Telemetry>,
    admission: Admission,
    tail: PathBuf,
    trace_path: PathBuf,
    cache: CacheStore,
    services: Arc<BTreeMap<String, ServiceTarget>>,
    objects: Arc<BTreeMap<String, String>>,
    registry: ObjectRegistry,
) -> Result<(), TaskError> {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            () = shutdown.wait() => return Ok(()),
            _ = interval.tick() => {
                queue_tick_inner(
                    node.clone(),
                    queues.clone(),
                    service.clone(),
                    plan.clone(),
                    target.clone(),
                    limits,
                    Arc::clone(&telemetry),
                    admission.clone(),
                    tail.clone(),
                    trace_path.clone(),
                    cache.clone(),
                    Arc::clone(&services),
                    Arc::clone(&objects),
                    Arc::clone(&registry),
                )
                .await?;
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "queue tick uses explicit dependencies for testability"
)]
async fn queue_tick_inner(
    node: Node<Repository>,
    queues: QueueBrokerState,
    service: String,
    plan: QueueConsumerPlan,
    target: ServiceTarget,
    limits: Limits,
    telemetry: Arc<Telemetry>,
    admission: Admission,
    tail: PathBuf,
    trace_path: PathBuf,
    cache: CacheStore,
    services: Arc<BTreeMap<String, ServiceTarget>>,
    objects: Arc<BTreeMap<String, String>>,
    registry: ObjectRegistry,
) -> Result<(), TaskError> {
    Telemetry::inc(&telemetry.queue_ticks);
    let now = chrono::Utc::now();
    let stats = queues
        .stats(&plan.queue, now)
        .await
        .map_err(|error| TaskError::new(error.to_string()))?;
    if !BatchWindow::new(plan.limit, plan.timeout).ready(&stats, now) {
        return Ok(());
    }
    let batch = queues
        .lease_batch(&plan.queue, plan.limit, now, plan.shard)
        .await
        .map_err(|error| TaskError::new(error.to_string()))?;
    if batch.is_empty() {
        return Ok(());
    }
    Telemetry::inc(&telemetry.queue_leases);
    Telemetry::add(&telemetry.queue_messages, batch.len());
    let event = queue_event(&batch);
    let leases = batch.into_leases();
    let Ok(_permit) = admission.admit() else {
        Telemetry::inc(&telemetry.queue_errors);
        conclude_queue(&queues, leases, Outcome::Retry, &plan.policy)
            .await
            .map_err(|error| TaskError::new(error.to_string()))?;
        return Ok(());
    };
    let started = Instant::now();
    let queue_cell = cell(&service, &format!("/__queue/{}", plan.queue));
    let dispatch = match dispatch_queue_with_host(
        node,
        queues.clone(),
        queue_cell,
        event,
        target,
        limits,
        Arc::clone(&telemetry),
        cache,
        trace_path,
        services,
        objects,
        registry,
    )
    .await
    {
        Ok(dispatch) => dispatch,
        Err(error) => {
            let outcome = if error.is_non_retryable() {
                Outcome::Fail
            } else {
                Outcome::Retry
            };
            Telemetry::inc(&telemetry.queue_errors);
            Telemetry::observe(
                &telemetry.queue_dispatch_duration_ms,
                &telemetry.queue_dispatch_duration,
                started,
            );
            conclude_queue(&queues, leases, outcome, &plan.policy)
                .await
                .map_err(|error| TaskError::new(error.to_string()))?;
            return Ok(());
        }
    };
    Telemetry::inc(&telemetry.queue_dispatches);
    Telemetry::observe(
        &telemetry.queue_dispatch_duration_ms,
        &telemetry.queue_dispatch_duration,
        started,
    );
    record_console_logs(&tail, &service, queue_cell, &dispatch.logs)
        .map_err(|error| TaskError::new(error.to_string()))?;
    conclude_queue_dispatched(
        &queues,
        leases,
        dispatch.dispatch.dispositions,
        &plan.policy,
    )
    .await
    .map_err(|error| TaskError::new(error.to_string()))
}

fn record_console_logs(
    tail_path: &Path,
    service: &str,
    cell: peren_primitives::CellId,
    logs: &[peren_runtime::WorkerLogEvent],
) -> Result<(), tail::TailError> {
    let request_id = uuid::Uuid::new_v4().to_string();
    for log in logs {
        tail::append(
            tail_path,
            &tail::Event::Console(tail::ConsoleEvent {
                service: service.to_string(),
                cell: Some(cell.to_string()),
                request_id: Some(request_id.clone()),
                event: "queue".into(),
                level: match log.level {
                    peren_runtime::WorkerLogLevel::Debug => tail::ConsoleLevel::Debug,
                    peren_runtime::WorkerLogLevel::Info => tail::ConsoleLevel::Info,
                    peren_runtime::WorkerLogLevel::Warn => tail::ConsoleLevel::Warn,
                    peren_runtime::WorkerLogLevel::Error => tail::ConsoleLevel::Error,
                },
                message: log.message.clone(),
                timestamp_ms: log.timestamp_ms,
            }),
        )?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "queue dispatch mirrors listener capability wiring"
)]
async fn dispatch_queue_with_host(
    node: Node<Repository>,
    queues: QueueBrokerState,
    cell: CellId,
    event: QueueEvent,
    target: ServiceTarget,
    limits: Limits,
    telemetry: Arc<Telemetry>,
    cache: CacheStore,
    trace_path: PathBuf,
    services: Arc<BTreeMap<String, ServiceTarget>>,
    objects: Arc<BTreeMap<String, String>>,
    registry: ObjectRegistry,
) -> Result<QueueDispatchResult, QueueDispatchError> {
    let d1 = turso_routes(&target.d1)
        .await
        .map_err(|error| QueueDispatchError::Setup(error.to_string()))?;
    let r2 = target.r2.clone();
    let kv = target.kv.clone();
    let outbound_hosts = Arc::clone(&target.outbound_hosts);
    let aws = Arc::clone(&target.aws);
    let mtls = Arc::clone(&target.mtls);
    let dispatch_queues = queues.clone();
    let host_node = node.clone();
    node.restore_and_dispatch(cell, |input| async move {
        let path = input.path;
        let lease = input.lease;
        let store = input.repository;
        let mut resident = peren_cell::WorkerCell::activate_with_capabilities(
            &path,
            lease,
            store,
            target.bundle,
            IsolateLimits::new(limits.heap, limits.execution),
            target.environment,
            move |storage| {
                Arc::new(ProcessHost {
                    inner: RoutedHost::new(
                        storage,
                        d1,
                        r2,
                        dispatch_queues.clone(),
                        cache.clone(),
                        kv,
                    ),
                    node: host_node,
                    services,
                    objects,
                    registry,
                    outbound_hosts,
                    aws,
                    mtls,
                    queues: dispatch_queues,
                    r2_notifications: target.r2_notifications,
                    cache,
                    limits,
                    telemetry,
                    trace: trace::TraceSink::local(trace_path),
                    trace_context: None,
                })
            },
        )
        .await?;
        let dispatch = resident.dispatch_queue(event).await?;
        resident
            .checkpoint_if_wal_exceeds(target.checkpoint_threshold_bytes)
            .await?;
        let logs = resident.take_console_events();
        resident.release().await?;
        Ok(QueueDispatchResult { dispatch, logs })
    })
    .await
    .map_err(QueueDispatchError::Node)
}

struct QueueDispatchResult {
    dispatch: QueueDispatch,
    logs: Vec<WorkerLogEvent>,
}

enum QueueDispatchError {
    Node(crate::NodeError),
    Setup(String),
}

impl QueueDispatchError {
    fn is_non_retryable(&self) -> bool {
        matches!(self, Self::Node(error) if error.is_non_retryable())
    }
}

impl fmt::Display for QueueDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(error) => write!(formatter, "{error}"),
            Self::Setup(error) => formatter.write_str(error),
        }
    }
}

fn queue_event(batch: &peren_queues::Batch) -> QueueEvent {
    QueueEvent {
        queue: batch.queue.clone(),
        metrics: QueueMetrics {
            ready: batch.metrics.ready,
            delayed: batch.metrics.delayed,
            leased: batch.metrics.leased,
            oldest_ready_timestamp: batch
                .metrics
                .oldest_ready_at
                .map(|value| value.timestamp_millis()),
        },
        messages: batch
            .leases
            .iter()
            .map(|lease| QueueMessage {
                id: lease.message.id.to_string(),
                body: lease.message.body.clone(),
                attempts: u32::from(lease.message.attempts),
                timestamp: lease.message.available_at.timestamp_millis(),
            })
            .collect(),
    }
}

async fn conclude_queue_dispatched(
    queues: &QueueBrokerState,
    leases: Vec<QueueLease>,
    dispositions: Vec<peren_runtime::QueueDisposition>,
    policy: &Policy,
) -> Result<(), peren_queues::QueueError> {
    let mut outcomes = HashMap::new();
    for disposition in dispositions {
        outcomes.insert(
            disposition.id,
            (
                match disposition.outcome {
                    QueueDispositionKind::Ack => Outcome::Ack,
                    QueueDispositionKind::Retry => Outcome::Retry,
                },
                disposition
                    .delay_seconds
                    .map(i64::from)
                    .map(chrono::Duration::seconds),
            ),
        );
    }
    for lease in leases {
        let (outcome, delay) = outcomes
            .remove(&lease.message.id.to_string())
            .unwrap_or((Outcome::Ack, None));
        let _conclusion: Conclusion = queues
            .conclude_with_delay(lease.id, outcome, policy, chrono::Utc::now(), delay)
            .await?;
    }
    Ok(())
}

async fn conclude_queue(
    queues: &QueueBrokerState,
    leases: Vec<QueueLease>,
    outcome: Outcome,
    policy: &Policy,
) -> Result<(), peren_queues::QueueError> {
    for lease in leases {
        let _conclusion: Conclusion = queues
            .conclude_with_delay(lease.id, outcome, policy, chrono::Utc::now(), None)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_consumers_resolve_defaults_and_overrides() {
        let services = vec![peren_config::Service {
            name: "api".into(),
            worker_bundle_path: std::path::PathBuf::from("worker.js"),
            compatibility_date: "2026-01-01".into(),
            compatibility_flags: Vec::new(),
            entrypoint: peren_config::Entrypoint::Stateless,
            cron_triggers: Vec::new(),
            tail_consumers: Vec::new(),
            consumes_queues: vec![
                QueueConsumer::Name("plain".into()),
                QueueConsumer::Settings(peren_config::QueueSettings {
                    queue: "custom".into(),
                    overrides: peren_config::QueueOverrides {
                        max_batch_size: Some(9),
                        max_retries: Some(1),
                        max_batch_timeout_secs: Some(2),
                        dead_letter_queue: Some("custom-dead".into()),
                        ..Default::default()
                    },
                }),
            ],
            bindings: std::collections::BTreeMap::new(),
            vars: std::collections::BTreeMap::new(),
            expose_node_id: false,
            secrets: std::collections::BTreeMap::new(),
            secrets_store_refs: std::collections::BTreeMap::new(),
            additional_modules: std::collections::BTreeMap::new(),
            source_maps: std::collections::BTreeMap::new(),
            assets: None,
            isolate_fair_share_percent: None,
            checkpoint_threshold_bytes: None,
            max_cpu_time_ms: None,
            max_subrequests_per_invocation: None,
            max_heap_bytes: None,
            max_execution_time_ms: None,
            workflow_retention_days: None,
            deploy_max_resident_age_secs: None,
            max_steps_per_instance: None,
            tenant_id: None,
            project_id: None,
            placement_regions: Vec::new(),
        }];
        let defaults = QueueDefaults {
            max_batch_size: 25,
            max_batch_timeout_secs: 10,
            max_retries: 3,
            max_concurrency: 4,
            dead_letter_queue: Some("dead".into()),
            retry_delay_secs: 7,
        };

        let consumers = queue_consumers(&services, Some(&defaults));

        assert_eq!(consumers["api"][0].queue, "plain");
        assert_eq!(consumers["api"][0].limit, 25);
        assert_eq!(consumers["api"][0].concurrency, 4);
        assert_eq!(consumers["api"][0].timeout, chrono::Duration::seconds(10));
        assert_eq!(consumers["api"][0].policy.dead_letter, Some("dead".into()));
        assert_eq!(consumers["api"][1].queue, "custom");
        assert_eq!(consumers["api"][1].limit, 9);
        assert_eq!(consumers["api"][1].concurrency, 4);
        assert_eq!(consumers["api"][1].timeout, chrono::Duration::seconds(2));
        assert_eq!(consumers["api"][1].policy.max_retries, 1);
        assert_eq!(
            consumers["api"][1].policy.dead_letter,
            Some("custom-dead".into())
        );

        let shards = consumer_shards(&consumers["api"][0]);
        assert_eq!(shards.len(), 4);
        assert_eq!(shards[0].shard.unwrap().index, 0);
        assert_eq!(shards[3].shard.unwrap().index, 3);
        assert!(shards.iter().all(|plan| plan.shard.unwrap().total == 4));
    }
}
