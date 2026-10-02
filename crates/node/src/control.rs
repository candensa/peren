#[cfg(test)]
use std::collections::BTreeMap;
use std::{
    path::PathBuf,
    sync::atomic::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use axum::{
    Json,
    body::Body,
    extract::Path as AxumPath,
    http::{Method, Response, StatusCode},
};
use axum::{
    Json as AxumJson,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode as AxumStatusCode, Uri, header},
    response::IntoResponse,
};
#[cfg(test)]
use peren_cell::OwnershipLease;
#[cfg(test)]
use peren_replication::ReplicaRepository;
#[cfg(test)]
use peren_runtime::{
    IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle, WorkerEnvironment,
};
#[cfg(test)]
use peren_storage::ListOptions as StorageListOptions;
#[cfg(test)]
use serde::Deserialize;
#[cfg(test)]
use serde_json::Value as JsonValue;

use crate::Environment;
#[cfg(test)]
use crate::{
    Repository,
    process::{self, ObjectRegistry, ProcessError, SocketApp},
};
use crate::{admission::Mode as AdmissionMode, process::App};

#[derive(serde::Deserialize)]
pub(super) struct DevEventsQuery {
    #[serde(default = "default_event_limit")]
    limit: usize,
}

const fn default_event_limit() -> usize {
    100
}

pub(super) async fn dev_overview(State(app): State<App>) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    AxumJson(serde_json::json!({
        "ready": app.readiness.load(Ordering::Acquire),
        "node": app.node.as_uuid(),
        "incarnation": app.incarnation,
        "topology": dev_topology_json(&app),
        "health": dev_health_json(&app),
    }))
    .into_response()
}

pub(super) async fn dev_topology(State(app): State<App>) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    AxumJson(dev_topology_json(&app)).into_response()
}

pub(super) async fn dev_events(
    State(app): State<App>,
    Query(query): Query<DevEventsQuery>,
) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    match crate::tail::read_recent(&app.data.join("tail"), query.limit.min(1_000)) {
        Ok(report) => AxumJson(serde_json::json!({ "events": report.events })).into_response(),
        Err(error) => (
            AxumStatusCode::INTERNAL_SERVER_ERROR,
            AxumJson(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub(super) async fn dev_bindings(State(app): State<App>) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    AxumJson(serde_json::json!({ "services": dev_bindings_json(&app) })).into_response()
}

pub(super) async fn dev_queues(State(app): State<App>) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    AxumJson(serde_json::json!({ "queues": dev_queues_json(&app) })).into_response()
}

pub(super) async fn dev_objects(State(app): State<App>) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    let objects = app
        .socket
        .as_ref()
        .map(|socket| {
            socket
                .registry
                .lock()
                .map(|registry| {
                    registry
                        .iter()
                        .map(|(class, ids)| {
                            serde_json::json!({
                                "class": class,
                                "ids": ids.iter().collect::<Vec<_>>(),
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();
    AxumJson(serde_json::json!({ "objects": objects })).into_response()
}

pub(super) async fn dev_health(State(app): State<App>) -> impl IntoResponse {
    if !app.dev_inspector {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    AxumJson(dev_health_json(&app)).into_response()
}

fn dev_health_json(app: &App) -> serde_json::Value {
    let snapshot = app.admission.snapshot();
    serde_json::json!({
        "ready": app.readiness.load(Ordering::Acquire),
        "retired": app.retired.load(Ordering::Acquire),
        "admission": admission_name(snapshot.mode),
        "capacity": snapshot.capacity,
        "active": snapshot.active,
        "available": snapshot.available,
        "admitted": snapshot.admitted,
        "completed": snapshot.completed,
        "refused": snapshot.refused,
        "disk_removal_safe": snapshot.active == 0,
    })
}

fn dev_bindings_json(app: &App) -> Vec<serde_json::Value> {
    app.config
        .raw
        .services
        .iter()
        .map(|service| {
            let bindings = service
                .bindings
                .iter()
                .map(|(name, binding)| binding_json(name, binding))
                .collect::<Vec<_>>();
            serde_json::json!({
                "service": service.name,
                "bindings": bindings,
                "vars": service.vars.keys().collect::<Vec<_>>(),
                "secrets": service.secrets.keys().chain(service.secrets_store_refs.keys()).collect::<Vec<_>>(),
            })
        })
        .collect()
}

fn binding_json(name: &str, binding: &peren_config::Binding) -> serde_json::Value {
    let mut value = serde_json::json!({
        "name": name,
        "kind": binding_kind(binding),
    });
    if let Some(target) = binding_target(binding) {
        value["target"] = serde_json::Value::String(target);
    }
    value
}

fn binding_target(binding: &peren_config::Binding) -> Option<String> {
    match binding {
        peren_config::Binding::Service { entrypoint, .. } => Some(entrypoint.clone()),
        peren_config::Binding::DurableObjectNamespace { class_name, .. }
        | peren_config::Binding::Workflow { class_name, .. } => Some(class_name.clone()),
        peren_config::Binding::Kv { namespace, .. }
        | peren_config::Binding::Dispatcher { namespace } => Some(namespace.clone()),
        peren_config::Binding::D1Database { database_name, .. } => Some(database_name.clone()),
        peren_config::Binding::R2Bucket { bucket, .. } => Some(bucket.clone()),
        peren_config::Binding::Queue { queue_name } => Some(queue_name.clone()),
        _ => None,
    }
}

fn dev_queues_json(app: &App) -> Vec<serde_json::Value> {
    let producers = app
        .config
        .raw
        .services
        .iter()
        .flat_map(|service| {
            service
                .bindings
                .iter()
                .filter_map(|(binding, config)| match config {
                    peren_config::Binding::Queue { queue_name } => Some((
                        queue_name.clone(),
                        serde_json::json!({
                            "service": service.name,
                            "binding": binding,
                        }),
                    )),
                    _ => None,
                })
        })
        .fold(
            std::collections::BTreeMap::<String, Vec<serde_json::Value>>::new(),
            |mut queues, (queue, producer)| {
                queues.entry(queue).or_default().push(producer);
                queues
            },
        );
    let consumers = app
        .config
        .raw
        .services
        .iter()
        .flat_map(|service| {
            service.consumes_queues.iter().map(|consumer| {
                (
                    queue_name(consumer),
                    serde_json::json!({ "service": service.name }),
                )
            })
        })
        .fold(
            std::collections::BTreeMap::<String, Vec<serde_json::Value>>::new(),
            |mut queues, (queue, consumer)| {
                queues.entry(queue).or_default().push(consumer);
                queues
            },
        );
    producers
        .keys()
        .chain(consumers.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|queue| {
            serde_json::json!({
                "queue": queue,
                "producers": producers.get(queue).cloned().unwrap_or_default(),
                "consumers": consumers.get(queue).cloned().unwrap_or_default(),
            })
        })
        .collect()
}

fn queue_name(consumer: &peren_config::QueueConsumer) -> String {
    match consumer {
        peren_config::QueueConsumer::Name(name) => name.clone(),
        peren_config::QueueConsumer::Settings(settings) => settings.queue.clone(),
    }
}

fn dev_topology_json(app: &App) -> serde_json::Value {
    let services = app
        .config
        .raw
        .services
        .iter()
        .map(|service| {
            let bindings = service
                .bindings
                .iter()
                .map(|(name, binding)| {
                    serde_json::json!({
                        "name": name,
                        "kind": binding_kind(binding),
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "name": service.name,
                "worker": service.worker_bundle_path,
                "entrypoint": entrypoint_kind(&service.entrypoint),
                "bindings": bindings,
                "queue_consumers": queue_consumers(service),
                "tail_consumers": service.tail_consumers.iter().map(|tail| tail.service.clone()).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    let sockets = app
        .config
        .raw
        .sockets
        .iter()
        .map(|socket| {
            serde_json::json!({
                "name": socket.name,
                "service": socket.service,
                "listen": socket.listen,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "services": services,
        "sockets": sockets,
        "bucket": format!("{:?}", app.config.raw.bucket.kind).to_lowercase(),
        "data": app.data,
    })
}

fn queue_consumers(service: &peren_config::Service) -> Vec<String> {
    service
        .consumes_queues
        .iter()
        .map(|consumer| match consumer {
            peren_config::QueueConsumer::Name(name) => name.clone(),
            peren_config::QueueConsumer::Settings(settings) => settings.queue.clone(),
        })
        .collect()
}

fn entrypoint_kind(entrypoint: &peren_config::Entrypoint) -> &'static str {
    match entrypoint {
        peren_config::Entrypoint::Stateless => "stateless",
        peren_config::Entrypoint::DurableObject { .. } => "durable_object",
    }
}

fn binding_kind(binding: &peren_config::Binding) -> &'static str {
    match binding {
        peren_config::Binding::Ai { .. } => "ai",
        peren_config::Binding::AnalyticsEngine { .. } => "analytics_engine",
        peren_config::Binding::AwsSigv4 { .. } => "aws_sigv4",
        peren_config::Binding::Container { .. } => "container",
        peren_config::Binding::D1Database { .. } => "d1",
        peren_config::Binding::Dispatcher { .. } => "dispatcher",
        peren_config::Binding::DurableObjectNamespace { .. } => "durable_object_namespace",
        peren_config::Binding::Hyperdrive { .. } => "hyperdrive",
        peren_config::Binding::Images { .. } => "images",
        peren_config::Binding::Kv { .. } => "kv",
        peren_config::Binding::Loader => "loader",
        peren_config::Binding::MtlsCertificate { .. } => "mtls_certificate",
        peren_config::Binding::Queue { .. } => "queue",
        peren_config::Binding::R2Bucket { .. } => "r2",
        peren_config::Binding::RateLimiter { .. } => "rate_limiter",
        peren_config::Binding::SecretsStoreSecret { .. } => "secret",
        peren_config::Binding::Service { .. } => "service",
        peren_config::Binding::Vectorize { .. } => "vectorize",
        peren_config::Binding::Outbound { .. } => "outbound",
        peren_config::Binding::Workflow { .. } => "workflow",
        peren_config::Binding::Assets => "assets",
    }
}

#[cfg(test)]
#[derive(Deserialize)]
pub(super) struct TestInvocation {
    method: String,
    #[serde(default)]
    args: Vec<JsonValue>,
}

#[derive(serde::Serialize)]
struct NodeControlReport {
    node: String,
    listener: String,
    ready: bool,
    admission: &'static str,
    active: u64,
    incarnation: String,
    disk_removal_safe: bool,
    retired: bool,
}

#[derive(serde::Deserialize)]
pub(super) struct DeploymentQuery {
    service: Option<String>,
}

#[derive(serde::Deserialize)]
pub(super) struct DeploymentRecordRequest {
    #[serde(default = "default_percent")]
    percent: u8,
    #[serde(default)]
    preview: bool,
}

const fn default_percent() -> u8 {
    100
}

struct ControlEnvironment {
    data: PathBuf,
}

impl Environment for ControlEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        (name == "PEREN_DATA_DIR").then(|| self.data.display().to_string())
    }
}

pub(super) async fn deployment_record(
    State(app): State<App>,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    if !is_peer(&app) {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    if let Err(error) = authorize_control_mutation(&app, &headers, "POST", &uri, &body) {
        return control_auth_error(error.message());
    }
    let request = match serde_json::from_slice::<DeploymentRecordRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                AxumStatusCode::BAD_REQUEST,
                AxumJson(serde_json::json!({ "error": error.to_string() })),
            )
                .into_response();
        }
    };
    if request.percent > 100 {
        return (
            AxumStatusCode::BAD_REQUEST,
            AxumJson(serde_json::json!({ "error": "percent must be between 0 and 100" })),
        )
            .into_response();
    }
    let environment = ControlEnvironment {
        data: app.data.clone(),
    };
    let deployment = crate::Deployment::new(&app.config, &environment);
    match deployment.record(&crate::DeployRecord {
        percent: request.percent,
        preview: request.preview,
    }) {
        Ok(report) => {
            AxumJson(serde_json::json!({ "generations": report.generations })).into_response()
        }
        Err(error) => deployment_error(&error),
    }
}

pub(super) async fn deployment_list(
    State(app): State<App>,
    Query(query): Query<DeploymentQuery>,
) -> impl IntoResponse {
    if !is_peer(&app) {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    let environment = ControlEnvironment {
        data: app.data.clone(),
    };
    let deployment = crate::Deployment::new(&app.config, &environment);
    match deployment.list(&crate::DeployList {
        service: query.service,
    }) {
        Ok(report) => {
            AxumJson(serde_json::json!({ "generations": report.generations })).into_response()
        }
        Err(error) => deployment_error(&error),
    }
}

pub(super) async fn deployment_verify(
    State(app): State<App>,
    Query(query): Query<DeploymentQuery>,
) -> impl IntoResponse {
    if !is_peer(&app) {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    let environment = ControlEnvironment {
        data: app.data.clone(),
    };
    let deployment = crate::Deployment::new(&app.config, &environment);
    match deployment.verify(&crate::DeployVerify {
        service: query.service,
    }) {
        Ok(report) => {
            AxumJson(serde_json::json!({ "generations": report.generations })).into_response()
        }
        Err(error) => deployment_error(&error),
    }
}

pub(super) async fn deployment_health(
    State(app): State<App>,
    Query(query): Query<DeploymentQuery>,
) -> impl IntoResponse {
    if !is_peer(&app) {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    let environment = ControlEnvironment {
        data: app.data.clone(),
    };
    let deployment = crate::Deployment::new(&app.config, &environment);
    match deployment.health(&crate::DeployHealth {
        service: query.service,
    }) {
        Ok(report) => AxumJson(serde_json::json!({
            "services": report.services.into_iter().map(|service| serde_json::json!({
                "service": service.service,
                "digest": service.digest,
                "percent": service.percent,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => deployment_error(&error),
    }
}

fn deployment_error(error: &crate::DeployError) -> axum::response::Response {
    (
        AxumStatusCode::CONFLICT,
        AxumJson(serde_json::json!({ "error": error.to_string() })),
    )
        .into_response()
}

pub(super) async fn node_status(State(app): State<App>) -> impl IntoResponse {
    match peer_report(&app) {
        Some(report) => (AxumStatusCode::OK, AxumJson(report)).into_response(),
        None => AxumStatusCode::NOT_FOUND.into_response(),
    }
}

pub(super) async fn node_drain(
    State(app): State<App>,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    if !is_peer(&app) {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    if let Err(error) = authorize_control_mutation(&app, &headers, "POST", &uri, &body) {
        return control_auth_error(error.message());
    }
    app.readiness.store(false, Ordering::Release);
    app.admission.set_mode(AdmissionMode::Draining);
    let report = peer_report(&app).expect("peer listener has a control report");
    (AxumStatusCode::OK, AxumJson(report)).into_response()
}

pub(super) async fn node_control_only(
    State(app): State<App>,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    if !is_peer(&app) {
        return AxumStatusCode::NOT_FOUND.into_response();
    }
    if let Err(error) = authorize_control_mutation(&app, &headers, "POST", &uri, &body) {
        return control_auth_error(error.message());
    }
    app.readiness.store(false, Ordering::Release);
    app.admission.set_mode(AdmissionMode::ControlOnly);
    let report = peer_report(&app).expect("peer listener has a control report");
    (AxumStatusCode::OK, AxumJson(report)).into_response()
}

pub(super) async fn node_retire(
    State(app): State<App>,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    let Some(report) = peer_report(&app) else {
        return AxumStatusCode::NOT_FOUND.into_response();
    };
    if let Err(error) = authorize_control_mutation(&app, &headers, "POST", &uri, &body) {
        return control_auth_error(error.message());
    }
    if !report.disk_removal_safe {
        return (AxumStatusCode::CONFLICT, AxumJson(report)).into_response();
    }
    app.admission.set_mode(AdmissionMode::ControlOnly);
    app.retired.store(true, Ordering::Release);
    let report = peer_report(&app).expect("peer listener has a control report");
    (AxumStatusCode::OK, AxumJson(report)).into_response()
}

fn peer_report(app: &App) -> Option<NodeControlReport> {
    if !is_peer(app) {
        return None;
    }
    let snapshot = app.admission.snapshot();
    let ready = app.readiness.load(Ordering::Acquire);
    Some(NodeControlReport {
        node: app.node.as_uuid().to_string(),
        listener: app.metrics.listener.to_string(),
        ready,
        admission: admission_name(snapshot.mode),
        active: snapshot.active,
        incarnation: app.incarnation.to_string(),
        disk_removal_safe: !ready && snapshot.active == 0,
        retired: app.retired.load(Ordering::Acquire),
    })
}

fn is_peer(app: &App) -> bool {
    app.metrics.listener.as_ref() == "peer"
}

fn authorize_control_mutation(
    app: &App,
    headers: &HeaderMap,
    method: &str,
    uri: &Uri,
    body: &[u8],
) -> Result<(), ControlAuthError> {
    if !app.config.raw.control.require_signed_mutations {
        return Ok(());
    }
    let target_node = required_header(headers, "x-peren-target-node")?;
    if target_node != app.node.as_uuid().to_string() {
        return Err(ControlAuthError::TargetNode);
    }
    let timestamp_ms = required_header(headers, "x-peren-request-timestamp-ms")?
        .parse::<i64>()
        .map_err(|_| ControlAuthError::Timestamp)?;
    let nonce = required_header(headers, "x-peren-nonce")?;
    let version = required_header(headers, "x-peren-signature-version")?;
    let signature = required_header(headers, "x-peren-signature")?;
    let target = uri.path_and_query().map_or_else(
        || uri.path().to_string(),
        |value| value.as_str().to_string(),
    );
    peren_security::verify_control_request(
        &control_key_dir(app),
        &peren_security::ControlRequest {
            target_node: &target_node,
            method,
            target: &target,
            body,
            timestamp_ms,
            nonce: &nonce,
        },
        &version,
        &signature,
    )
    .map_err(|_| ControlAuthError::Signature)?;
    remember_control_nonce(app, &target_node, &nonce, timestamp_ms)
}

fn required_header(
    headers: &HeaderMap,
    name: &'static str,
) -> Result<String, ControlAuthError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or(ControlAuthError::RequiredSignature)
}

fn remember_control_nonce(
    app: &App,
    node: &str,
    nonce: &str,
    timestamp_ms: i64,
) -> Result<(), ControlAuthError> {
    let now = now_ms().map_err(|()| ControlAuthError::Clock)?;
    let mut seen = app
        .control_replay
        .lock()
        .map_err(|_| ControlAuthError::ReplayState)?;
    let floor = now.saturating_sub(5 * 60 * 1_000);
    seen.retain(|_, seen_at| *seen_at >= floor);
    let key = format!("{node}:{nonce}");
    if seen.insert(key, timestamp_ms).is_some() {
        return Err(ControlAuthError::Replay);
    }
    Ok(())
}

fn control_key_dir(app: &App) -> PathBuf {
    app.data.join("credentials")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlAuthError {
    RequiredSignature,
    TargetNode,
    Timestamp,
    Signature,
    Clock,
    ReplayState,
    Replay,
}

impl ControlAuthError {
    const fn message(self) -> &'static str {
        match self {
            Self::RequiredSignature => "control request signature is required",
            Self::TargetNode => "target node does not match this listener",
            Self::Timestamp => "timestamp is invalid",
            Self::Signature => "control request signature is invalid",
            Self::Clock => "system clock is invalid",
            Self::ReplayState => "control replay state is unavailable",
            Self::Replay => "control request nonce was already used",
        }
    }
}

fn control_auth_error(message: &str) -> axum::response::Response {
    (
        AxumStatusCode::UNAUTHORIZED,
        AxumJson(serde_json::json!({ "error": message })),
    )
        .into_response()
}

fn now_ms() -> Result<i64, ()> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?;
    i64::try_from(duration.as_millis()).map_err(|_| ())
}

fn admission_name(mode: AdmissionMode) -> &'static str {
    match mode {
        AdmissionMode::Serving => "serving",
        AdmissionMode::Draining => "draining",
        AdmissionMode::ControlOnly => "control_only",
    }
}

pub(super) async fn metrics(State(app): State<App>) -> impl IntoResponse {
    let socket = app.socket.as_ref();
    let admission = socket.map(|socket| socket.admission.snapshot());
    let websocket_active = socket
        .and_then(|socket| socket.websocket_sessions.len().ok())
        .unwrap_or(0);
    let body = crate::metrics::render(
        app.readiness.load(Ordering::Acquire),
        &app.metrics,
        admission,
        websocket_active,
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

#[cfg(test)]
pub(super) async fn binding(
    State(app): State<App>,
    AxumPath((service, binding)): AxumPath<(String, String)>,
    Json(invocation): Json<TestInvocation>,
) -> Response<Body> {
    let Some(socket) = app.socket else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if socket.service.as_ref() != service {
        return (
            AxumStatusCode::NOT_FOUND,
            test_error("service is not attached to this socket"),
        )
            .into_response();
    }
    let Some(kind) = test_binding_kind(&socket.environment, &binding) else {
        return (
            AxumStatusCode::NOT_FOUND,
            test_error("binding was not found"),
        )
            .into_response();
    };
    let Ok(bundle) = test_bundle(&binding, &kind, &invocation.method) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("binding method is not supported by the test surface"),
        )
            .into_response();
    };
    let body = match serde_json::to_vec(&serde_json::json!({ "args": invocation.args })) {
        Ok(body) => body,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, test_error(&error.to_string())).into_response();
        }
    };
    let mut harness = socket.clone();
    harness.bundle = bundle;
    match tokio::task::spawn_blocking(move || {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                test_error("failed to create test dispatch runtime"),
            )
                .into_response();
        };
        runtime
            .block_on(process::dispatch_worker(
                harness,
                Method::POST,
                Uri::from_static("/__test/binding"),
                HeaderMap::new(),
                axum::body::Bytes::from(body),
            ))
            .unwrap_or_else(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    test_error(&error.to_string()),
                )
                    .into_response()
            })
    })
    .await
    {
        Ok(response) => response,
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            test_error(&error.to_string()),
        )
            .into_response(),
    }
}

#[cfg(test)]
fn test_error(message: &str) -> Json<JsonValue> {
    Json(serde_json::json!({ "error": message }))
}

#[cfg(test)]
fn test_object_class(environment: &WorkerEnvironment, namespace: &str) -> Option<String> {
    environment
        .get("__perenBindings")
        .and_then(|bindings| serde_json::from_str::<JsonValue>(bindings).ok())
        .and_then(|bindings| {
            bindings
                .get(namespace)?
                .get("className")?
                .as_str()
                .map(str::to_string)
        })
}

#[cfg(test)]
fn test_binding_kind(environment: &WorkerEnvironment, name: &str) -> Option<String> {
    environment
        .get("__perenBindings")
        .and_then(|bindings| serde_json::from_str::<JsonValue>(bindings).ok())
        .and_then(|bindings| {
            bindings
                .get(name)?
                .get("type")?
                .as_str()
                .map(str::to_string)
        })
}

#[cfg(test)]
fn test_bundle(binding: &str, kind: &str, method: &str) -> Result<WorkerBundle, ProcessError> {
    let supported = matches!(
        (kind, method),
        (
            "kv",
            "get" | "kvGet" | "kvGetWithMetadata" | "compareAndSet" | "put" | "delete" | "list"
        ) | ("r2", "r2Get" | "put" | "delete" | "list")
            | (
                "d1",
                "exec" | "batch" | "queryAll" | "queryFirst" | "queryRun" | "queryRaw"
            )
            | ("queue", "send" | "sendBatch")
            | ("service", "fetch")
            | ("dispatcher", "dispatchFetch")
            | ("vectorize", "upsert" | "query" | "getByIds" | "deleteByIds")
            | ("ai", "run")
            | ("rate_limiter", "limit")
            | ("analytics_engine", "writeDataPoint")
    );
    if !supported {
        return Err(ProcessError::TestBindingMethod);
    }
    let binding = serde_json::to_string(binding).map_err(ProcessError::Environment)?;
    let method = serde_json::to_string(method).map_err(ProcessError::Environment)?;
    let source = format!(
        r#"export default {{
  async fetch(request, env) {{
    const {{ args }} = await request.json();
    const target = env[{binding}];
    const result = await invoke(target, {method}, args ?? []);
    return Response.json({{ result }});
  }}
}};

async function invoke(target, method, args) {{
  if (method === "kvGet") return await encodeKvValue(await target.get(args[0], args[1] ?? {{}}), args[1] ?? {{}});
  if (method === "kvGetWithMetadata") {{
    const result = await target.getWithMetadata(args[0], args[1] ?? {{}});
    return {{ value: await encodeKvValue(result.value, args[1] ?? {{}}), metadata: result.metadata ?? null, version: result.version ?? null }};
  }}
  if (method === "r2Get") return await encodeR2Object(await target.get(args[0], args[1] ?? {{}}));
  if (method === "queryAll") return await target.prepare(args[0]).bind(...(args[1] ?? [])).all();
  if (method === "queryFirst") return await target.prepare(args[0]).bind(...(args[1] ?? [])).first();
  if (method === "queryRun") return await target.prepare(args[0]).bind(...(args[1] ?? [])).run();
  if (method === "queryRaw") return await target.prepare(args[0]).bind(...(args[1] ?? [])).raw();
  if (method === "batch") {{
    const statements = Array.from(args[0] ?? [], (statement) => target.prepare(statement.sql).bind(...(statement.params ?? [])));
    return await target.batch(statements);
  }}
  if (method === "fetch") return await encodeResponse(await target.fetch(decodeRequest(args[0])));
  if (method === "dispatchFetch") return await encodeResponse(await target.get(args[0]).fetch(decodeRequest(args[1])));
  if (method === "run") return await target.run(args[0], args[1], args[2]);
  if (method === "sendBatch") return await target.sendBatch(args[0] ?? []);
  return await target[method](...(args ?? []));
}}

function decodeRequest(request) {{
  return new Request(request.url, {{
    method: request.method,
    headers: request.headers ?? [],
    body: request.body == null || request.body.length === 0 ? undefined : new Uint8Array(request.body),
  }});
}}

async function encodeResponse(response) {{
  return {{
    status: response.status,
    headers: Array.from(response.headers.entries()),
    body: Array.from(new Uint8Array(await response.arrayBuffer())),
  }};
}}

async function encodeR2Object(object) {{
  if (object === null) return null;
  return {{
    key: object.key,
    size: object.size,
    contentType: object.httpMetadata?.contentType ?? null,
    customMetadata: object.customMetadata ?? {{}},
    __perenBody: Array.from(new Uint8Array(await object.arrayBuffer())),
  }};
}}

async function encodeKvValue(value, options) {{
  if (value === null) return null;
  if (options.type === "arrayBuffer") return {{ __perenBytes: Array.from(new Uint8Array(value)) }};
  if (options.type === "stream") return {{ __perenBytes: Array.from(new Uint8Array(await new Response(value).arrayBuffer())) }};
  return value;
}}
"#
    );
    let entry = ModuleName::parse("test.js").map_err(ProcessError::Bundle)?;
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(ModuleKind::JavaScript, source.into_bytes())?,
        )]),
    )
    .map_err(ProcessError::Bundle)
}

#[cfg(test)]
fn test_service(app: &App, service: &str) -> Result<SocketApp, Box<Response<Body>>> {
    let Some(socket) = app.socket.clone() else {
        return Err(Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response()));
    };
    if socket.service.as_ref() != service {
        return Err(Box::new(
            (
                AxumStatusCode::NOT_FOUND,
                test_error("service is not attached to this socket"),
            )
                .into_response(),
        ));
    }
    Ok(socket)
}

#[cfg(test)]
fn test_query_service(uri: &Uri) -> Option<String> {
    uri.query().and_then(|query| {
        query.split('&').find_map(|part| {
            let (name, value) = part.split_once('=')?;
            (name == "service").then(|| value.to_string())
        })
    })
}

#[cfg(test)]
pub(super) async fn object_list(
    State(app): State<App>,
    AxumPath(namespace): AxumPath<String>,
    uri: Uri,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    let Ok(socket) = test_service(&app, &service) else {
        return (
            AxumStatusCode::NOT_FOUND,
            test_error("service is not attached to this socket"),
        )
            .into_response();
    };
    let cells = socket
        .registry
        .lock()
        .expect("object registry lock is not poisoned")
        .get(&namespace)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect::<Vec<_>>();
    Json(serde_json::json!({ "namespace": namespace, "cells": cells })).into_response()
}

#[cfg(test)]
pub(super) async fn object_alarm(
    State(app): State<App>,
    AxumPath((namespace, id)): AxumPath<(String, String)>,
    uri: Uri,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    let socket = match test_service(&app, &service) {
        Ok(socket) => socket,
        Err(response) => return *response,
    };
    match test_dispatch_alarm(socket, namespace, id).await {
        Ok(()) => Json(serde_json::json!({ "ran": true })).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            test_error(&error.to_string()),
        )
            .into_response(),
    }
}

#[cfg(test)]
pub(super) async fn object_evict(
    State(app): State<App>,
    AxumPath((_namespace, _id)): AxumPath<(String, String)>,
    uri: Uri,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    match test_service(&app, &service) {
        Ok(_) => Json(serde_json::json!({ "evicted": true })).into_response(),
        Err(response) => *response,
    }
}

#[cfg(test)]
pub(super) async fn object_storage_get(
    State(app): State<App>,
    AxumPath((namespace, id, key)): AxumPath<(String, String, String)>,
    uri: Uri,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    let socket = match test_service(&app, &service) {
        Ok(socket) => socket,
        Err(response) => return *response,
    };
    match test_storage(socket, namespace, id, TestStorage::Get(key)).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            test_error(&error.to_string()),
        )
            .into_response(),
    }
}

#[cfg(test)]
pub(super) async fn object_storage_list(
    State(app): State<App>,
    AxumPath((namespace, id)): AxumPath<(String, String)>,
    uri: Uri,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    let socket = match test_service(&app, &service) {
        Ok(socket) => socket,
        Err(response) => return *response,
    };
    match test_storage(socket, namespace, id, TestStorage::List).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            test_error(&error.to_string()),
        )
            .into_response(),
    }
}

#[cfg(test)]
pub(super) async fn object_storage_put(
    State(app): State<App>,
    AxumPath((namespace, id, key)): AxumPath<(String, String, String)>,
    uri: Uri,
    body: axum::body::Bytes,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    let socket = match test_service(&app, &service) {
        Ok(socket) => socket,
        Err(response) => return *response,
    };
    match test_storage(
        socket,
        namespace,
        id,
        TestStorage::Put {
            key,
            value: body.to_vec(),
        },
    )
    .await
    {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            test_error(&error.to_string()),
        )
            .into_response(),
    }
}

#[cfg(test)]
pub(super) async fn object_storage_delete(
    State(app): State<App>,
    AxumPath((namespace, id, key)): AxumPath<(String, String, String)>,
    uri: Uri,
) -> Response<Body> {
    let Some(service) = test_query_service(&uri) else {
        return (
            StatusCode::BAD_REQUEST,
            test_error("missing service query parameter"),
        )
            .into_response();
    };
    let socket = match test_service(&app, &service) {
        Ok(socket) => socket,
        Err(response) => return *response,
    };
    match test_storage(socket, namespace, id, TestStorage::Delete(key)).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            test_error(&error.to_string()),
        )
            .into_response(),
    }
}

#[cfg(test)]
fn register_object(registry: &ObjectRegistry, namespace: &str, id: &str) {
    registry
        .lock()
        .expect("object registry lock is not poisoned")
        .entry(namespace.to_string())
        .or_default()
        .insert(id.to_string());
}

#[cfg(test)]
enum TestStorage {
    Get(String),
    List,
    Put { key: String, value: Vec<u8> },
    Delete(String),
}

#[cfg(test)]
async fn test_storage(
    socket: SocketApp,
    namespace: String,
    id: String,
    action: TestStorage,
) -> Result<JsonValue, ProcessError> {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(ProcessError::Runtime)?;
        runtime.block_on(test_storage_inner(socket, namespace, id, action))
    })
    .await
    .map_err(|error| ProcessError::Task(error.to_string()))?
}

#[cfg(test)]
#[cfg(test)]
async fn test_storage_inner(
    socket: SocketApp,
    namespace: String,
    id: String,
    action: TestStorage,
) -> Result<JsonValue, ProcessError> {
    register_object(&socket.registry, &namespace, &id);
    socket
        .node
        .restore_and_dispatch(process::object_cell(&namespace, &id), |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut storage = peren_storage::CellStorage::open(&path)
                .map_err(peren_cell::CellError::from)?;
            let value = match action {
                TestStorage::Get(key) => {
                    let found = storage
                        .get("do", key.as_bytes())
                        .map_err(peren_cell::CellError::from)?;
                    let value = serde_json::json!({
                        "found": found.is_some(),
                        "value": found.map(|bytes| String::from_utf8_lossy(&bytes).to_string()),
                    });
                    lease.release().await.map_err(peren_cell::CellError::from)?;
                    value
                }
                TestStorage::List => {
                    let page = storage
                        .list("do", &StorageListOptions::default())
                        .map_err(peren_cell::CellError::from)?;
                    let value = serde_json::json!({
                        "entries": page.entries.into_iter().map(|(key, value)| serde_json::json!({
                            "name": String::from_utf8_lossy(&key).to_string(),
                            "value": String::from_utf8_lossy(&value).to_string(),
                        })).collect::<Vec<_>>(),
                        "keys": [],
                        "list_complete": page.next_cursor.is_none(),
                        "cursor": page.next_cursor.map(|cursor| String::from_utf8_lossy(&cursor).to_string()),
                    });
                    lease.release().await.map_err(peren_cell::CellError::from)?;
                    value
                }
                TestStorage::Put { key, value } => {
                    let revision = storage
                        .put("do", key.as_bytes(), &value)
                        .map_err(peren_cell::CellError::from)?;
                    let checkpoint = storage.checkpoint().map_err(peren_cell::CellError::from)?;
                    publish_test_storage(&store, lease, checkpoint.database, revision).await?;
                    serde_json::json!({ "ok": true })
                }
                TestStorage::Delete(key) => {
                    let deleted = storage
                        .delete("do", key.as_bytes())
                        .map_err(peren_cell::CellError::from)?;
                    let checkpoint = storage.checkpoint().map_err(peren_cell::CellError::from)?;
                    publish_test_storage(&store, lease, checkpoint.database, deleted.revision).await?;
                    serde_json::json!({ "deleted": deleted.value })
                }
            };
            Ok(value)
        })
        .await
        .map_err(ProcessError::from)
}

#[cfg(test)]
async fn publish_test_storage(
    store: &Repository,
    lease: <Repository as crate::NodeRepository>::Lease,
    database: Vec<u8>,
    revision: peren_primitives::StorageRevision,
) -> Result<(), crate::NodeError> {
    store
        .checkpoint(lease.cell(), lease.epoch(), revision, &database)
        .await?;
    lease.release().await.map_err(peren_cell::CellError::from)?;
    Ok(())
}

#[cfg(test)]
async fn test_dispatch_alarm(
    socket: SocketApp,
    namespace: String,
    id: String,
) -> Result<(), ProcessError> {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(ProcessError::Runtime)?;
        runtime.block_on(async move {
            register_object(&socket.registry, &namespace, &id);
            let class = test_object_class(&socket.environment, &namespace)
                .ok_or(ProcessError::DurableObjectNamespace(namespace.clone()))?;
            let service = socket
                .objects
                .get(&class)
                .ok_or_else(|| ProcessError::DurableObjectClass(class.clone()))?;
            let target = socket
                .services
                .get(service)
                .ok_or_else(|| ProcessError::ServiceBundle(service.clone()))?;
            socket
                .node
                .dispatch_alarm(
                    process::object_cell(&namespace, &id),
                    target.bundle.clone(),
                    IsolateLimits::new(socket.limits.heap, socket.limits.execution),
                )
                .await
                .map_err(ProcessError::Node)
        })
    })
    .await
    .map_err(|error| ProcessError::Task(error.to_string()))?
}
