use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

#[cfg(test)]
use axum::routing::post;
use axum::{
    Router,
    body::{self, Body},
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::{any, get},
};
use futures::{SinkExt, StreamExt};
use hyper::upgrade::{OnUpgrade, Upgraded};
use hyper_util::rt::TokioIo;
use peren_primitives::CellId;
use peren_runtime::{
    HttpRequest, HttpResponse, InvocationLimits, IsolateLimits, WebSocketCloseEvent,
    WebSocketDispatch, WebSocketMessageEvent,
};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{CloseFrame, Role},
    },
};

use crate::{
    asset, control,
    host::RoutedHost,
    metrics::Telemetry,
    process::{App, ProcessError, ProcessHost, SocketApp, cell, has_binding, turso_routes},
    tail,
    trace::{self, SpanKind, SpanRecord, TraceContext},
    websocket,
    websocket::Session as WebSocketSession,
};

pub(super) async fn listener(
    name: &str,
    address: SocketAddr,
    inherited: &mut BTreeMap<String, std::net::TcpListener>,
) -> Result<TcpListener, std::io::Error> {
    match inherited.remove(name) {
        Some(listener) => {
            listener.set_nonblocking(true)?;
            TcpListener::from_std(listener)
        }
        None => TcpListener::bind(address).await,
    }
}

pub(super) fn router(app: App) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(|State(app): State<App>| async move {
                if app.readiness.load(Ordering::Acquire) {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route("/metrics", get(control::metrics))
        .route("/control/v1/node", get(control::node_status))
        .route(
            "/control/v1/node/drain",
            axum::routing::post(control::node_drain),
        )
        .route(
            "/control/v1/node/control",
            axum::routing::post(control::node_control_only),
        )
        .route(
            "/control/v1/node/retire",
            axum::routing::post(control::node_retire),
        )
        .route(
            "/control/v1/deployments",
            get(control::deployment_list).post(control::deployment_record),
        )
        .route(
            "/control/v1/deployments/verify",
            get(control::deployment_verify),
        )
        .route(
            "/control/v1/deployments/health",
            get(control::deployment_health),
        );
    if app.dev_inspector {
        router = router
            .route("/__peren/dev", get(control::dev_overview))
            .route("/__peren/dev/topology", get(control::dev_topology))
            .route("/__peren/dev/events", get(control::dev_events))
            .route("/__peren/dev/health", get(control::dev_health));
    }

    #[cfg(test)]
    let router = test_router(router);

    router
        .route("/", any(dispatch))
        .route("/{*path}", any(dispatch))
        .with_state(app)
}

#[cfg(test)]
fn test_router(router: Router<App>) -> Router<App> {
    router
        .route(
            "/__test/binding/{service}/{binding}/invoke",
            post(control::binding),
        )
        .route("/__test/do/{namespace}", get(control::object_list))
        .route(
            "/__test/do/{namespace}/{id}/alarm/run",
            post(control::object_alarm),
        )
        .route(
            "/__test/do/{namespace}/{id}/evict",
            post(control::object_evict),
        )
        .route(
            "/__test/do/{namespace}/{id}/storage",
            get(control::object_storage_list),
        )
        .route(
            "/__test/do/{namespace}/{id}/storage/{key}",
            get(control::object_storage_get)
                .put(control::object_storage_put)
                .delete(control::object_storage_delete),
        )
}

async fn dispatch(State(app): State<App>, request: Request<Body>) -> Response<Body> {
    let telemetry = Arc::clone(&app.metrics.telemetry);
    let started = Instant::now();
    let Some(socket) = app.socket.clone() else {
        Telemetry::inc(&telemetry.http_requests);
        Telemetry::inc(&telemetry.http_errors);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (mut parts, body) = request.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let headers = parts.headers;
    let on_upgrade = parts.extensions.remove::<OnUpgrade>();
    let body = match body::to_bytes(body, socket.limits.body_limit.saturating_add(1)).await {
        Ok(body) => body,
        Err(error) => {
            Telemetry::inc(&telemetry.http_requests);
            Telemetry::inc(&telemetry.http_errors);
            return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
        }
    };
    if websocket::is_upgrade(&headers) {
        if let Some(on_upgrade) = on_upgrade {
            return dispatch_upgrade(
                app,
                socket,
                UpgradeRequest {
                    on_upgrade,
                    method,
                    uri,
                    headers,
                    body,
                    started,
                },
            )
            .await;
        }
        Telemetry::inc(&telemetry.http_requests);
        Telemetry::inc(&telemetry.http_errors);
        return StatusCode::UPGRADE_REQUIRED.into_response();
    }
    let Ok(permit) = socket.admission.admit() else {
        Telemetry::inc(&telemetry.http_requests);
        Telemetry::inc(&telemetry.http_errors);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create dispatch runtime",
            )
                .into_response();
        };
        runtime
            .block_on(dispatch_worker(socket, method, uri, headers, body))
            .unwrap_or_else(IntoResponse::into_response)
    })
    .await
    {
        Ok(response) => {
            Telemetry::inc(&telemetry.http_requests);
            Telemetry::observe(
                &telemetry.http_duration_ms,
                &telemetry.http_duration,
                started,
            );
            if response.status().is_server_error() {
                Telemetry::inc(&telemetry.http_errors);
            }
            response
        }
        Err(error) => {
            Telemetry::inc(&telemetry.http_requests);
            Telemetry::observe(
                &telemetry.http_duration_ms,
                &telemetry.http_duration,
                started,
            );
            Telemetry::inc(&telemetry.http_errors);
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}

struct UpgradeRequest {
    on_upgrade: OnUpgrade,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: axum::body::Bytes,
    started: Instant,
}

async fn dispatch_upgrade(app: App, socket: SocketApp, request: UpgradeRequest) -> Response<Body> {
    let telemetry = Arc::clone(&app.metrics.telemetry);
    let Ok(permit) = socket.admission.admit() else {
        Telemetry::inc(&telemetry.http_requests);
        Telemetry::inc(&telemetry.http_errors);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let bridge_socket = socket.clone();
    let response_headers = request.headers.clone();
    let UpgradeRequest {
        on_upgrade,
        method,
        uri,
        headers,
        body,
        started,
    } = request;
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return Err(ProcessError::Runtime(std::io::Error::other(
                "failed to create dispatch runtime",
            )));
        };
        runtime.block_on(dispatch_upgrade_worker(socket, method, uri, headers, body))
    })
    .await
    {
        Ok(Ok(accepted)) => {
            Telemetry::inc(&telemetry.http_requests);
            Telemetry::observe(
                &telemetry.http_duration_ms,
                &telemetry.http_duration,
                started,
            );
            let session = accepted.session.clone();
            let response = websocket::response(&response_headers, accepted.response)
                .map_err(ProcessError::WebSocketSession)
                .unwrap_or_else(IntoResponse::into_response);
            tokio::spawn(async move {
                if let Ok(upgraded) = on_upgrade.await {
                    let stream = WebSocketStream::from_raw_socket(
                        TokioIo::new(upgraded),
                        Role::Server,
                        None,
                    )
                    .await;
                    bridge(stream, bridge_socket, session).await;
                }
            });
            response
        }
        Ok(Err(error)) => {
            Telemetry::inc(&telemetry.http_requests);
            Telemetry::observe(
                &telemetry.http_duration_ms,
                &telemetry.http_duration,
                started,
            );
            Telemetry::inc(&telemetry.http_errors);
            error.into_response()
        }
        Err(error) => {
            Telemetry::inc(&telemetry.http_requests);
            Telemetry::observe(
                &telemetry.http_duration_ms,
                &telemetry.http_duration,
                started,
            );
            Telemetry::inc(&telemetry.http_errors);
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}

struct AcceptedUpgrade {
    response: HttpResponse,
    session: WebSocketSession,
}

async fn bridge(
    mut stream: WebSocketStream<TokioIo<Upgraded>>,
    app: SocketApp,
    session: WebSocketSession,
) {
    let session_id = session.id.clone();
    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let event = WebSocketMessageEvent {
                    id: session.id.clone(),
                    message: text.to_string(),
                };
                let Ok(dispatch) =
                    dispatch_websocket_message_worker(app.clone(), session.cell, event).await
                else {
                    let _ = stream.close(None).await;
                    let _ = app.websocket_sessions.remove(&session_id);
                    return;
                };
                for frame in dispatch.outbound {
                    if stream.send(Message::Text(frame.into())).await.is_err() {
                        let _ = app.websocket_sessions.remove(&session_id);
                        return;
                    }
                }
            }
            Ok(Message::Close(frame)) => {
                let (code, reason) = close_parts(frame.as_ref());
                let event = WebSocketCloseEvent {
                    id: session.id.clone(),
                    code,
                    reason,
                    was_clean: true,
                };
                if let Ok(dispatch) =
                    dispatch_websocket_close_worker(app.clone(), session.cell, event).await
                {
                    for frame in dispatch.outbound {
                        if stream.send(Message::Text(frame.into())).await.is_err() {
                            let _ = app.websocket_sessions.remove(&session_id);
                            return;
                        }
                    }
                }
                let _ = stream.close(None).await;
                let _ = app.websocket_sessions.remove(&session_id);
                return;
            }
            Ok(Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {}
            Err(_) => {
                let event = WebSocketCloseEvent {
                    id: session.id.clone(),
                    code: 1006,
                    reason: "connection error".into(),
                    was_clean: false,
                };
                let _ = dispatch_websocket_close_worker(app.clone(), session.cell, event).await;
                let _ = app.websocket_sessions.remove(&session_id);
                return;
            }
        }
    }
    let event = WebSocketCloseEvent {
        id: session.id,
        code: 1000,
        reason: String::new(),
        was_clean: true,
    };
    let _ = dispatch_websocket_close_worker(app.clone(), session.cell, event).await;
    let _ = app.websocket_sessions.remove(&session_id);
}

async fn dispatch_websocket_message_worker(
    app: SocketApp,
    cell: CellId,
    event: WebSocketMessageEvent,
) -> Result<WebSocketDispatch, ProcessError> {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(ProcessError::Runtime)?;
        runtime.block_on(dispatch_websocket_message(app, cell, event))
    })
    .await
    .map_err(|error| ProcessError::Task(error.to_string()))?
}

async fn dispatch_websocket_close_worker(
    app: SocketApp,
    cell: CellId,
    event: WebSocketCloseEvent,
) -> Result<WebSocketDispatch, ProcessError> {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(ProcessError::Runtime)?;
        runtime.block_on(dispatch_websocket_close(app, cell, event))
    })
    .await
    .map_err(|error| ProcessError::Task(error.to_string()))?
}

fn close_parts(frame: Option<&CloseFrame>) -> (u16, String) {
    frame.map_or((1000, String::new()), |frame| {
        (frame.code.into(), frame.reason.to_string())
    })
}

pub(super) struct Dispatch {
    request: HttpRequest,
    cell: CellId,
    isolate: IsolateLimits,
    invocation: InvocationLimits,
    host: bool,
    trace: TraceContext,
}

struct DispatchResult {
    response: HttpResponse,
    logs: Vec<peren_runtime::WorkerLogEvent>,
}

#[allow(
    clippy::too_many_lines,
    reason = "HTTP ingress records tail, asset, websocket and trace lifecycle state in one request boundary"
)]
pub(crate) async fn dispatch_worker(
    app: SocketApp,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response<Body>, ProcessError> {
    let upgrade = websocket::is_upgrade(&headers);
    if body.len() > app.limits.body_limit {
        return Err(ProcessError::BodyLimit);
    }
    let started = Instant::now();
    let path = uri
        .path_and_query()
        .map_or_else(|| "/".to_string(), ToString::to_string);
    let method_name = method.to_string();
    let tail_path = app.tail.clone();
    let service_name = app.service.to_string();
    let request_id = uuid::Uuid::new_v4().to_string();
    let dispatch_id = uuid::Uuid::new_v4().to_string();
    let traceparent = traceparent(&headers);
    let trace = TraceContext::ingress(
        traceparent.as_deref(),
        request_id.clone(),
        dispatch_id.clone(),
        app.sampling_ratio,
    );
    let worker_first = app
        .assets
        .as_ref()
        .is_some_and(|assets| assets.run_worker_first(uri.path()));
    if !worker_first
        && let Some(response) = asset::serve(app.assets.as_ref(), &method, uri.path()).await?
    {
        let _ = app.trace.record_span(
            &trace,
            SpanRecord {
                service: &service_name,
                cell: None,
                name: "http.asset",
                kind: SpanKind::Server,
                started,
                started_at_ms: trace::now_ms(),
                outcome: if response.status().as_u16() >= 500 {
                    "error"
                } else {
                    "ok"
                },
                attributes: BTreeMap::from([
                    ("http.request.method".into(), method_name.clone()),
                    ("url.path".into(), uri.path().to_string()),
                    (
                        "http.response.status_code".into(),
                        response.status().as_u16().to_string(),
                    ),
                ]),
            },
        );
        record_tail(TailRecord {
            tail: &tail_path,
            service: &service_name,
            method: method_name,
            path,
            status: response.status().as_u16(),
            started,
            request: &request_id,
            dispatch: &dispatch_id,
            traceparent: traceparent.as_deref(),
        })?;
        return Ok(response);
    }
    let request = request(method_name.clone(), &path, &headers, &body)?;
    let dispatch = Dispatch {
        request,
        cell: cell(&app.service, uri.path()),
        isolate: IsolateLimits::new(app.limits.heap, app.limits.execution),
        invocation: InvocationLimits::new(app.limits.body, app.limits.subrequests),
        host: uses_host(&app),
        trace,
    };
    let websocket_sessions = Arc::clone(&app.websocket_sessions);
    let telemetry = Arc::clone(&app.telemetry);
    let session_cell = dispatch.cell;
    let session_path = path.clone();
    let dispatch = dispatch_cell(app, dispatch).await?;
    let response = dispatch.response;
    record_tail(TailRecord {
        tail: &tail_path,
        service: &service_name,
        method: method_name,
        path: path.clone(),
        status: if upgrade && response.upgrade {
            101
        } else {
            response.status
        },
        started,
        request: &request_id,
        dispatch: &dispatch_id,
        traceparent: traceparent.as_deref(),
    })?;
    record_console(
        &tail_path,
        &service_name,
        session_cell,
        "fetch",
        &request_id,
        dispatch.logs,
    )?;
    if upgrade && response.upgrade {
        if let Some(session) = response.websocket_id.as_ref() {
            let inserted = websocket_sessions.record(WebSocketSession {
                id: session.clone(),
                service: service_name.clone(),
                path: session_path.clone(),
                cell: session_cell,
            })?;
            if inserted {
                Telemetry::inc(&telemetry.websocket_sessions);
            }
        }
        websocket::response(&headers, response).map_err(ProcessError::WebSocketSession)
    } else {
        response_body(response)
    }
}

async fn dispatch_upgrade_worker(
    app: SocketApp,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<AcceptedUpgrade, ProcessError> {
    let response = dispatch_worker(app.clone(), method, uri, headers, body).await?;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Err(ProcessError::WebSocketSession(
            websocket::RegistryError::Rejected,
        ));
    }
    let session = response
        .headers()
        .get("x-peren-websocket-session")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ProcessError::WebSocketSession(websocket::RegistryError::MissingSession))?;
    let session = app
        .websocket_sessions
        .get(session)?
        .ok_or_else(|| ProcessError::WebSocketSession(websocket::RegistryError::MissingSession))?;
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| Ok((name.as_str().to_string(), value.to_str()?.to_string())))
        .collect::<Result<Vec<_>, ProcessError>>()?;
    Ok(AcceptedUpgrade {
        response: HttpResponse {
            status: response.status().as_u16(),
            headers,
            body: Vec::new(),
            upgrade: true,
            websocket_id: Some(session.id.clone()),
        },
        session,
    })
}

fn request(
    method: String,
    path: &str,
    headers: &HeaderMap,
    body: &axum::body::Bytes,
) -> Result<HttpRequest, ProcessError> {
    let headers = headers
        .iter()
        .map(|(name, value)| Ok((name.as_str().to_string(), value.to_str()?.to_string())))
        .collect::<Result<Vec<_>, ProcessError>>()?;
    Ok(HttpRequest {
        method,
        url: format!("http://worker.invalid{path}"),
        headers,
        body: body.to_vec(),
        mtls: None,
    })
}

fn uses_host(app: &SocketApp) -> bool {
    !app.d1.is_empty()
        || !app.r2.is_empty()
        || !app.kv.is_empty()
        || !app.consumers.is_empty()
        || host_bound_binding(&app.environment)
        || app.environment.get("__perenCache").is_some()
}

fn host_bound_binding(environment: &peren_runtime::WorkerEnvironment) -> bool {
    [
        "queue",
        "service",
        "durable_object_namespace",
        "ai",
        "vectorize",
        "outbound",
        "images",
        "container",
    ]
    .into_iter()
    .any(|kind| has_binding(environment, kind))
}

async fn dispatch_cell(app: SocketApp, dispatch: Dispatch) -> Result<DispatchResult, ProcessError> {
    if dispatch.host {
        dispatch_cell_host(app, dispatch).await
    } else {
        dispatch_cell_plain(app, dispatch).await
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "plain cell dispatch keeps restore, invoke, commit and release trace points together"
)]
async fn dispatch_cell_plain(
    app: SocketApp,
    dispatch: Dispatch,
) -> Result<DispatchResult, ProcessError> {
    let service = app.service.to_string();
    let trace_sink = app.trace.clone();
    let trace_context = dispatch.trace.clone();
    app.node
        .restore_and_dispatch(dispatch.cell, |input| async move {
            let cell_id = dispatch.cell.to_string();
            let restore_started = Instant::now();
            let restore_started_at_ms = trace::now_ms();
            let restore_source = match &input.restore.source {
                crate::RestoreSource::Empty => "empty".to_string(),
                crate::RestoreSource::Restored { .. } => "restored".to_string(),
            };
            let _ = trace_sink.record_span(
                &trace_context.child(),
                SpanRecord {
                    service: &service,
                    cell: Some(&cell_id),
                    name: "cell.restore",
                    kind: SpanKind::Internal,
                    started: restore_started,
                    started_at_ms: restore_started_at_ms,
                    outcome: "ok",
                    attributes: BTreeMap::from([("peren.restore.source".into(), restore_source)]),
                },
            );
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = peren_cell::WorkerCell::activate(
                &path,
                lease,
                store,
                app.bundle,
                dispatch.isolate,
                app.environment,
            )
            .await?;
            let dispatch_started = Instant::now();
            let dispatch_started_at_ms = trace::now_ms();
            let response = resident
                .dispatch_http(dispatch.request, dispatch.invocation)
                .await?;
            resident
                .checkpoint_if_wal_exceeds(app.checkpoint_threshold_bytes)
                .await?;
            let commit = resident.last_commit();
            let _ = trace_sink.record_span(
                &trace_context,
                SpanRecord {
                    service: &service,
                    cell: Some(&cell_id),
                    name: "cell.dispatch",
                    kind: SpanKind::Server,
                    started: dispatch_started,
                    started_at_ms: dispatch_started_at_ms,
                    outcome: if response.status >= 500 {
                        "error"
                    } else {
                        "ok"
                    },
                    attributes: BTreeMap::from([(
                        "http.response.status_code".into(),
                        response.status.to_string(),
                    )]),
                },
            );
            if let Some(commit) = commit {
                let _ = trace_sink.record_span(
                    &trace_context.child(),
                    SpanRecord {
                        service: &service,
                        cell: Some(&cell_id),
                        name: "cell.commit",
                        kind: SpanKind::Internal,
                        started: Instant::now(),
                        started_at_ms: trace::now_ms(),
                        outcome: "ok",
                        attributes: BTreeMap::from([(
                            "peren.storage.revision".into(),
                            commit.revision.get().to_string(),
                        )]),
                    },
                );
            }
            let logs = resident.take_console_events();
            let release_started = Instant::now();
            let release_started_at_ms = trace::now_ms();
            resident.release().await?;
            let _ = trace_sink.record_span(
                &trace_context.child(),
                SpanRecord {
                    service: &service,
                    cell: Some(&cell_id),
                    name: "cell.release",
                    kind: SpanKind::Internal,
                    started: release_started,
                    started_at_ms: release_started_at_ms,
                    outcome: "ok",
                    attributes: BTreeMap::new(),
                },
            );
            Ok(DispatchResult { response, logs })
        })
        .await
        .map_err(ProcessError::Node)
}

#[allow(
    clippy::too_many_lines,
    reason = "host cell dispatch mirrors capability wiring and lifecycle trace points"
)]
async fn dispatch_cell_host(
    app: SocketApp,
    dispatch: Dispatch,
) -> Result<DispatchResult, ProcessError> {
    let d1 = turso_routes(&app.d1).await?;
    let r2 = app.r2.clone();
    let kv = app.kv.clone();
    let queues = app.queues.clone();
    let cache = app.cache.clone();
    let outbound_hosts = Arc::clone(&app.outbound_hosts);
    let aws = Arc::clone(&app.aws);
    let services = Arc::clone(&app.services);
    let objects = Arc::clone(&app.objects);
    let registry = Arc::clone(&app.registry);
    let node = app.node.clone();
    let limits = app.limits;
    let trace_sink = app.trace.clone();
    let trace_context = dispatch.trace.clone();

    app.node
        .restore_and_dispatch(dispatch.cell, |input| async move {
            let cell_id = dispatch.cell.to_string();
            let restore_started = Instant::now();
            let restore_started_at_ms = trace::now_ms();
            let restore_source = match &input.restore.source {
                crate::RestoreSource::Empty => "empty".to_string(),
                crate::RestoreSource::Restored { .. } => "restored".to_string(),
            };
            let _ = trace_sink.record_span(
                &trace_context.child(),
                SpanRecord {
                    service: &app.service,
                    cell: Some(&cell_id),
                    name: "cell.restore",
                    kind: SpanKind::Internal,
                    started: restore_started,
                    started_at_ms: restore_started_at_ms,
                    outcome: "ok",
                    attributes: BTreeMap::from([("peren.restore.source".into(), restore_source)]),
                },
            );
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let host_trace_sink = trace_sink.clone();
            let host_trace_context = trace_context.clone();
            let mut resident = peren_cell::WorkerCell::activate_with_capabilities(
                &path,
                lease,
                store,
                app.bundle,
                dispatch.isolate,
                app.environment,
                move |storage| {
                    Arc::new(ProcessHost {
                        inner: RoutedHost::new(
                            storage,
                            d1,
                            r2,
                            queues.clone(),
                            cache.clone(),
                            kv.clone(),
                        ),
                        node,
                        services,
                        objects,
                        registry: Arc::clone(&registry),
                        outbound_hosts: Arc::clone(&outbound_hosts),
                        aws: Arc::clone(&aws),
                        mtls: Arc::clone(&app.mtls),
                        queues,
                        r2_notifications: app.r2_notifications.clone(),
                        cache,
                        limits,
                        telemetry: Arc::clone(&app.telemetry),
                        trace: host_trace_sink.clone(),
                        trace_context: Some(host_trace_context.clone()),
                    })
                },
            )
            .await?;
            let dispatch_started = Instant::now();
            let dispatch_started_at_ms = trace::now_ms();
            let response = resident
                .dispatch_http(dispatch.request, dispatch.invocation)
                .await?;
            resident
                .checkpoint_if_wal_exceeds(app.checkpoint_threshold_bytes)
                .await?;
            let commit = resident.last_commit();
            let _ = trace_sink.record_span(
                &trace_context,
                SpanRecord {
                    service: &app.service,
                    cell: Some(&dispatch.cell.to_string()),
                    name: "cell.dispatch",
                    kind: SpanKind::Server,
                    started: dispatch_started,
                    started_at_ms: dispatch_started_at_ms,
                    outcome: if response.status >= 500 {
                        "error"
                    } else {
                        "ok"
                    },
                    attributes: BTreeMap::from([(
                        "http.response.status_code".into(),
                        response.status.to_string(),
                    )]),
                },
            );
            if let Some(commit) = commit {
                let _ = trace_sink.record_span(
                    &trace_context.child(),
                    SpanRecord {
                        service: &app.service,
                        cell: Some(&dispatch.cell.to_string()),
                        name: "cell.commit",
                        kind: SpanKind::Internal,
                        started: Instant::now(),
                        started_at_ms: trace::now_ms(),
                        outcome: "ok",
                        attributes: BTreeMap::from([(
                            "peren.storage.revision".into(),
                            commit.revision.get().to_string(),
                        )]),
                    },
                );
            }
            let logs = resident.take_console_events();
            let release_started = Instant::now();
            let release_started_at_ms = trace::now_ms();
            resident.release().await?;
            let _ = trace_sink.record_span(
                &trace_context.child(),
                SpanRecord {
                    service: &app.service,
                    cell: Some(&dispatch.cell.to_string()),
                    name: "cell.release",
                    kind: SpanKind::Internal,
                    started: release_started,
                    started_at_ms: release_started_at_ms,
                    outcome: "ok",
                    attributes: BTreeMap::new(),
                },
            );
            Ok(DispatchResult { response, logs })
        })
        .await
        .map_err(ProcessError::Node)
}

pub(crate) async fn dispatch_websocket_message(
    app: SocketApp,
    cell: CellId,
    event: WebSocketMessageEvent,
) -> Result<WebSocketDispatch, ProcessError> {
    dispatch_websocket(app, cell, WebSocketEvent::Message(event)).await
}

pub(crate) async fn dispatch_websocket_close(
    app: SocketApp,
    cell: CellId,
    event: WebSocketCloseEvent,
) -> Result<WebSocketDispatch, ProcessError> {
    dispatch_websocket(app, cell, WebSocketEvent::Close(event)).await
}

enum WebSocketEvent {
    Message(WebSocketMessageEvent),
    Close(WebSocketCloseEvent),
}

impl WebSocketEvent {
    fn name(&self) -> &'static str {
        match self {
            Self::Message(_) => "websocket_message",
            Self::Close(_) => "websocket_close",
        }
    }
}

async fn dispatch_websocket(
    app: SocketApp,
    cell: CellId,
    event: WebSocketEvent,
) -> Result<WebSocketDispatch, ProcessError> {
    let tail_path = app.tail.clone();
    let service_name = app.service.to_string();
    let request_id = uuid::Uuid::new_v4().to_string();
    let event_name = event.name();

    let (dispatch, logs) = if uses_host(&app) {
        dispatch_websocket_host(app, cell, event).await?
    } else {
        dispatch_websocket_plain(app, cell, event).await?
    };

    record_console(
        &tail_path,
        &service_name,
        cell,
        event_name,
        &request_id,
        logs,
    )?;
    Ok(dispatch)
}

async fn dispatch_websocket_plain(
    app: SocketApp,
    cell: CellId,
    event: WebSocketEvent,
) -> Result<(WebSocketDispatch, Vec<peren_runtime::WorkerLogEvent>), ProcessError> {
    app.node
        .restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = peren_cell::WorkerCell::activate(
                &path,
                lease,
                store,
                app.bundle,
                IsolateLimits::new(app.limits.heap, app.limits.execution),
                app.environment,
            )
            .await?;
            let dispatch = match event {
                WebSocketEvent::Message(event) => {
                    resident.dispatch_websocket_message(event).await?
                }
                WebSocketEvent::Close(event) => resident.dispatch_websocket_close(event).await?,
            };
            resident
                .checkpoint_if_wal_exceeds(app.checkpoint_threshold_bytes)
                .await?;
            let logs = resident.take_console_events();
            resident.release().await?;
            Ok((dispatch, logs))
        })
        .await
        .map_err(ProcessError::Node)
}

async fn dispatch_websocket_host(
    app: SocketApp,
    cell: CellId,
    event: WebSocketEvent,
) -> Result<(WebSocketDispatch, Vec<peren_runtime::WorkerLogEvent>), ProcessError> {
    let d1 = turso_routes(&app.d1).await?;
    let r2 = app.r2.clone();
    let kv = app.kv.clone();
    let queues = app.queues.clone();
    let cache = app.cache.clone();
    let outbound_hosts = Arc::clone(&app.outbound_hosts);
    let aws = Arc::clone(&app.aws);
    let services = Arc::clone(&app.services);
    let objects = Arc::clone(&app.objects);
    let registry = Arc::clone(&app.registry);
    let node = app.node.clone();
    let limits = app.limits;

    app.node
        .restore_and_dispatch(cell, |input| async move {
            let path = input.path;
            let lease = input.lease;
            let store = input.repository;
            let mut resident = peren_cell::WorkerCell::activate_with_capabilities(
                &path,
                lease,
                store,
                app.bundle,
                IsolateLimits::new(app.limits.heap, app.limits.execution),
                app.environment,
                move |storage| {
                    Arc::new(ProcessHost {
                        inner: RoutedHost::new(
                            storage,
                            d1,
                            r2,
                            queues.clone(),
                            cache.clone(),
                            kv.clone(),
                        ),
                        node,
                        services,
                        objects,
                        registry: Arc::clone(&registry),
                        outbound_hosts: Arc::clone(&outbound_hosts),
                        aws: Arc::clone(&aws),
                        mtls: Arc::clone(&app.mtls),
                        queues,
                        r2_notifications: app.r2_notifications.clone(),
                        cache,
                        limits,
                        telemetry: Arc::clone(&app.telemetry),
                        trace: app.trace.clone(),
                        trace_context: None,
                    })
                },
            )
            .await?;
            let dispatch = match event {
                WebSocketEvent::Message(event) => {
                    resident.dispatch_websocket_message(event).await?
                }
                WebSocketEvent::Close(event) => resident.dispatch_websocket_close(event).await?,
            };
            resident
                .checkpoint_if_wal_exceeds(app.checkpoint_threshold_bytes)
                .await?;
            let logs = resident.take_console_events();
            resident.release().await?;
            Ok((dispatch, logs))
        })
        .await
        .map_err(ProcessError::Node)
}

fn record_console(
    tail_path: &Path,
    service: &str,
    cell: CellId,
    event: &str,
    request_id: &str,
    logs: Vec<peren_runtime::WorkerLogEvent>,
) -> Result<(), ProcessError> {
    for log in logs {
        tail::append(
            tail_path,
            &tail::Event::Console(tail::ConsoleEvent {
                service: service.to_string(),
                cell: Some(cell.to_string()),
                request_id: Some(request_id.to_string()),
                event: event.to_string(),
                level: match log.level {
                    peren_runtime::WorkerLogLevel::Debug => tail::ConsoleLevel::Debug,
                    peren_runtime::WorkerLogLevel::Info => tail::ConsoleLevel::Info,
                    peren_runtime::WorkerLogLevel::Warn => tail::ConsoleLevel::Warn,
                    peren_runtime::WorkerLogLevel::Error => tail::ConsoleLevel::Error,
                },
                message: log.message,
                timestamp_ms: log.timestamp_ms,
            }),
        )
        .map_err(ProcessError::Tail)?;
    }
    Ok(())
}

fn traceparent(headers: &HeaderMap) -> Option<String> {
    headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_traceparent(value))
        .map(str::to_string)
}

fn valid_traceparent(value: &str) -> bool {
    let parts: Vec<_> = value.split('-').collect();
    parts.len() == 4
        && parts[0].len() == 2
        && parts[1].len() == 32
        && parts[2].len() == 16
        && parts[3].len() == 2
        && parts
            .iter()
            .all(|part| part.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn response_body(response: HttpResponse) -> Result<Response<Body>, ProcessError> {
    let mut builder = Response::builder().status(response.status);
    for (name, value) in response.headers {
        builder = builder.header(HeaderName::try_from(name)?, HeaderValue::try_from(value)?);
    }
    builder
        .body(Body::from(response.body))
        .map_err(ProcessError::Response)
}

struct TailRecord<'a> {
    tail: &'a Path,
    service: &'a str,
    method: String,
    path: String,
    status: u16,
    started: Instant,
    request: &'a str,
    dispatch: &'a str,
    traceparent: Option<&'a str>,
}

fn record_tail(record: TailRecord<'_>) -> Result<(), ProcessError> {
    tail::append(
        record.tail,
        &tail::Event::Request(tail::RequestEvent {
            service: record.service.to_string(),
            request_id: Some(record.request.to_string()),
            dispatch_id: Some(record.dispatch.to_string()),
            traceparent: record.traceparent.map(str::to_string),
            method: record.method,
            path: record.path,
            status: record.status,
            outcome: if record.status >= 500 { "error" } else { "ok" }.into(),
            wall_time_ms: u64::try_from(record.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }),
    )
    .map_err(ProcessError::Tail)
}
