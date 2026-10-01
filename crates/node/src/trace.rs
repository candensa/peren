use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use peren_config::{Otlp, ValidatedConfig};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use uuid::Uuid;

use crate::{Environment, Shutdown, TaskError};

const FILE: &str = "events.jsonl";

#[derive(Clone, Debug)]
pub(crate) struct TraceSink {
    root: Arc<PathBuf>,
    exporter: Option<mpsc::Sender<Event>>,
}

impl TraceSink {
    #[must_use]
    pub(crate) fn local(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
            exporter: None,
        }
    }

    #[must_use]
    pub(crate) fn with_exporter(root: PathBuf, exporter: mpsc::Sender<Event>) -> Self {
        Self {
            root: Arc::new(root),
            exporter: Some(exporter),
        }
    }

    pub(crate) fn append(&self, event: Event) -> Result<(), TraceError> {
        append(&self.root, &event)?;
        if let Some(exporter) = &self.exporter {
            let _ = exporter.try_send(event);
        }
        Ok(())
    }

    pub(crate) fn record_span(
        &self,
        context: &TraceContext,
        span: SpanRecord<'_>,
    ) -> Result<(), TraceError> {
        if !context.sampled {
            return Ok(());
        }
        self.append(Event {
            service: span.service.to_string(),
            cell: span.cell.map(ToString::to_string),
            trace_id: context.trace_id.clone(),
            span_id: context.span_id.clone(),
            parent_span_id: context.parent_span_id.clone(),
            request_id: context.request_id.clone(),
            dispatch_id: context.dispatch_id.clone(),
            traceparent: Some(context.traceparent()),
            name: span.name.to_string(),
            kind: span.kind,
            outcome: span.outcome.to_string(),
            started_at_ms: span.started_at_ms,
            duration_ms: u64::try_from(span.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            attributes: span.attributes,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Event {
    pub service: String,
    #[serde(default)]
    pub cell: Option<String>,
    pub trace_id: String,
    pub span_id: String,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub dispatch_id: Option<String>,
    #[serde(default)]
    pub traceparent: Option<String>,
    pub name: String,
    pub kind: SpanKind,
    pub outcome: String,
    pub started_at_ms: u64,
    pub duration_ms: u64,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanKind {
    Internal,
    Server,
    Client,
}

#[derive(Clone, Debug)]
pub(crate) struct TraceContext {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    request_id: Option<String>,
    dispatch_id: Option<String>,
    sampled: bool,
}

impl TraceContext {
    #[must_use]
    pub(crate) fn ingress(
        inbound: Option<&str>,
        request_id: String,
        dispatch_id: String,
        sampling_ratio: f64,
    ) -> Self {
        if let Some(parent) = inbound.and_then(parse_traceparent) {
            return Self {
                trace_id: parent.trace_id,
                span_id: new_span_id(),
                parent_span_id: Some(parent.span_id),
                request_id: Some(request_id),
                dispatch_id: Some(dispatch_id),
                sampled: parent.sampled && sampled(sampling_ratio),
            };
        }
        Self {
            trace_id: new_trace_id(),
            span_id: new_span_id(),
            parent_span_id: None,
            request_id: Some(request_id),
            dispatch_id: Some(dispatch_id),
            sampled: sampled(sampling_ratio),
        }
    }

    #[must_use]
    pub(crate) fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: new_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            request_id: self.request_id.clone(),
            dispatch_id: self.dispatch_id.clone(),
            sampled: self.sampled,
        }
    }

    #[must_use]
    pub(crate) fn traceparent(&self) -> String {
        let flags = if self.sampled { "01" } else { "00" };
        format!("00-{}-{}-{flags}", self.trace_id, self.span_id)
    }
}

pub(crate) struct SpanRecord<'a> {
    pub(crate) service: &'a str,
    pub(crate) cell: Option<&'a str>,
    pub(crate) name: &'a str,
    pub(crate) kind: SpanKind,
    pub(crate) started: Instant,
    pub(crate) started_at_ms: u64,
    pub(crate) outcome: &'a str,
    pub(crate) attributes: BTreeMap<String, String>,
}

#[derive(Debug)]
struct ParentContext {
    trace_id: String,
    span_id: String,
    sampled: bool,
}

fn parse_traceparent(value: &str) -> Option<ParentContext> {
    let parts: Vec<_> = value.split('-').collect();
    if parts.len() != 4
        || parts[0].len() != 2
        || parts[1].len() != 32
        || parts[2].len() != 16
        || parts[3].len() != 2
        || !parts
            .iter()
            .all(|part| part.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return None;
    }
    Some(ParentContext {
        trace_id: parts[1].to_ascii_lowercase(),
        span_id: parts[2].to_ascii_lowercase(),
        sampled: u8::from_str_radix(parts[3], 16).is_ok_and(|flags| flags & 1 == 1),
    })
}

fn new_trace_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn new_span_id() -> String {
    Uuid::new_v4().simple().to_string()[..16].to_string()
}

fn sampled(ratio: f64) -> bool {
    ratio >= 1.0 || (ratio > 0.0 && rand::random::<f64>() < ratio)
}

#[must_use]
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub(crate) fn append(root: &Path, event: &Event) -> Result<(), TraceError> {
    fs::create_dir_all(root).map_err(|source| TraceError::Create {
        path: root.to_path_buf(),
        source,
    })?;
    let path = root.join(FILE);
    let mut line = serde_json::to_vec(event)?;
    line.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|source| TraceError::Write {
            path: path.clone(),
            source,
        })?;
    file.write_all(&line)
        .map_err(|source| TraceError::Write { path, source })
}

#[derive(Debug)]
pub struct Read {
    pub service: Option<String>,
    pub trace_id: Option<String>,
    pub request_id: Option<String>,
    pub limit: usize,
}

#[derive(Debug)]
pub struct Report {
    pub events: Vec<Event>,
}

pub fn read(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Read,
) -> Result<Report, TraceError> {
    if let Some(service) = &request.service
        && !config
            .raw
            .services
            .iter()
            .any(|candidate| &candidate.name == service)
    {
        return Err(TraceError::Service(service.clone()));
    }
    let path = root(environment).join(FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(source) => return Err(TraceError::Read { path, source }),
    };
    let mut events = text
        .lines()
        .rev()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .filter_map(Result::ok)
        .filter(|event: &Event| {
            request
                .service
                .as_ref()
                .is_none_or(|service| &event.service == service)
                && request
                    .trace_id
                    .as_ref()
                    .is_none_or(|trace_id| &event.trace_id == trace_id)
                && request
                    .request_id
                    .as_ref()
                    .is_none_or(|request_id| event.request_id.as_ref() == Some(request_id))
        })
        .take(request.limit.max(1))
        .collect::<Vec<_>>();
    events.reverse();
    Ok(Report { events })
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(super::process::default_data, PathBuf::from)
        .join("traces")
}

pub(crate) struct Exporter {
    receiver: mpsc::Receiver<Event>,
    config: Otlp,
}

impl Exporter {
    #[must_use]
    pub(crate) fn new(config: Otlp) -> (mpsc::Sender<Event>, Self) {
        let capacity = config.channel_capacity.max(1);
        let (sender, receiver) = mpsc::channel(capacity);
        (sender, Self { receiver, config })
    }

    pub(crate) async fn run(mut self, mut shutdown: Shutdown) -> Result<(), TaskError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.config.request_timeout_secs.max(1)))
            .build()
            .map_err(|error| TaskError::new(error.to_string()))?;
        let mut batch = Vec::new();
        let mut interval =
            time::interval(Duration::from_secs(self.config.flush_interval_secs.max(1)));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.wait() => break,
                _ = interval.tick() => {
                    flush(&client, &self.config, &mut batch).await;
                }
                event = self.receiver.recv() => {
                    match event {
                        Some(event) => {
                            batch.push(event);
                            if batch.len() >= self.config.batch_max_spans.max(1) {
                                flush(&client, &self.config, &mut batch).await;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        flush(&client, &self.config, &mut batch).await;
        Ok(())
    }
}

async fn flush(client: &reqwest::Client, config: &Otlp, batch: &mut Vec<Event>) {
    if batch.is_empty() {
        return;
    }
    let payload = otlp_payload(config, batch);
    for attempt in 0..=config.max_retries {
        let mut request = client.post(&config.endpoint).json(&payload);
        for (key, value) in &config.headers {
            request = request.header(key, value);
        }
        if request
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            batch.clear();
            return;
        }
        let delay = config
            .retry_base_delay_ms
            .saturating_mul(u64::from(attempt + 1));
        time::sleep(Duration::from_millis(delay)).await;
    }
    batch.clear();
}

fn otlp_payload(config: &Otlp, events: &[Event]) -> serde_json::Value {
    let spans = events.iter().map(span_json).collect::<Vec<_>>();
    json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [{
                    "key": "service.name",
                    "value": { "stringValue": config.service_name }
                }]
            },
            "scopeSpans": [{
                "scope": { "name": "peren.cell_lifecycle" },
                "spans": spans
            }]
        }]
    })
}

fn span_json(event: &Event) -> serde_json::Value {
    let mut attributes = vec![
        otlp_attr("peren.service", &event.service),
        otlp_attr("peren.outcome", &event.outcome),
    ];
    if let Some(cell) = &event.cell {
        attributes.push(otlp_attr("peren.cell", cell));
    }
    if let Some(request_id) = &event.request_id {
        attributes.push(otlp_attr("peren.request_id", request_id));
    }
    if let Some(dispatch_id) = &event.dispatch_id {
        attributes.push(otlp_attr("peren.dispatch_id", dispatch_id));
    }
    attributes.extend(
        event
            .attributes
            .iter()
            .map(|(key, value)| otlp_attr(key, value)),
    );
    json!({
        "traceId": event.trace_id,
        "spanId": event.span_id,
        "parentSpanId": event.parent_span_id,
        "name": event.name,
        "kind": match event.kind {
            SpanKind::Internal => 1,
            SpanKind::Server => 2,
            SpanKind::Client => 3,
        },
        "startTimeUnixNano": event.started_at_ms.saturating_mul(1_000_000).to_string(),
        "endTimeUnixNano": event.started_at_ms.saturating_add(event.duration_ms).saturating_mul(1_000_000).to_string(),
        "attributes": attributes,
        "status": {
            "code": if event.outcome == "error" { 2 } else { 1 }
        }
    })
}

fn otlp_attr(key: &str, value: &str) -> serde_json::Value {
    json!({ "key": key, "value": { "stringValue": value } })
}

#[derive(Debug, Error)]
pub enum TraceError {
    #[error("service {0:?} is not present in the fleet config")]
    Service(String),
    #[error("failed to create trace directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write trace event log {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read trace event log {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::DataEnv;
    use peren_config::FleetConfig;

    #[test]
    fn ingress_context_accepts_w3c_parent() {
        let context = TraceContext::ingress(
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            "request".into(),
            "dispatch".into(),
            1.0,
        );
        assert_eq!(context.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(context.parent_span_id.as_deref(), Some("00f067aa0ba902b7"));
        assert!(context.sampled);
    }

    #[test]
    fn appends_and_filters_trace_events() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("worker.js"),
            "export default { fetch() { return new Response('ok') } };",
        )
        .unwrap();
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
"#,
            temp.path().join("worker.js").display()
        ))
        .unwrap()
        .validate()
        .unwrap();
        let env = DataEnv::new(temp.path().join("data"));
        append(
            &env.path().join("traces"),
            &Event {
                service: "api".into(),
                cell: Some("api:/".into()),
                trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
                span_id: "00f067aa0ba902b7".into(),
                parent_span_id: None,
                request_id: Some("request-1".into()),
                dispatch_id: Some("dispatch-1".into()),
                traceparent: None,
                name: "cell.dispatch".into(),
                kind: SpanKind::Server,
                outcome: "ok".into(),
                started_at_ms: 1,
                duration_ms: 2,
                attributes: BTreeMap::new(),
            },
        )
        .unwrap();
        let report = read(
            &config,
            &env,
            &Read {
                service: Some("api".into()),
                trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".into()),
                request_id: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(report.events.len(), 1);
    }
}
