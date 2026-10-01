use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use peren_config::ValidatedConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::Environment;

const FILE: &str = "events.jsonl";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Level {
    Log,
    Warn,
    Error,
}

#[derive(Debug)]
pub struct Read {
    pub service: String,
    pub level: Option<Level>,
}

#[derive(Debug)]
pub struct Report {
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    Request(RequestEvent),
    Console(ConsoleEvent),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestEvent {
    pub service: String,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub dispatch_id: Option<String>,
    #[serde(default)]
    pub traceparent: Option<String>,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub outcome: String,
    pub wall_time_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConsoleEvent {
    pub service: String,
    #[serde(default)]
    pub cell: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    pub event: String,
    pub level: ConsoleLevel,
    pub message: String,
    pub timestamp_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl ConsoleLevel {
    #[must_use]
    pub const fn otel_severity_number(self) -> u8 {
        match self {
            Self::Debug => 5,
            Self::Info => 9,
            Self::Warn => 13,
            Self::Error => 17,
        }
    }

    #[must_use]
    pub const fn otel_severity_text(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

pub(crate) fn append(root: &Path, event: &Event) -> Result<(), TailError> {
    fs::create_dir_all(root).map_err(|source| TailError::Create {
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
        .map_err(|source| TailError::Write {
            path: path.clone(),
            source,
        })?;
    file.write_all(&line)
        .map_err(|source| TailError::Write { path, source })
}

pub fn read(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: &Read,
) -> Result<Report, TailError> {
    if !config
        .raw
        .services
        .iter()
        .any(|service| service.name == request.service)
    {
        return Err(TailError::Service(request.service.clone()));
    }
    let path = root(environment).join(FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(source) => return Err(TailError::Read { path, source }),
    };
    let mut events = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let event: Event = serde_json::from_str(line)?;
        if event.service() == request.service && allowed(request.level, &event) {
            events.push(event);
        }
    }
    Ok(Report { events })
}

pub fn read_recent(root: &Path, limit: usize) -> Result<Report, TailError> {
    let path = root.join(FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(source) => return Err(TailError::Read { path, source }),
    };
    let mut events = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        events.push(serde_json::from_str(line)?);
    }
    if events.len() > limit {
        events.drain(..events.len() - limit);
    }
    Ok(Report { events })
}

impl Event {
    #[must_use]
    pub fn service(&self) -> &str {
        match self {
            Event::Request(event) => &event.service,
            Event::Console(event) => &event.service,
        }
    }
}

fn allowed(level: Option<Level>, event: &Event) -> bool {
    match (level, event) {
        (None | Some(Level::Log), _) => true,
        (Some(Level::Warn), Event::Request(event)) => event.status >= 400,
        (Some(Level::Error), Event::Request(event)) => event.status >= 500,
        (Some(Level::Warn), Event::Console(event)) => event.level >= ConsoleLevel::Warn,
        (Some(Level::Error), Event::Console(event)) => event.level >= ConsoleLevel::Error,
    }
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(super::process::default_data, PathBuf::from)
        .join("tail")
}

#[derive(Debug, Error)]
pub enum TailError {
    #[error("service {0:?} is not present in the fleet config")]
    Service(String),
    #[error("failed to create tail directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write tail event log {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read tail event log {path:?}")]
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
    use uuid::Uuid;

    #[test]
    fn console_levels_have_stable_otel_severity_mapping() {
        assert_eq!(ConsoleLevel::Debug.otel_severity_number(), 5);
        assert_eq!(ConsoleLevel::Debug.otel_severity_text(), "debug");
        assert_eq!(ConsoleLevel::Info.otel_severity_number(), 9);
        assert_eq!(ConsoleLevel::Info.otel_severity_text(), "info");
        assert_eq!(ConsoleLevel::Warn.otel_severity_number(), 13);
        assert_eq!(ConsoleLevel::Warn.otel_severity_text(), "warn");
        assert_eq!(ConsoleLevel::Error.otel_severity_number(), 17);
        assert_eq!(ConsoleLevel::Error.otel_severity_text(), "error");
    }

    #[test]
    fn appends_and_filters_tail_events() {
        let temp = std::env::temp_dir().join(format!("peren-tail-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp).unwrap();
        let config = config(&temp.join("worker.js"));
        let env = DataEnv::new(temp.join("data"));
        append(
            &env.path().join("tail"),
            &Event::Request(RequestEvent {
                service: "api".into(),
                request_id: Some("request-1".into()),
                dispatch_id: Some("dispatch-1".into()),
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
                method: "GET".into(),
                path: "/ok".into(),
                status: 200,
                outcome: "ok".into(),
                wall_time_ms: 3,
            }),
        )
        .unwrap();
        append(
            &env.path().join("tail"),
            &Event::Request(RequestEvent {
                service: "api".into(),
                request_id: Some("request-2".into()),
                dispatch_id: Some("dispatch-2".into()),
                traceparent: None,
                method: "POST".into(),
                path: "/fail".into(),
                status: 500,
                outcome: "error".into(),
                wall_time_ms: 9,
            }),
        )
        .unwrap();
        append(
            &env.path().join("tail"),
            &Event::Console(ConsoleEvent {
                service: "api".into(),
                cell: None,
                request_id: Some("dispatch-1".into()),
                event: "fetch".into(),
                level: ConsoleLevel::Warn,
                message: "careful".into(),
                timestamp_ms: 10,
            }),
        )
        .unwrap();

        let all = read(
            &config,
            &env,
            &Read {
                service: "api".into(),
                level: None,
            },
        )
        .unwrap();
        let errors = read(
            &config,
            &env,
            &Read {
                service: "api".into(),
                level: Some(Level::Error),
            },
        )
        .unwrap();
        let warnings = read(
            &config,
            &env,
            &Read {
                service: "api".into(),
                level: Some(Level::Warn),
            },
        )
        .unwrap();

        assert_eq!(all.events.len(), 3);
        assert_eq!(errors.events.len(), 1);
        assert!(matches!(&errors.events[0], Event::Request(event) if event.path == "/fail"));
        assert_eq!(warnings.events.len(), 2);
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn reads_recent_tail_events_without_a_service_filter() {
        let temp = std::env::temp_dir().join(format!("peren-tail-recent-{}", Uuid::new_v4()));
        let tail = temp.join("tail");
        fs::create_dir_all(&tail).unwrap();
        for index in 0..3 {
            append(
                &tail,
                &Event::Request(RequestEvent {
                    service: "api".into(),
                    request_id: Some(format!("request-{index}")),
                    dispatch_id: None,
                    traceparent: None,
                    method: "GET".into(),
                    path: format!("/{index}"),
                    status: 200,
                    outcome: "ok".into(),
                    wall_time_ms: index,
                }),
            )
            .unwrap();
        }

        let recent = read_recent(&tail, 2).unwrap();

        assert_eq!(recent.events.len(), 2);
        assert!(matches!(
            &recent.events[0],
            Event::Request(event) if event.path == "/1"
        ));
        assert!(matches!(
            &recent.events[1],
            Event::Request(event) if event.path == "/2"
        ));
        fs::remove_dir_all(temp).unwrap();
    }

    fn config(bundle: &std::path::Path) -> ValidatedConfig {
        fs::write(
            bundle,
            "export default { fetch() { return new Response('ok') } };",
        )
        .unwrap();
        let text = format!(
            r#"
[node]
node_id = "{}"
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
            Uuid::new_v4(),
            bundle.display()
        );
        FleetConfig::from_toml(&text).unwrap().validate().unwrap()
    }
}
