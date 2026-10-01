use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use peren_config::{Binding, ValidatedConfig};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Environment, process};

const FILE: &str = "workflows.json";

#[derive(Debug)]
pub struct Target {
    pub service: String,
    pub binding: String,
    pub instance: String,
}

#[derive(Debug)]
pub struct Cancel {
    pub target: Target,
    pub reason: Option<String>,
}

#[derive(Debug)]
pub struct Report {
    pub state: State,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct State {
    pub service: String,
    pub binding: String,
    pub instance: String,
    pub status: Status,
    pub reason: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Unknown,
    Canceled,
    Deleted,
}

pub fn status(
    config: &ValidatedConfig,
    environment: &impl Environment,
    target: Target,
) -> Result<Report, WorkflowError> {
    binding(config, &target)?;
    let registry = Registry::load(root(environment))?;
    Ok(Report {
        state: registry.find(&target).unwrap_or(State {
            service: target.service,
            binding: target.binding,
            instance: target.instance,
            status: Status::Unknown,
            reason: None,
            updated_at_ms: 0,
        }),
    })
}

pub fn cancel(
    config: &ValidatedConfig,
    environment: &impl Environment,
    request: Cancel,
) -> Result<Report, WorkflowError> {
    binding(config, &request.target)?;
    write_state(
        environment,
        State {
            service: request.target.service,
            binding: request.target.binding,
            instance: request.target.instance,
            status: Status::Canceled,
            reason: request.reason.filter(|reason| !reason.trim().is_empty()),
            updated_at_ms: now_ms()?,
        },
    )
}

pub fn delete(
    config: &ValidatedConfig,
    environment: &impl Environment,
    target: Target,
) -> Result<Report, WorkflowError> {
    binding(config, &target)?;
    write_state(
        environment,
        State {
            service: target.service,
            binding: target.binding,
            instance: target.instance,
            status: Status::Deleted,
            reason: None,
            updated_at_ms: now_ms()?,
        },
    )
}

fn write_state(environment: &impl Environment, state: State) -> Result<Report, WorkflowError> {
    let mut registry = Registry::load(root(environment))?;
    registry.upsert(state.clone());
    registry.save()?;
    Ok(Report { state })
}

fn binding<'a>(config: &'a ValidatedConfig, target: &Target) -> Result<&'a Binding, WorkflowError> {
    let service = config
        .raw
        .services
        .iter()
        .find(|service| service.name == target.service)
        .ok_or_else(|| WorkflowError::Service(target.service.clone()))?;
    let binding = service
        .bindings
        .get(&target.binding)
        .ok_or_else(|| WorkflowError::Binding {
            service: target.service.clone(),
            binding: target.binding.clone(),
        })?;
    if matches!(binding, Binding::Workflow { .. }) {
        Ok(binding)
    } else {
        Err(WorkflowError::BindingKind {
            service: target.service.clone(),
            binding: target.binding.clone(),
        })
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Registry {
    states: Vec<State>,
    #[serde(skip)]
    root: PathBuf,
}

impl Registry {
    fn load(root: PathBuf) -> Result<Self, WorkflowError> {
        let path = root.join(FILE);
        match fs::read(&path) {
            Ok(bytes) => {
                let mut registry: Registry = serde_json::from_slice(&bytes)?;
                registry.root = root;
                Ok(registry)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                states: Vec::new(),
                root,
            }),
            Err(source) => Err(WorkflowError::Read { path, source }),
        }
    }

    fn save(&self) -> Result<(), WorkflowError> {
        fs::create_dir_all(&self.root).map_err(|source| WorkflowError::Create {
            path: self.root.clone(),
            source,
        })?;
        let path = self.root.join(FILE);
        fs::write(&path, serde_json::to_vec_pretty(self)?)
            .map_err(|source| WorkflowError::Write { path, source })
    }

    fn find(&self, target: &Target) -> Option<State> {
        self.states
            .iter()
            .find(|state| {
                state.service == target.service
                    && state.binding == target.binding
                    && state.instance == target.instance
            })
            .cloned()
    }

    fn upsert(&mut self, state: State) {
        if let Some(existing) = self.states.iter_mut().find(|existing| {
            existing.service == state.service
                && existing.binding == state.binding
                && existing.instance == state.instance
        }) {
            *existing = state;
        } else {
            self.states.push(state);
        }
        self.states.sort_by(|left, right| {
            left.service
                .cmp(&right.service)
                .then_with(|| left.binding.cmp(&right.binding))
                .then_with(|| left.instance.cmp(&right.instance))
        });
    }
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(process::default_data, PathBuf::from)
        .join("workflows")
}

fn now_ms() -> Result<i64, WorkflowError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkflowError::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| WorkflowError::Time)
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("service {0:?} is not present in the fleet config")]
    Service(String),
    #[error("service {service:?} has no binding {binding:?}")]
    Binding { service: String, binding: String },
    #[error("service {service:?} binding {binding:?} is not a workflow binding")]
    BindingKind { service: String, binding: String },
    #[error("system clock cannot produce a valid workflow timestamp")]
    Time,
    #[error("failed to read workflow registry {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create workflow registry directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write workflow registry {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
