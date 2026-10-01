use crate::error::MigrateError;
use peren_config::Binding;
use std::collections::HashMap;

pub(crate) const OPERATOR_INPUT_REQUIRED: &str = "OPERATOR-INPUT-REQUIRED";

#[derive(Debug, Clone)]
pub struct MigratedDeployment {
    pub worker_name: String,
    pub main_module_path: String,
    pub compatibility_date: Option<String>,
    pub compatibility_flags: Vec<String>,
    pub bindings: HashMap<String, Binding>,
    pub plain_vars: HashMap<String, String>,
    pub cron_expressions: Vec<String>,
    pub queue_consumers: Vec<String>,
    pub warnings: Vec<MigrationWarning>,
    pub assets_directory: Option<String>,
    pub assets_run_worker_first: Option<MigratedRunWorkerFirst>,
}

#[derive(Debug, Clone)]
pub enum MigratedRunWorkerFirst {
    Always(bool),
    Patterns(Vec<String>),
}

impl MigratedDeployment {
    pub fn validate_ready_to_deploy(&self) -> Result<(), MigrateError> {
        for (name, binding) in &self.bindings {
            let unresolved = match binding {
                Binding::R2Bucket {
                    endpoint,
                    credential_scope,
                    ..
                } => [endpoint.as_str(), credential_scope.as_str()]
                    .into_iter()
                    .find(|f| f.starts_with(OPERATOR_INPUT_REQUIRED)),
                Binding::Vectorize {
                    endpoint,
                    credential_scope,
                    ..
                }
                | Binding::Hyperdrive {
                    pgcat_endpoint: endpoint,
                    credential_scope,
                    ..
                }
                | Binding::Ai {
                    endpoint,
                    credential_scope,
                    ..
                }
                | Binding::MtlsCertificate {
                    cert_pem_env: endpoint,
                    key_pem_env: credential_scope,
                } => [endpoint.as_str(), credential_scope.as_str()]
                    .into_iter()
                    .find(|f| f.starts_with(OPERATOR_INPUT_REQUIRED)),
                Binding::AnalyticsEngine {
                    credential_scope, ..
                } if credential_scope.starts_with(OPERATOR_INPUT_REQUIRED) => {
                    Some(credential_scope.as_str())
                }
                _ => None,
            };
            if let Some(field) = unresolved {
                return Err(MigrateError::UnresolvedOperatorInput {
                    binding_name: name.clone(),
                    field: field.to_string(),
                });
            }
            if let Binding::Container {
                default_port: 0, ..
            } = binding
            {
                return Err(MigrateError::UnresolvedOperatorInput {
                    binding_name: name.clone(),
                    field: "default_port".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum MigrationWarning {
    PartiallySupportedCompatFlag {
        flag: String,
        note: &'static str,
    },
    ObservabilityNotAutoTranslated,
    NeedsOperatorInput {
        binding_name: String,
        field: &'static str,
    },
    UnmappedTrigger {
        event_name: String,
        event_type: String,
    },
    AssetsFieldNotTranslated {
        field: &'static str,
    },
    WorkerLoaderFieldNotTranslated {
        binding_name: String,
        field: String,
    },
    DispatchNamespaceFieldNotTranslated {
        binding_name: String,
        field: String,
    },
    FieldNotTranslated {
        field: String,
        reason: &'static str,
    },
}

impl std::fmt::Display for MigrationWarning {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartiallySupportedCompatFlag { flag, note } => {
                write!(formatter, "compatibility flag {flag:?} is partial: {note}")
            }
            Self::ObservabilityNotAutoTranslated => {
                formatter.write_str("observability requires explicit Peren exporter configuration")
            }
            Self::NeedsOperatorInput {
                binding_name,
                field,
            } => write!(formatter, "binding {binding_name:?} requires {field}"),
            Self::UnmappedTrigger {
                event_name,
                event_type,
            } => write!(
                formatter,
                "trigger {event_name:?} of type {event_type:?} requires explicit routing"
            ),
            Self::AssetsFieldNotTranslated { field } => {
                write!(formatter, "asset field {field:?} has no Peren translation")
            }
            Self::WorkerLoaderFieldNotTranslated {
                binding_name,
                field,
            }
            | Self::DispatchNamespaceFieldNotTranslated {
                binding_name,
                field,
            } => write!(
                formatter,
                "binding {binding_name:?} field {field:?} has no Peren translation"
            ),
            Self::FieldNotTranslated { field, reason } => {
                write!(formatter, "field {field:?} was not translated: {reason}")
            }
        }
    }
}
