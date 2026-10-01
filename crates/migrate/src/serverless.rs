use crate::error::MigrateError;
use crate::iam::{IamMapping, arn_resource_name, arn_service, map_scoped_resource};
use crate::{
    migration::{MigratedDeployment, MigrationWarning},
    path::validate_relative_module_path,
};
use peren_config::Binding;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Deserialize)]
struct ServerlessYml {
    service: String,
    #[serde(default)]
    provider: ServerlessProvider,
    #[serde(default)]
    functions: BTreeMap<String, ServerlessFunction>,
}

#[derive(Debug, Default, Deserialize)]
struct ServerlessProvider {
    #[serde(default)]
    runtime: Option<String>,
    #[serde(default, rename = "iamRoleStatements")]
    iam_role_statements: Vec<IamRoleStatement>,
}

#[derive(Debug, Deserialize)]
struct IamRoleStatement {
    #[serde(default, rename = "Effect")]
    effect: Option<String>,
    #[serde(default, rename = "Action")]
    action: Option<StringOrList>,
    #[serde(default, rename = "Resource")]
    resource: Option<StringOrList>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum StringOrList {
    One(String),
    Many(Vec<String>),
}

impl StringOrList {
    #[must_use]
    fn as_vec(&self) -> Vec<String> {
        match self {
            StringOrList::One(s) => vec![s.clone()],
            StringOrList::Many(v) => v.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ServerlessFunction {
    handler: String,
    #[serde(default)]
    events: Vec<serde_yaml::Value>,
    #[serde(default)]
    environment: HashMap<String, String>,
}

fn parse_serverless_yml(text: &str) -> Result<ServerlessYml, MigrateError> {
    crate::refuse_if_source_too_large("serverless.yml", text)?;
    crate::yaml::refuse_if_yaml_has_anchors_or_aliases(text).map_err(|reason| {
        MigrateError::UnsafeYamlConstruct {
            format: "serverless.yml",
            reason,
        }
    })?;
    serde_yaml::from_str(text).map_err(|e| MigrateError::ParseYaml {
        format: "serverless.yml",
        message: e.to_string(),
    })
}

fn capabilities_from_iam_statements(
    statements: &[IamRoleStatement],
) -> Result<(HashMap<String, Binding>, Vec<MigrationWarning>), MigrateError> {
    let mut bindings = HashMap::new();
    let mut warnings = Vec::new();

    for (i, stmt) in statements.iter().enumerate() {
        if stmt.effect.as_deref() != Some("Allow") {
            continue; // a Deny statement grants nothing to map onto a binding
        }
        let resources = stmt
            .resource
            .as_ref()
            .map(StringOrList::as_vec)
            .unwrap_or_default();
        let actions = stmt
            .action
            .as_ref()
            .map(StringOrList::as_vec)
            .unwrap_or_default();

        for resource in &resources {
            let context = format!("iamRoleStatements[{i}]");
            if resource.as_str() == "*" {
                return Err(MigrateError::UnsupportedLambdaDirective(format!(
                    "{context}: wildcard Resource \"*\" (actions {actions:?}) grants unscoped \
                     access — peren's bindings are always scoped to one named resource, so a \
                     wildcard-resource statement has no safe automatic mapping"
                )));
            }

            let Some(service) = arn_service(resource) else {
                return Err(MigrateError::UnsupportedLambdaDirective(format!(
                    "{context}: resource {resource:?} is not a recognized ARN shape"
                )));
            };
            let resource_name = arn_resource_name(resource);

            match map_scoped_resource(service, &resource_name, &context) {
                IamMapping::Binding {
                    suggested_name,
                    binding,
                } => {
                    if let Binding::R2Bucket { .. } = &*binding {
                        warnings.push(MigrationWarning::NeedsOperatorInput {
                            binding_name: suggested_name.clone(),
                            field: "endpoint (peren needs a real S3-compatible endpoint URL — no AWS config carries this)",
                        });
                        warnings.push(MigrationWarning::NeedsOperatorInput {
                            binding_name: suggested_name.clone(),
                            field: "credential_scope (a ScopedCredential name the operator must mint separately)",
                        });
                    }
                    bindings.insert(suggested_name, *binding);
                }
                IamMapping::Refused(reason) => {
                    return Err(MigrateError::UnsupportedLambdaDirective(reason));
                }
            }
        }
    }

    Ok((bindings, warnings))
}

fn event_trigger(event: &serde_yaml::Value) -> Option<MigrationWarning> {
    let mapping = event.as_mapping()?;
    let (event_type, _) = mapping.iter().next()?;
    Some(MigrationWarning::UnmappedTrigger {
        event_name: event_type.as_str().unwrap_or("?").to_string(),
        event_type: event_type.as_str().unwrap_or("?").to_string(),
    })
}

pub fn import_serverless_yml(text: &str) -> Result<Vec<MigratedDeployment>, MigrateError> {
    let config = parse_serverless_yml(text)?;
    let (shared_bindings, shared_warnings) =
        capabilities_from_iam_statements(&config.provider.iam_role_statements)?;
    let runtime_warning =
        config
            .provider
            .runtime
            .as_ref()
            .map(|_| MigrationWarning::FieldNotTranslated {
                field: "provider.runtime".to_string(),
                reason: "the Lambda runtime does not select Peren's JavaScript runtime",
            });

    let mut deployments = Vec::new();
    for (fn_name, func) in &config.functions {
        if let Err(reason) = validate_relative_module_path(&func.handler) {
            return Err(MigrateError::UnsafeModulePath {
                path: func.handler.clone(),
                reason,
            });
        }

        let mut warnings = shared_warnings.clone();
        warnings.extend(runtime_warning.clone());
        if !shared_bindings.is_empty() {
            warnings.push(MigrationWarning::NeedsOperatorInput {
                binding_name: format!("{}-{fn_name}", config.service),
                field: "provider-level iamRoleStatements are shared across every function in this service per Serverless Framework's own single-role-per-service IAM model; the same derived bindings are wired into every function, which may overgrant relative to what this specific function actually touches — per-function iamRoleStatements overrides require an operator decision and are not inferred by this importer",
            });
        }
        for event in &func.events {
            if let Some(warning) = event_trigger(event) {
                warnings.push(warning);
            }
        }

        deployments.push(MigratedDeployment {
            worker_name: format!("{}-{fn_name}", config.service),
            main_module_path: func.handler.clone(),
            compatibility_date: None,
            compatibility_flags: Vec::new(),
            bindings: shared_bindings.clone(),
            plain_vars: func.environment.clone(),
            cron_expressions: Vec::new(),
            queue_consumers: Vec::new(),
            warnings,
            assets_directory: None,
            assets_run_worker_first: None,
        });
    }

    Ok(deployments)
}
