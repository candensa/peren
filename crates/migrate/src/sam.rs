use crate::error::MigrateError;
use crate::iam::{IamMapping, map_scoped_resource, refuse_unscoped_managed_policy};
use crate::{
    migration::{MigratedDeployment, MigrationWarning},
    path::validate_relative_module_path,
};
use peren_config::Binding;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

const SERVERLESS_FUNCTION_TYPE: &str = "AWS::Serverless::Function";

#[derive(Debug, Deserialize)]
struct SamTemplate {
    #[serde(rename = "Resources")]
    resources: BTreeMap<String, SamResource>,
}

#[derive(Debug, Deserialize)]
struct SamResource {
    #[serde(rename = "Type")]
    resource_type: String,
    #[serde(rename = "Properties", default)]
    properties: Option<SamFunctionProperties>,
}

#[derive(Debug, Deserialize)]
struct SamFunctionProperties {
    #[serde(rename = "Handler")]
    handler: String,
    #[serde(rename = "Runtime", default)]
    runtime: Option<String>,
    #[serde(rename = "MemorySize", default)]
    memory_size: Option<u32>,
    #[serde(rename = "Timeout", default)]
    timeout: Option<u32>,
    #[serde(rename = "Policies", default)]
    policies: Vec<SamPolicy>,
    #[serde(rename = "Events", default)]
    events: BTreeMap<String, SamEvent>,
    #[serde(rename = "Environment", default)]
    environment: Option<SamEnvironment>,
}

#[derive(Debug, Deserialize)]
struct SamEnvironment {
    #[serde(rename = "Variables", default)]
    variables: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SamPolicy {
    Named(String),
    Template(BTreeMap<String, serde_yaml::Value>),
}

#[derive(Debug, Deserialize)]
struct SamEvent {
    #[serde(rename = "Type")]
    event_type: String,
}

fn parse_sam_template(text: &str) -> Result<SamTemplate, MigrateError> {
    crate::refuse_if_source_too_large("SAM template.yaml", text)?;
    crate::yaml::refuse_if_yaml_has_anchors_or_aliases(text).map_err(|reason| {
        MigrateError::UnsafeYamlConstruct {
            format: "SAM template.yaml",
            reason,
        }
    })?;
    serde_yaml::from_str(text).map_err(|e| MigrateError::ParseYaml {
        format: "SAM template.yaml",
        message: e.to_string(),
    })
}

pub fn import_sam_template(text: &str) -> Result<Vec<MigratedDeployment>, MigrateError> {
    let template = parse_sam_template(text)?;
    template
        .resources
        .iter()
        .filter(|(_, resource)| resource.resource_type == SERVERLESS_FUNCTION_TYPE)
        .map(|(name, resource)| deployment(name, resource))
        .collect()
}

fn deployment(name: &str, resource: &SamResource) -> Result<MigratedDeployment, MigrateError> {
    let properties = resource.properties.as_ref().ok_or_else(|| {
        MigrateError::UnsupportedLambdaDirective(format!(
            "{name}: declared as {SERVERLESS_FUNCTION_TYPE} but has no Properties block"
        ))
    })?;
    validate_relative_module_path(&properties.handler).map_err(|reason| {
        MigrateError::UnsafeModulePath {
            path: properties.handler.clone(),
            reason,
        }
    })?;
    let (bindings, mut warnings) = policy_bindings(name, &properties.policies)?;
    warnings.extend(properties.events.iter().map(|(event_name, event)| {
        MigrationWarning::UnmappedTrigger {
            event_name: event_name.clone(),
            event_type: event.event_type.clone(),
        }
    }));
    for (field, present) in [
        ("Runtime", properties.runtime.is_some()),
        ("MemorySize", properties.memory_size.is_some()),
        ("Timeout", properties.timeout.is_some()),
    ] {
        if present {
            warnings.push(MigrationWarning::FieldNotTranslated {
                field: format!("Resources.{name}.Properties.{field}"),
                reason: "AWS Lambda runtime limits require explicit Peren service configuration",
            });
        }
    }
    Ok(MigratedDeployment {
        worker_name: name.to_string(),
        main_module_path: properties.handler.clone(),
        compatibility_date: None,
        compatibility_flags: Vec::new(),
        bindings,
        plain_vars: properties
            .environment
            .as_ref()
            .map(|environment| environment.variables.clone())
            .unwrap_or_default(),
        cron_expressions: Vec::new(),
        queue_consumers: Vec::new(),
        warnings,
        assets_directory: None,
        assets_run_worker_first: None,
    })
}

fn policy_bindings(
    function: &str,
    policies: &[SamPolicy],
) -> Result<(HashMap<String, Binding>, Vec<MigrationWarning>), MigrateError> {
    let mut bindings = HashMap::new();
    let mut warnings = Vec::new();
    for policy in policies {
        let map = match policy {
            SamPolicy::Template(map) => map,
            SamPolicy::Named(name) => {
                return Err(MigrateError::UnsupportedLambdaDirective(
                    refuse_unscoped_managed_policy(name, function),
                ));
            }
        };
        let Some((template, parameters)) = map.iter().next() else {
            return Err(MigrateError::UnsupportedLambdaDirective(format!(
                "{function}: empty policy template entry"
            )));
        };
        let (service, key) = policy_target(function, template)?;
        let Some(resource) = yaml_str_param(parameters, key) else {
            return Err(MigrateError::UnsupportedLambdaDirective(format!(
                "{function}: {template} requires a literal {key} parameter"
            )));
        };
        match map_scoped_resource(service, &resource, function) {
            IamMapping::Binding {
                suggested_name,
                binding,
            } => {
                if matches!(*binding, Binding::R2Bucket { .. }) {
                    warnings.extend(r2_warnings(&suggested_name));
                }
                bindings.insert(suggested_name, *binding);
            }
            IamMapping::Refused(reason) => {
                return Err(MigrateError::UnsupportedLambdaDirective(reason));
            }
        }
    }
    Ok((bindings, warnings))
}

fn policy_target<'a>(
    function: &str,
    template: &'a str,
) -> Result<(&'a str, &'a str), MigrateError> {
    match template {
        "SQSPollerPolicy" | "SQSSendMessagePolicy" => Ok(("sqs", "QueueName")),
        "S3ReadPolicy" | "S3WritePolicy" | "S3CrudPolicy" => Ok(("s3", "BucketName")),
        "DynamoDBCrudPolicy" | "DynamoDBReadPolicy" | "DynamoDBWritePolicy" => {
            Err(MigrateError::UnsupportedLambdaDirective(format!(
                "{function}: {template} has no safe Peren binding equivalent"
            )))
        }
        _ => Err(MigrateError::UnsupportedLambdaDirective(format!(
            "{function}: unsupported SAM policy template {template:?}"
        ))),
    }
}

fn r2_warnings(name: &str) -> [MigrationWarning; 2] {
    [
        MigrationWarning::NeedsOperatorInput {
            binding_name: name.to_string(),
            field: "endpoint",
        },
        MigrationWarning::NeedsOperatorInput {
            binding_name: name.to_string(),
            field: "credential_scope",
        },
    ]
}

fn yaml_str_param(value: &serde_yaml::Value, key: &str) -> Option<String> {
    let mapping = value.as_mapping()?;
    for (k, v) in mapping {
        if k.as_str() == Some(key) {
            return v.as_str().map(std::string::ToString::to_string);
        }
    }
    None
}
