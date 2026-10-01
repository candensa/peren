use crate::migration::OPERATOR_INPUT_REQUIRED;
use peren_config::Binding;

pub(crate) enum IamMapping {
    Binding {
        suggested_name: String,
        binding: Box<Binding>,
    },
    Refused(String),
}

pub(crate) fn arn_service(arn: &str) -> Option<&str> {
    let mut parts = arn.splitn(6, ':');
    if parts.next() != Some("arn") {
        return None;
    }
    parts.next()?; // partition
    parts.next()
}

pub(crate) fn arn_resource_name(arn: &str) -> String {
    let resource_part = arn.splitn(6, ':').nth(5).unwrap_or(arn);
    let first_segment = resource_part.split('/').next().unwrap_or(resource_part);
    first_segment.to_string()
}

pub(crate) fn map_scoped_resource(service: &str, resource_name: &str, context: &str) -> IamMapping {
    match service {
        "sqs" => IamMapping::Binding {
            suggested_name: synthesize_binding_name(resource_name),
            binding: Box::new(Binding::Queue {
                queue_name: resource_name.to_string(),
            }),
        },
        "s3" => IamMapping::Binding {
            suggested_name: synthesize_binding_name(resource_name),
            binding: Box::new(Binding::R2Bucket {
                endpoint: format!("{OPERATOR_INPUT_REQUIRED}-endpoint"),
                bucket: resource_name.to_string(),
                credential_scope: format!(
                    "{OPERATOR_INPUT_REQUIRED}-credential_scope-{resource_name}"
                ),
                region: None,
                access_key_env: None,
                secret_key_env: None,
                token_env: None,
                allow_http: false,
                prefix: None,
                notifications: Vec::new(),
            }),
        },
        "dynamodb" => IamMapping::Refused(format!(
            "{context}: peren has no DynamoDB-equivalent binding — D1 (SQLite) is not a safe \
             analog for DynamoDB's data model, so this is refused rather than silently mapped to \
             the wrong storage type"
        )),
        other => IamMapping::Refused(format!(
            "{context}: AWS service {other:?} (resource {resource_name:?}) has no entry in \
             peren-migrate's IAM mapping table yet"
        )),
    }
}

pub(crate) fn refuse_unscoped_managed_policy(policy_name: &str, context: &str) -> String {
    format!(
        "{context}: managed policy {policy_name:?} grants unscoped, account-wide access to a \
         resource class, not one specific named resource — peren's bindings are always scoped to a \
         single named queue/bucket/database, so accepting this would either silently overgrant or \
         require guessing which specific resource was meant; use a scoped reference (a SAM policy \
         template naming QueueName/BucketName, or a concrete-ARN IAM statement) instead"
    )
}

fn synthesize_binding_name(resource_name: &str) -> String {
    let mut out = String::with_capacity(resource_name.len());
    let mut last_was_sep = false;
    for ch in resource_name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            last_was_sep = false;
        } else if !last_was_sep {
            out.push('_');
            last_was_sep = true;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "BINDING".to_string()
    } else {
        trimmed.to_string()
    }
}
