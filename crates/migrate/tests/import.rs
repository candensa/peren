use peren_config::Binding;
use peren_migrate::{
    MigrateError, MigrationWarning, WranglerFormat, import_sam_template, import_serverless_yml,
    import_wrangler,
};

#[test]
fn wrangler_translates_the_supported_service_contract() {
    let migration = import_wrangler(
        r#"
name = "api"
main = "dist/index.js"
compatibility_date = "2026-01-01"
compatibility_flags = ["nodejs_als"]
[vars]
MODE = "production"
[triggers]
crons = ["*/5 * * * *"]
[[kv_namespaces]]
binding = "CACHE"
id = "cloudflare-cache"
[[d1_databases]]
binding = "DB"
database_name = "app"
database_id = "cloudflare-database"
[[durable_objects.bindings]]
name = "ROOMS"
class_name = "Room"
[queues]
consumers = [{ queue = "jobs" }]
"#,
        WranglerFormat::Toml,
    )
    .unwrap();

    assert_eq!(migration.worker_name, "api");
    assert_eq!(migration.plain_vars["MODE"], "production");
    assert_eq!(migration.cron_expressions, ["0 */5 * * * *"]);
    assert!(matches!(migration.bindings["CACHE"], Binding::Kv { .. }));
    assert!(matches!(
        migration.bindings["DB"],
        Binding::D1Database { .. }
    ));
    assert!(matches!(
        migration.bindings["ROOMS"],
        Binding::DurableObjectNamespace { .. }
    ));
    assert_eq!(migration.queue_consumers, ["jobs"]);
    assert!(migration.warnings.iter().any(|warning| matches!(
        warning,
        MigrationWarning::FieldNotTranslated { field, .. }
            if field == "kv_namespaces[binding=CACHE].id"
    )));
}

#[test]
fn jsonc_comments_and_trailing_commas_are_accepted() {
    let migration = import_wrangler(
        r#"{
          // Wrangler permits JSONC.
          "name": "api",
          "main": "dist/index.js",
          "compatibility_date": "2026-01-01",
        }"#,
        WranglerFormat::Jsonc,
    )
    .unwrap();
    assert_eq!(migration.worker_name, "api");
}

#[test]
fn unknown_fields_and_unsupported_flags_never_disappear() {
    let migration = import_wrangler(
        r#"
name = "api"
main = "index.js"
mystery = true
"#,
        WranglerFormat::Toml,
    )
    .unwrap();
    assert!(migration.warnings.iter().any(|warning| matches!(
        warning,
        MigrationWarning::FieldNotTranslated { field, .. } if field == "mystery"
    )));

    let error = import_wrangler(
        r#"
name = "api"
main = "index.js"
compatibility_flags = ["unknown_runtime_mode"]
"#,
        WranglerFormat::Toml,
    )
    .unwrap_err();
    assert!(matches!(error, MigrateError::UnsupportedCompatFlags(_)));
}

#[test]
fn unsafe_module_paths_are_refused_at_import() {
    let error = import_wrangler(
        r#"
name = "api"
main = "../../etc/passwd"
"#,
        WranglerFormat::Toml,
    )
    .unwrap_err();
    assert!(matches!(error, MigrateError::UnsafeModulePath { .. }));
}

#[test]
fn unresolved_external_bindings_cannot_be_deployed() {
    let migration = import_wrangler(
        r#"
name = "api"
main = "index.js"
[[r2_buckets]]
binding = "FILES"
bucket_name = "files"
"#,
        WranglerFormat::Toml,
    )
    .unwrap();
    assert!(matches!(
        migration.validate_ready_to_deploy(),
        Err(MigrateError::UnresolvedOperatorInput { .. })
    ));
}

#[test]
fn sam_maps_scoped_resources_and_refuses_unscoped_policy() {
    let migrations = import_sam_template(
        r"
Resources:
  Worker:
    Type: AWS::Serverless::Function
    Properties:
      Handler: dist/index.js
      Policies:
        - SQSSendMessagePolicy:
            QueueName: jobs
",
    )
    .unwrap();
    assert_eq!(migrations.len(), 1);
    assert!(
        migrations[0].bindings.values().any(
            |binding| matches!(binding, Binding::Queue { queue_name } if queue_name == "jobs")
        )
    );

    let error = import_sam_template(
        r"
Resources:
  Worker:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.js
      Policies: [AmazonSQSFullAccess]
",
    )
    .unwrap_err();
    assert!(matches!(error, MigrateError::UnsupportedLambdaDirective(_)));
}

#[test]
fn serverless_maps_concrete_arns_and_refuses_wildcards() {
    let migrations = import_serverless_yml(
        r"
service: jobs
provider:
  iamRoleStatements:
    - Effect: Allow
      Action: sqs:SendMessage
      Resource: arn:aws:sqs:eu-west-1:123456789012:jobs
functions:
  worker:
    handler: dist/index.js
",
    )
    .unwrap();
    assert_eq!(migrations.len(), 1);

    let error = import_serverless_yml(
        r#"
service: jobs
provider:
  iamRoleStatements:
    - Effect: Allow
      Action: sqs:SendMessage
      Resource: "*"
functions: {}
"#,
    )
    .unwrap_err();
    assert!(matches!(error, MigrateError::UnsupportedLambdaDirective(_)));
}

#[test]
fn lambda_runtime_is_reported_instead_of_silently_reinterpreted() {
    let migrations = import_serverless_yml(
        r"
service: jobs
provider:
  runtime: nodejs22.x
functions:
  worker:
    handler: index.js
",
    )
    .unwrap();

    assert!(migrations[0].compatibility_date.is_none());
    assert!(migrations[0].warnings.iter().any(|warning| matches!(
        warning,
        MigrationWarning::FieldNotTranslated { field, .. } if field == "provider.runtime"
    )));
}

#[test]
fn yaml_aliases_are_refused_before_deserialization() {
    let error = import_serverless_yml(
        r"
service: unsafe
provider: &provider
  runtime: nodejs22.x
functions:
  worker:
    handler: index.js
    environment: *provider
",
    )
    .unwrap_err();
    assert!(matches!(error, MigrateError::UnsafeYamlConstruct { .. }));
}

#[test]
fn wrangler_required_secrets_become_safe_notices_not_values() {
    let migration = import_wrangler(
        r#"
name = "api"
main = "index.js"
[secrets]
required = ["STRIPE_SECRET_KEY"]
[vars]
PUBLIC_VALUE = "visible"
"#,
        WranglerFormat::Toml,
    )
    .unwrap();

    assert_eq!(
        migration.plain_vars.get("PUBLIC_VALUE"),
        Some(&"visible".into())
    );
    assert!(!migration.plain_vars.contains_key("STRIPE_SECRET_KEY"));
    assert!(migration.warnings.iter().any(|warning| matches!(
        warning,
        MigrationWarning::FieldNotTranslated { field, reason }
            if field == "secrets.required[STRIPE_SECRET_KEY]"
                && reason.contains("never carries the actual")
                && !reason.contains("visible")
    )));
}
