use peren_config::{
    BucketKind, ConsoleBackend, CredentialsSource, FleetConfig, QueueBroker, QueueConsumer,
};

const MINIMAL: &str = include_str!("../../../tests/fixtures/current/config/minimal.toml");
const REPRESENTATIVE: &str =
    include_str!("../../../tests/fixtures/current/config/representative.toml");
const UNKNOWN_SERVICE: &str = include_str!("../../../tests/fixtures/current/config/invalid.toml");

#[test]
fn current_minimal_config_keeps_defaults() {
    let config = FleetConfig::from_toml(MINIMAL).unwrap().validate().unwrap();

    assert_eq!(config.raw.bucket.kind, BucketKind::Memory);
    assert!(config.raw.mtls.require_client_cert);
    assert!(!config.raw.control.require_signed_mutations);
    assert_eq!(config.raw.routing.ownership_cache_ttl_secs, 45);
    assert_eq!(config.raw.limits.request_body_bytes, 32 * 1024 * 1024);
    assert_eq!(config.services.len(), 1);
}

#[test]
fn eks_pod_identity_is_a_supported_s3_credential_source() {
    let source = MINIMAL.replace(
        "kind = \"memory\"",
        "kind = \"s3\"\nbucket = \"fleet\"\nendpoint = \"https://s3.us-east-1.amazonaws.com\"\ncredentials_source = \"eks_pod_identity\"",
    );

    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    assert_eq!(config.raw.bucket.kind, BucketKind::S3);
    assert_eq!(
        config.raw.bucket.credentials_source,
        CredentialsSource::EksPodIdentity
    );
}

#[test]
fn s3_bucket_static_credentials_require_distinct_environment_references() {
    let source = MINIMAL.replace(
        "kind = \"memory\"",
        r#"kind = "s3"
bucket = "fleet"
endpoint = "https://s3.us-east-1.amazonaws.com"
credentials_source = "configured"
access_key_env = "FLEET_BUCKET_KEY"
secret_key_env = "FLEET_BUCKET_KEY""#,
    );

    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(error.problems().iter().any(|problem| {
        problem.field == "bucket.secret_key_env"
            && problem.message.contains("different environment variable")
    }));
}

#[test]
fn azure_blob_bucket_rejects_ambient_credential_sources() {
    let source = MINIMAL.replace(
        "kind = \"memory\"",
        r#"kind = "azure_blob"
bucket = "fleet"
credentials_source = "workload_identity"
azure_account_env = "AZURE_STORAGE_ACCOUNT"
azure_access_key_env = "AZURE_STORAGE_ACCESS_KEY""#,
    );

    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(error.problems().iter().any(|problem| {
        problem.field == "bucket.credentials_source"
            && problem.message == "Azure Blob buckets require configured environment credentials"
    }));
}

#[test]
fn current_representative_config_resolves_references() {
    let config = FleetConfig::from_toml(REPRESENTATIVE)
        .unwrap()
        .validate()
        .unwrap();

    assert_eq!(config.services.len(), 2);
    assert_eq!(config.raw.sockets.len(), 2);
    assert!(config.raw.queues.is_some());
}

#[test]
fn validation_reports_unknown_socket_service() {
    let error = FleetConfig::from_toml(UNKNOWN_SERVICE)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(error.problems().iter().any(|problem| {
        problem.field == "sockets[0].service" && problem.message.contains("missing-service")
    }));
}

#[test]
fn validation_accumulates_independent_problems() {
    let source = MINIMAL
        .replace("name = \"hello\"", "name = \"bad/name\"")
        .replace("service = \"hello\"", "service = \"missing\"")
        .replace("kind = \"memory\"", "kind = \"file\"");
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(error.problems().len() >= 3);
}

#[test]
fn expose_node_id_reserves_all_worker_environment_writers() {
    let source = MINIMAL.replace(
        r#"compatibility_date = "2026-01-01""#,
        r#"compatibility_date = "2026-01-01"
expose_node_id = true

[services.vars]
PEREN_NODE_ID = "var"

[services.secrets]
PEREN_NODE_ID = "PEREN_SECRET_NODE_ID"

[services.secrets_store_refs]
PEREN_NODE_ID = "stored-node-id"

[services.bindings.PEREN_NODE_ID]
type = "secrets_store_secret"
secret_name = "stored-node-id""#,
    );
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();
    let fields: Vec<_> = error
        .problems()
        .iter()
        .map(|problem| problem.field.as_str())
        .collect();

    assert!(fields.contains(&"services[0].vars.PEREN_NODE_ID"));
    assert!(fields.contains(&"services[0].secrets.PEREN_NODE_ID"));
    assert!(fields.contains(&"services[0].secrets_store_refs.PEREN_NODE_ID"));
    assert!(fields.contains(&"services[0].bindings.PEREN_NODE_ID"));
}

#[test]
fn optional_root_tables_keep_their_compatibility_defaults() {
    let source = format!(
        "{MINIMAL}\n[control]\n\n[deploy]\n\n[d1_time_travel]\n\n[workflow]\n\n[tracing]\n\n[containers]\n"
    );
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    assert!(!config.raw.control.require_signed_mutations);
    assert!(config.raw.deploy.as_ref().unwrap().enable_preview);
    assert_eq!(config.raw.time_travel.retention_days, 30);
    assert_eq!(config.raw.workflow.wakeup_sweep_interval_secs, 15);
    assert!((config.raw.tracing.sampling_ratio - 1.0).abs() < f64::EPSILON);
    assert_eq!(
        config
            .raw
            .containers
            .as_ref()
            .unwrap()
            .max_instances_per_node,
        32
    );
    assert_eq!(config.raw.limits.subrequests_per_invocation, 10_000);
}

#[test]
fn control_mutation_signing_can_be_required_explicitly() {
    let source = format!("{MINIMAL}\n[control]\nrequire_signed_mutations = true\n");
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    assert!(config.raw.control.require_signed_mutations);
}

#[test]
fn validation_accumulates_queue_tenant_and_telemetry_errors() {
    let source = MINIMAL.replace(
        "[bucket]",
        "[queues]\nnats_url = \"nats://localhost\"\n\n[queues.consumer_defaults]\nmax_batch_size = 0\nmax_concurrency = 251\n\n[tracing]\nsampling_ratio = 2.0\n\n[[tenants]]\nid = \"acme\"\ncell_quota = 10\n\n[[tenants]]\nid = \"acme\"\ncell_quota = 20\n\n[bucket]",
    );
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field.ends_with("max_batch_size"))
    );
    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field.ends_with("max_concurrency"))
    );
    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field == "tracing.sampling_ratio")
    );
    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field == "tenants[1].id")
    );
}

#[test]
fn listener_addresses_are_parsed_before_construction() {
    let source = MINIMAL.replace("127.0.0.1:7500", "localhost:7500");
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field == "node.advertise_addr")
    );
}

#[test]
fn provider_binding_shapes_and_defaults_are_frozen() {
    let bindings = r#"
[services.bindings.CACHE]
type = "kv"
namespace = "cache"
unique_key = "kv-key"
backend = { kind = "redis", url_env = "REDIS_URL" }

[services.bindings.DB]
type = "d1_database"
database_name = "app"
unique_key = "d1-key"
backend = { kind = "external", url_env = "DATABASE_URL", driver = "postgres" }

[services.bindings.TURSO]
type = "d1_database"
database_name = "edge"
unique_key = "turso-key"
backend = { kind = "turso", url_env = "TURSO_URL", token_env = "TURSO_TOKEN", replica_path = "./data/turso.db" }

[services.bindings.SQL]
type = "hyperdrive"
pgcat_endpoint = "postgres://pgcat"
credential_scope = "database"

[services.bindings.OBJECTS]
type = "r2_bucket"
endpoint = "https://objects.example"
bucket = "uploads"
credential_scope = "objects"

[[services.bindings.OBJECTS.notifications]]
queue_name = "changes"
event_types = ["object_create", "object_delete"]
prefix = "images/"
"#;
    let source = MINIMAL.replace("[[sockets]]", &format!("{bindings}\n[[sockets]]"));
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();
    let service = &config.raw.services[0];

    assert!(matches!(
        &service.bindings["CACHE"],
        peren_config::Binding::Kv { backend: peren_config::KvBackend::Redis { url_env }, .. }
        if url_env == "REDIS_URL"
    ));
    assert!(matches!(
        &service.bindings["DB"],
        peren_config::Binding::D1Database {
            backend: peren_config::D1Backend::External {
                driver: peren_config::D1Driver::Postgres,
                ..
            },
            ..
        }
    ));
    assert!(matches!(
        &service.bindings["TURSO"],
        peren_config::Binding::D1Database {
            backend: peren_config::D1Backend::Turso {
                url_env,
                token_env,
                replica_path: Some(replica_path),
            },
            ..
        } if url_env == "TURSO_URL" && token_env == "TURSO_TOKEN" && replica_path == "./data/turso.db"
    ));
    assert!(matches!(
        &service.bindings["SQL"],
        peren_config::Binding::Hyperdrive {
            max_age_secs: 60,
            stale_while_revalidate_secs: 15,
            pool_max_connections: 10,
            ..
        }
    ));
    assert!(matches!(
        &service.bindings["OBJECTS"],
        peren_config::Binding::R2Bucket { notifications, .. }
        if notifications.len() == 1
    ));
}

#[test]
fn aws_sigv4_static_credentials_are_validated_independently_from_bucket_credentials() {
    let bindings = r#"
[services.bindings.AWS]
type = "aws_sigv4"
credential_source = "environment"
region = "us-east-1"
service = "bedrock"
allowed_hosts = ["bedrock-runtime.us-east-1.amazonaws.com"]
access_key_env = "AWS_ACCESS_KEY_ID"
"#;
    let source = MINIMAL.replace("[[sockets]]", &format!("{bindings}\n[[sockets]]"));

    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(error.problems().iter().any(|problem| {
        problem.field == "services[0].bindings.AWS.secret_key_env"
            && problem.message == "field is required for the selected provider"
    }));
}

#[test]
fn compatibility_dates_and_cron_expressions_are_validated() {
    let source = MINIMAL.replace("2026-01-01", "2026-02-30").replace(
        "[[sockets]]",
        "[[services.cron_triggers]]\nexpression = \"not a schedule\"\n\n[[sockets]]",
    );
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field.ends_with("compatibility_date"))
    );
    assert!(
        error
            .problems()
            .iter()
            .any(|problem| problem.field.ends_with("cron_triggers[0].expression"))
    );
}

#[test]
fn cell_backed_queue_broker_validates_without_external_endpoint() {
    let source = MINIMAL.replace(
        "[bucket]",
        r#"[queues]
broker = "cell"
cell_path = "queues/cell.sqlite"

[bucket]"#,
    );
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    let queues = config.raw.queues.as_ref().unwrap();
    assert_eq!(queues.broker, QueueBroker::Cell);
    assert_eq!(queues.cell_path.as_deref(), Some("queues/cell.sqlite"));
}

#[test]
fn queue_consumer_overrides_inherit_fleet_defaults() {
    let source = MINIMAL
        .replace(
            "[bucket]",
            "[queues]\nnats_url = \"nats://localhost\"\n\n[queues.consumer_defaults]\nmax_batch_size = 25\nmax_batch_timeout_secs = 12\n\n[bucket]",
        )
        .replace(
            "compatibility_date = \"2026-01-01\"",
            "compatibility_date = \"2026-01-01\"\nconsumes_queues = [{ queue = \"jobs\", max_retries = 9 }]",
        );
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();
    let defaults = &config.raw.queues.as_ref().unwrap().consumer_defaults;
    let QueueConsumer::Settings(settings) = &config.raw.services[0].consumes_queues[0] else {
        panic!("expected queue settings");
    };

    let resolved = settings.resolve(defaults);
    assert_eq!(resolved.max_batch_size, 25);
    assert_eq!(resolved.max_batch_timeout_secs, 12);
    assert_eq!(resolved.max_retries, 9);
}

#[test]
fn console_backend_defaults_from_topology_and_accepts_an_override() {
    let standalone = MINIMAL.replace(
        "[bucket]",
        "[console]\nlisten = \"127.0.0.1:9000\"\ndata_dir = \"console\"\n\n[bucket]",
    );
    let config = FleetConfig::from_toml(&standalone).unwrap();
    let console = config.console.as_ref().unwrap();
    assert_eq!(
        console.effective_backend(&config.seed_peers),
        ConsoleBackend::Sqlite
    );

    let distributed = standalone
        .replace(
            "[node]",
            "seed_peers = [\"https://peer.example\"]\n\n[node]",
        )
        .replace(
            "data_dir = \"console\"",
            "data_dir = \"console\"\nbackend = \"sqlite\"",
        );
    let config = FleetConfig::from_toml(&distributed).unwrap();
    assert_eq!(
        config
            .console
            .as_ref()
            .unwrap()
            .effective_backend(&config.seed_peers),
        ConsoleBackend::Sqlite
    );
}

#[test]
fn projects_assign_services_to_their_declared_tenant() {
    let source = MINIMAL
        .replace(
            "[[services]]",
            r#"[[tenants]]
id = "acme"
cell_quota = 10

[[projects]]
id = "checkout"
tenant_id = "acme"

[[services]]"#,
        )
        .replace(
            r#"compatibility_date = "2026-01-01""#,
            r#"compatibility_date = "2026-01-01"
tenant_id = "acme"
project_id = "checkout""#,
        );
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    assert_eq!(config.raw.projects[0].id, "checkout");
    assert_eq!(
        config.raw.services[0].project_id.as_deref(),
        Some("checkout")
    );
}

#[test]
fn validation_rejects_invalid_project_assignments() {
    let source = MINIMAL
        .replace(
            "[[services]]",
            r#"[[tenants]]
id = "acme"
cell_quota = 10

[[tenants]]
id = "beta"
cell_quota = 10

[[projects]]
id = "checkout"
tenant_id = "acme"

[[projects]]
id = "checkout"
tenant_id = "missing"

[[services]]"#,
        )
        .replace(
            r#"compatibility_date = "2026-01-01""#,
            r#"compatibility_date = "2026-01-01"
tenant_id = "beta"
project_id = "checkout""#,
        );
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();
    let problems = error.problems();

    assert!(
        problems
            .iter()
            .any(|problem| problem.field == "projects[1].id")
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.field == "projects[1].tenant_id")
    );
    assert!(problems.iter().any(|problem| {
        problem.field == "services[0].project_id"
            && problem.message == "project belongs to a different tenant"
    }));
}

#[test]
fn dispatch_script_preserves_its_scoped_deployment_request() {
    let source = MINIMAL.replace(
        "[bucket]",
        r#"[[dispatch_namespaces]]
name = "customers"

[[dispatch_namespaces.scripts]]
name = "acme"
worker_bundle_path = "acme.js"
compatibility_date = "2026-01-01"
cell_quota = 20

[dispatch_namespaces.scripts.deployment]
requested_unique_key = "application"
kv_namespaces = ["CACHE"]
d1_databases = ["DB"]
durable_object_classes = ["ROOM"]
outbound_hosts = ["api.example.com"]

[[dispatch_namespaces.scripts.deployment.r2_buckets]]
binding_name = "FILES"
endpoint = "https://objects.example"
bucket = "uploads"
sub_path = "acme/"
credential_scope = "objects"

[bucket]"#,
    );
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();
    let deployment = &config.raw.dispatch_namespaces[0].scripts[0].deployment;

    assert_eq!(deployment.kv_namespaces, ["CACHE"]);
    assert_eq!(deployment.r2_buckets[0].binding_name, "FILES");
    assert_eq!(deployment.outbound_hosts, ["api.example.com"]);
}

#[test]
fn unsupported_exporters_are_rejected_instead_of_silent() {
    let source = MINIMAL.replace(
        "[bucket]",
        r#"[logpush]
endpoint = "https://logs.example"

[chronicle_export]
endpoint = "https://chronicle.example"

[bucket]"#,
    );
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();
    let fields: Vec<_> = error
        .problems()
        .iter()
        .map(|problem| problem.field.as_str())
        .collect();

    assert!(fields.contains(&"logpush"), "{fields:?}");
    assert!(fields.contains(&"chronicle_export"), "{fields:?}");
}

#[test]
fn otlp_exporter_config_is_supported() {
    let source = MINIMAL.replace(
        "[bucket]",
        r#"[otlp]
endpoint = "https://otel.example/v1/traces"
service_name = "peren-test"
channel_capacity = 8
batch_max_spans = 4
flush_interval_secs = 1
request_timeout_secs = 1

[bucket]"#,
    );
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();
    let otlp = config.raw.otlp.unwrap();

    assert_eq!(otlp.endpoint, "https://otel.example/v1/traces");
    assert_eq!(otlp.service_name, "peren-test");
}

#[test]
fn service_descriptors_are_canonical_and_secret_safe() {
    let bindings = r#"
[services.bindings.QUEUE]
type = "queue"
queue_name = "jobs"

[services.bindings.AI]
type = "ai"
endpoint = "https://api.openai.com/v1"
credential_scope = "ai"
provider = { kind = "open_ai", api_key_env = "OPENAI_API_KEY", default_model = "gpt-4.1-mini" }

[services.bindings.OBJECTS]
type = "r2_bucket"
endpoint = "https://objects.example"
bucket = "uploads"
credential_scope = "objects"
access_key_env = "R2_ACCESS_KEY"
secret_key_env = "R2_SECRET_KEY"
prefix = "images/"
"#;
    let source = MINIMAL.replace(
        "compatibility_flags = []",
        "compatibility_flags = [\"nodejs_compat\", \"streams_enable_constructors\"]\nadditional_modules = { \"helper.js\" = \"./helper.js\" }",
    ).replace("[[sockets]]", &format!("{bindings}\n[[services.consumes_queues]]\nqueue = \"jobs\"\n\n[[sockets]]"));
    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    let first = config.raw.service_descriptors();
    let second = config.raw.service_descriptors();
    let encoded = serde_json::to_string(&first).unwrap();
    let service = &first[0];

    assert_eq!(first, second);
    assert_eq!(service.digest(), second[0].digest());
    assert_eq!(service.name, "hello");
    assert_eq!(
        service
            .bindings
            .iter()
            .map(|binding| binding.name.as_str())
            .collect::<Vec<_>>(),
        vec!["AI", "OBJECTS", "QUEUE"]
    );
    assert_eq!(service.queues[0].queue, "jobs");
    assert!(encoded.contains("open_ai"));
    assert!(encoded.contains("uploads"));
    assert!(!encoded.contains("OPENAI_API_KEY"));
    assert!(!encoded.contains("R2_ACCESS_KEY"));
    assert!(!encoded.contains("R2_SECRET_KEY"));
}

#[test]
fn azure_blob_bucket_requires_container_and_credentials() {
    let source = MINIMAL.replace("kind = \"memory\"", "kind = \"azure_blob\"");
    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();

    for field in [
        "bucket.bucket",
        "bucket.azure_account_env",
        "bucket.azure_access_key_env",
    ] {
        assert!(
            error
                .problems()
                .iter()
                .any(|problem| problem.field == field),
            "{field} should be required for Azure Blob"
        );
    }
}

#[test]
fn service_secrets_accept_stable_store_references() {
    let source = MINIMAL.replace(
        "compatibility_date = \"2026-01-01\"",
        "compatibility_date = \"2026-01-01\"\nsecrets = { STRIPE_SECRET_KEY = { source = \"store\", name = \"services/api/STRIPE_SECRET_KEY\" } }",
    );

    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();
    let secret = config.raw.services[0]
        .secrets
        .get("STRIPE_SECRET_KEY")
        .unwrap();

    assert_eq!(
        secret.store_name("STRIPE_SECRET_KEY"),
        Some("services/api/STRIPE_SECRET_KEY")
    );
    assert_eq!(secret.env_variable(), None);
}

#[test]
fn secrets_backend_config_accepts_future_managers_without_service_shape_changes() {
    let source = format!(
        r#"{MINIMAL}
[secrets]
provider = "aws_secrets_manager"
region = "us-east-1"
credentials_source = "workload_identity"
prefix = "/peren/prod"
"#
    );

    let config = FleetConfig::from_toml(&source).unwrap().validate().unwrap();

    assert_eq!(
        config.raw.secrets.provider,
        peren_config::SecretsProvider::AwsSecretsManager
    );
    assert_eq!(config.raw.secrets.region.as_deref(), Some("us-east-1"));
    assert_eq!(
        config.raw.secrets.credentials_source,
        CredentialsSource::WorkloadIdentity
    );
    assert_eq!(config.raw.secrets.prefix.as_deref(), Some("/peren/prod"));
}

#[test]
fn external_secret_managers_reject_worker_owned_credentials() {
    let source = format!(
        r#"{MINIMAL}
[secrets]
provider = "aws_secrets_manager"
region = "us-east-1"
credentials_source = "environment"
"#
    );

    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();
    let peren_config::ConfigError::Invalid(_, problems) = error else {
        panic!("expected validation error");
    };

    assert!(
        problems.iter().any(|problem| {
            problem.field == "secrets.credentials_source"
                && problem.message
                    == "external secret managers must use a host-owned identity source"
        }),
        "{problems:?}"
    );
}

#[test]
fn external_secret_managers_require_provider_identity_fields() {
    let source = format!(
        r#"{MINIMAL}
[secrets]
provider = "gcp_secret_manager"
credentials_source = "workload_identity"
"#
    );

    let error = FleetConfig::from_toml(&source)
        .unwrap()
        .validate()
        .unwrap_err();
    let peren_config::ConfigError::Invalid(_, problems) = error else {
        panic!("expected validation error");
    };

    assert!(
        problems
            .iter()
            .any(|problem| problem.field == "secrets.project"),
        "{problems:?}"
    );
}
