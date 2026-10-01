use peren_operator::{
    Cli,
    kubernetes::{
        AdmissionMode, Backend, CheckReport, DurableAuthority, HelmBackend, KeyValue, Options,
        Values,
    },
};

#[test]
fn parses_kubernetes_commands() {
    use clap::Parser;

    Cli::try_parse_from([
        "peren",
        "kubernetes",
        "render",
        "fleet.toml",
        "--output",
        "values.yaml",
        "--image",
        "example/peren",
        "--tag",
        "1.2.3",
        "--replicas",
        "3",
        "--service-account-annotation",
        "eks.amazonaws.com/role-arn=arn:aws:iam::123:role/peren",
    ])
    .unwrap();
    Cli::try_parse_from([
        "peren",
        "kubernetes",
        "check",
        "fleet.toml",
        "--replicas",
        "3",
        "--json",
    ])
    .unwrap();
}

#[test]
fn render_values_embed_validated_config_without_applying_cluster_state() {
    let values = Values {
        image: "example/peren".into(),
        tag: "1.2.3".into(),
        replicas: 5,
        storage: "20Gi".into(),
        annotations: vec![KeyValue {
            key: "eks.amazonaws.com/role-arn".into(),
            value: "arn:aws:iam::123:role/peren".into(),
        }],
        config: minimal().into(),
    }
    .render();

    assert!(values.contains("replicaCount: 5"), "{values}");
    assert!(values.contains("repository: \"example/peren\""), "{values}");
    assert!(values.contains("tag: \"1.2.3\""), "{values}");
    assert!(values.contains("size: \"20Gi\""), "{values}");
    assert!(
        values.contains("eks.amazonaws.com/role-arn: \"arn:aws:iam::123:role/peren\""),
        "{values}"
    );
    assert!(values.contains("  inline: |\n    [node]"), "{values}");
    assert!(
        values.contains("worker_bundle_path = \"worker.js\""),
        "{values}"
    );
}

#[test]
fn helm_backend_builds_a_reviewable_fleet_plan() {
    let config = peren_config::FleetConfig::from_toml(kubernetes_ready())
        .unwrap()
        .validate()
        .unwrap();

    let plan = HelmBackend.plan(
        &config.raw,
        Options {
            image: "ghcr.io/peren/peren".into(),
            tag: "1.0.0".into(),
            replicas: 3,
            storage: "50Gi".into(),
            annotations: Vec::new(),
            config: kubernetes_ready().into(),
        },
    );

    assert_eq!(plan.fleet.name, "peren");
    assert_eq!(plan.fleet.replicas, 3);
    assert_eq!(plan.fleet.admission, AdmissionMode::Serving);
    assert_eq!(plan.fleet.services[0].name, "api");
    assert_eq!(plan.fleet.services[0].sockets, ["public"]);
    assert_eq!(
        plan.fleet.storage.durable_authority,
        DurableAuthority::ObjectStore
    );
    assert!(!plan.fleet.storage.disk_removal_safe);
    assert!(plan.values.contains("replicaCount: 3"), "{}", plan.values);
}

#[test]
fn check_rejects_local_storage_for_multi_replica_fleets() {
    let config = peren_config::FleetConfig::from_toml(minimal())
        .unwrap()
        .validate()
        .unwrap();

    let report = CheckReport::new(&config.raw, 3);

    assert!(
        report
            .problems
            .iter()
            .any(|problem| problem.field == "bucket.kind")
    );
}

#[test]
fn check_accepts_s3_workload_identity_and_pod_listeners() {
    let config = peren_config::FleetConfig::from_toml(kubernetes_ready())
        .unwrap()
        .validate()
        .unwrap();

    let report = CheckReport::new(&config.raw, 3);

    assert!(report.problems.is_empty(), "{report:?}");
    assert!(report.warnings.is_empty(), "{report:?}");
}

fn kubernetes_ready() -> &'static str {
    r#"[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "0.0.0.0:7000"
listen = "0.0.0.0:7000"

[bucket]
kind = "s3"
endpoint = "https://s3.example.com"
bucket = "peren-production"
region = "us-east-1"
credentials_source = "workload_identity"

[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "leaf.key"

[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"

[[sockets]]
name = "public"
listen = "0.0.0.0:8080"
service = "api"
"#
}

fn minimal() -> &'static str {
    r#"[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "0.0.0.0:7000"
listen = "0.0.0.0:7000"

[bucket]
kind = "memory"

[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "leaf.key"

[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"

[[sockets]]
name = "public"
listen = "0.0.0.0:8080"
service = "api"
"#
}
