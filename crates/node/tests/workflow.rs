#[path = "support/env.rs"]
mod env;

use env::DataEnv;
use peren_config::FleetConfig;
use peren_node::{
    WorkflowCancel, WorkflowStatus, WorkflowTarget, cancel_workflow, delete_workflow,
    workflow_status,
};
use peren_testkit::worker::TestWorker;

#[test]
fn workflow_status_cancel_and_delete_are_persisted() {
    let worker =
        TestWorker::from_source("export default { fetch() { return new Response('ok') } };");
    let config = config(worker.path());
    let env = DataEnv::new();
    let target = || WorkflowTarget {
        service: "api".into(),
        binding: "FLOW".into(),
        instance: "one".into(),
    };

    assert_eq!(
        workflow_status(&config, &env, target())
            .unwrap()
            .state
            .status,
        WorkflowStatus::Unknown
    );
    let canceled = cancel_workflow(
        &config,
        &env,
        WorkflowCancel {
            target: target(),
            reason: Some("operator".into()),
        },
    )
    .unwrap();
    assert_eq!(canceled.state.status, WorkflowStatus::Canceled);
    assert_eq!(
        workflow_status(&config, &env, target())
            .unwrap()
            .state
            .reason
            .as_deref(),
        Some("operator")
    );
    assert_eq!(
        delete_workflow(&config, &env, target())
            .unwrap()
            .state
            .status,
        WorkflowStatus::Deleted
    );
}

fn config(bundle: &std::path::Path) -> peren_config::ValidatedConfig {
    FleetConfig::from_toml(&format!(
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
leaf_key_path = "leaf.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[services.bindings.FLOW]
type = "workflow"
class_name = "Flow"
unique_key = "flow"
"#,
        bundle.display()
    ))
    .unwrap()
    .validate()
    .unwrap()
}
