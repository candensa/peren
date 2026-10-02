use std::path::Path;

use peren_config::{FleetConfig, ValidatedConfig};

pub fn worker(worker: &Path) -> ValidatedConfig {
    worker_with_control_signing(worker, false)
}

#[allow(dead_code)]
pub fn signed_control_worker(worker: &Path) -> ValidatedConfig {
    worker_with_control_signing(worker, true)
}

fn worker_with_control_signing(worker: &Path, require_signed_mutations: bool) -> ValidatedConfig {
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
leaf_key_path = "key.pem"
[control]
require_signed_mutations = {require_signed_mutations}
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
"#,
        worker.display()
    ))
    .unwrap()
    .validate()
    .unwrap()
}
