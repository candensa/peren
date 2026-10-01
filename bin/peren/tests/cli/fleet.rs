use crate::support::*;
use std::fs;

#[test]
fn node_join_uses_signed_identity_token_and_records_membership() {
    let config = config();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let keys = directory.join("keys");
    let node = "00000000-0000-0000-0000-000000000001";
    let config_path = config.to_str().unwrap();

    let token = deploy(
        &config,
        &data,
        &[
            "credential",
            "node",
            keys.to_str().unwrap(),
            "--cluster",
            "prod",
            "--node",
            node,
            "--peer-addr",
            "127.0.0.1:7000",
        ],
    );
    let token = token.trim();

    let joined = deploy(
        &config,
        &data,
        &[
            "node",
            "join",
            config_path,
            "--key-dir",
            keys.to_str().unwrap(),
            "--token",
            token,
        ],
    );

    assert_eq!(joined, format!("node {node} joined peer=127.0.0.1:7000\n"));
    let registry = fs::read_to_string(data.join("fleet/nodes.json")).unwrap();
    assert!(registry.contains("active"));
    assert!(registry.contains("127.0.0.1:7000"));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn node_drain_then_remove_records_lifecycle() {
    let config = config();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let node = "00000000-0000-0000-0000-000000000001";
    let config_path = config.to_str().unwrap();

    let refused = command(&["node", "remove", config_path, "--node", node])
        .env("PEREN_DATA_DIR", &data)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(1));
    assert!(
        String::from_utf8(refused.stderr)
            .unwrap()
            .contains("must be drained before removal")
    );

    let drained = deploy(
        &config,
        &data,
        &[
            "node",
            "drain",
            config_path,
            "--node",
            node,
            "--reason",
            "maintenance",
        ],
    );
    assert_eq!(drained, format!("node {node} draining\n"));

    let removed = deploy(
        &config,
        &data,
        &["node", "remove", config_path, "--node", node],
    );
    assert_eq!(removed, format!("node {node} removed\n"));
    assert!(data.join("fleet/nodes.json").exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn node_health_probes_configured_listeners() {
    let directory = temp("peren-node-health-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let peer = ready_server();
    let public = ready_server();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "{peer}"
listen = "{peer}"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "{public}"
service = "api"
"#,
            worker.display()
        ),
    )
    .unwrap();

    let output = peren(&["node", "health", config.to_str().unwrap()]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("peer {peer} ready")));
    assert!(stdout.contains(&format!("public {public} ready")));
    fs::remove_dir_all(directory).unwrap();
}
