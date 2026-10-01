use crate::support::*;
use std::fs;

#[test]
fn console_bootstrap_registers_operator_once() {
    let directory = temp("peren-console-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[console]
listen = "127.0.0.1:9000"
data_dir = "{}"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            directory.join("console").display(),
            worker.display()
        ),
    )
    .unwrap();

    let bootstrap = peren(&[
        "console",
        "bootstrap",
        config.to_str().unwrap(),
        "--workspace-name",
        "Ops",
    ]);
    assert!(bootstrap.status.success());
    let output = String::from_utf8(bootstrap.stdout).unwrap();
    assert!(output.contains("workspace: Ops\n"));
    let token = output
        .lines()
        .find_map(|line| line.strip_prefix("token: "))
        .unwrap()
        .to_string();

    let register = peren(&[
        "console",
        "register",
        config.to_str().unwrap(),
        "--token",
        &token,
        "--email",
        "ops@example.com",
        "--name",
        "Operator",
    ]);
    assert!(register.status.success());
    let output = String::from_utf8(register.stdout).unwrap();
    assert!(output.contains("user: "));
    assert!(output.contains("workspace: "));

    let second = peren(&[
        "console",
        "register",
        config.to_str().unwrap(),
        "--token",
        &token,
        "--email",
        "again@example.com",
        "--name",
        "Again",
    ]);
    assert_eq!(second.status.code(), Some(1));
    assert!(
        String::from_utf8(second.stderr)
            .unwrap()
            .contains("onboarding token")
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn workflow_status_cancel_and_delete_are_recorded() {
    let config = config_with_workflow();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let base = [
        config.to_str().unwrap(),
        "--service",
        "api",
        "--binding",
        "FLOW",
        "--instance-id",
        "one",
    ];

    let mut status = command(&[
        "workflow", "status", base[0], base[1], base[2], base[3], base[4], base[5], base[6],
    ]);
    status.env("PEREN_DATA_DIR", &data);
    let status = status.output().unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert_eq!(
        String::from_utf8(status.stdout).unwrap(),
        "workflow api FLOW one status=unknown\n"
    );

    let mut cancel = command(&[
        "workflow", "cancel", base[0], base[1], base[2], base[3], base[4], base[5], base[6],
        "--reason", "operator",
    ]);
    cancel.env("PEREN_DATA_DIR", &data);
    let cancel = cancel.output().unwrap();
    assert!(
        cancel.status.success(),
        "{}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    assert_eq!(
        String::from_utf8(cancel.stdout).unwrap(),
        "workflow api FLOW one status=canceled reason=operator\n"
    );

    let mut delete = command(&[
        "workflow", "delete", base[0], base[1], base[2], base[3], base[4], base[5], base[6],
    ]);
    delete.env("PEREN_DATA_DIR", &data);
    let delete = delete.output().unwrap();
    assert!(
        delete.status.success(),
        "{}",
        String::from_utf8_lossy(&delete.stderr)
    );
    assert_eq!(
        String::from_utf8(delete.stdout).unwrap(),
        "workflow api FLOW one status=deleted\n"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn tail_reads_captured_events() {
    let config = config();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let tail = data.join("tail");
    fs::create_dir_all(&tail).unwrap();
    fs::write(
        tail.join("events.jsonl"),
        r#"{"kind":"request","service":"api","method":"GET","path":"/ok","status":200,"outcome":"ok","wall_time_ms":4}
{"kind":"request","service":"api","method":"POST","path":"/fail","status":500,"outcome":"error","wall_time_ms":7}
"#,
    )
    .unwrap();

    let mut tail_command = command(&[
        "tail",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--level",
        "error",
    ]);
    tail_command.env("PEREN_DATA_DIR", &data);
    let output = tail_command.output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "api POST /fail status=500 outcome=error wall_time_ms=7\n"
    );
    let mut logs = command(&[
        "logs",
        config.to_str().unwrap(),
        "--service",
        "api",
        "--level",
        "error",
    ]);
    logs.env("PEREN_DATA_DIR", &data);
    let logs = logs.output().unwrap();
    assert!(
        logs.status.success(),
        "{}",
        String::from_utf8_lossy(&logs.stderr)
    );
    assert_eq!(
        String::from_utf8(logs.stdout).unwrap(),
        "api POST /fail status=500 outcome=error wall_time_ms=7\n"
    );

    fs::remove_dir_all(directory).unwrap();
}
