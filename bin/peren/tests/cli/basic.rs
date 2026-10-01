use crate::support::*;
use std::fs;

#[test]
fn global_help_and_version_are_successful() {
    let help = peren(&["--help"]);
    let version = peren(&["--version"]);

    assert!(help.status.success());
    assert!(
        String::from_utf8(help.stdout)
            .unwrap()
            .contains("Run and operate a Peren fleet")
    );
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        format!("peren {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn config_migrate_matches_top_level_migrate() {
    let directory = temp("peren-config-migrate-cli");
    fs::create_dir_all(&directory).unwrap();
    let wrangler = directory.join("wrangler.toml");
    fs::write(
        &wrangler,
        r#"
name = "hello"
main = "dist/index.js"
compatibility_date = "2026-01-01"
[[kv_namespaces]]
binding = "CACHE"
id = "cloudflare-id"
"#,
    )
    .unwrap();

    let top = peren(&["migrate", wrangler.to_str().unwrap()]);
    let nested = peren(&["config", "migrate", wrangler.to_str().unwrap()]);

    assert!(top.status.success());
    assert!(nested.status.success());
    assert_eq!(top.stdout, nested.stdout);
    let stdout = String::from_utf8(nested.stdout).unwrap();
    assert!(stdout.contains("[[services]]"));
    assert!(stdout.contains("name = \"hello\""));
    assert!(stdout.contains("type = \"kv\""));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn status_reports_ready_config() {
    let config = config();
    let output = peren(&["status", config.to_str().unwrap()]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ready: true"));
    assert!(stdout.contains("node: 00000000-0000-0000-0000-000000000001"));
    assert!(stdout.contains("services: 1"));
    assert!(stdout.contains("sockets: 1"));
    assert!(stdout.contains("storage: skipped"));
    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn status_can_emit_json_and_probe_storage() {
    let config = config();
    let output = peren(&[
        "status",
        config.to_str().unwrap(),
        "--json",
        "--storage-test",
    ]);

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["ready"], true);
    assert_eq!(value["node"], "00000000-0000-0000-0000-000000000001");
    assert_eq!(value["services"], 1);
    assert_eq!(value["sockets"], 1);
    assert_eq!(value["storage"], "writable");
    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn conformance_storage_checks_configured_repository() {
    let config = config();
    let output = peren(&["conformance", "storage", config.to_str().unwrap()]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("storage node="));
    assert!(stdout.contains(" cell="));
    assert!(stdout.contains(" revision=1 "));
    assert!(stdout.contains(" bytes="));
    assert!(stdout.contains(" range_read=true"));
    assert!(output.stderr.is_empty());
    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn diagnose_reports_valid_config() {
    let config = config();
    let output = peren(&["diagnose", config.to_str().unwrap()]);

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("node: 00000000-0000-0000-0000-000000000001"));
    assert!(stdout.contains("services: 1"));
    assert!(stdout.contains("sockets: 1"));
    assert!(stdout.contains("storage: skipped"));
    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn diagnose_storage_test_can_emit_json() {
    let config = config();
    let output = peren(&[
        "diagnose",
        config.to_str().unwrap(),
        "--storage-test",
        "--json",
    ]);

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["node"], "00000000-0000-0000-0000-000000000001");
    assert_eq!(value["services"], 1);
    assert_eq!(value["sockets"], 1);
    assert_eq!(value["storage"], "writable");
    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn usage_errors_use_clap_exit_status_and_stderr() {
    for args in [
        &[][..],
        &["unknown"][..],
        &["serve"][..],
        &["deploy", "fleet.toml", "--percent", "101"][..],
    ] {
        let output = peren(args);
        assert_eq!(output.status.code(), Some(2), "arguments: {args:?}");
        assert!(output.stdout.is_empty(), "arguments: {args:?}");
        assert!(!output.stderr.is_empty(), "arguments: {args:?}");
    }
}
