use crate::support::*;
use std::fs;

#[test]
fn deploy_records_lists_and_prunes_generations() {
    let config = config();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let config_path = config.to_str().unwrap();

    let first = deploy(&config, &data, &["deploy", config_path, "--percent", "25"]);
    assert!(first.contains("deployed api "));
    assert!(first.contains("preview=false"));
    assert!(first.contains("percent=25"));
    assert!(first.contains("maps=1"));
    let first_digest = first
        .split_whitespace()
        .nth(2)
        .expect("deployment digest is printed")
        .to_string();

    write_worker(directory, "changed");
    deploy(&config, &data, &["deploy", config_path]);

    let verified = deploy(&config, &data, &["deploy", "verify", config_path]);
    assert!(verified.contains("verified active=true"));
    let health = deploy(&config, &data, &["deploy", "health", config_path]);
    assert!(health.contains("api "));
    assert!(health.contains(" healthy percent=100"));

    write_worker(directory, "preview");
    let preview = deploy(&config, &data, &["deploy", config_path, "--preview"]);
    assert!(preview.contains("preview=true"));
    assert!(preview.contains("percent=0"));

    let listed = deploy(
        &config,
        &data,
        &["deploy", "list", config_path, "--service", "api"],
    );
    assert_eq!(listed.lines().count(), 3);
    assert_eq!(
        listed
            .lines()
            .filter(|line| line.contains("active=true"))
            .count(),
        1
    );
    assert!(listed.contains("preview=true"));
    assert!(listed.contains("maps=1"));

    let dry = deploy(
        &config,
        &data,
        &[
            "deploy",
            "prune",
            config_path,
            "--service",
            "api",
            "--keep",
            "0",
            "--dry-run",
        ],
    );
    assert_eq!(dry, "would prune 2 generations\n");

    deploy_drift(&config, &data);

    let rollback = command(&[
        "rollback",
        config_path,
        "--service",
        "api",
        "--to",
        &first_digest,
    ])
    .env("PEREN_DATA_DIR", &data)
    .output()
    .unwrap();
    assert_eq!(rollback.status.code(), Some(1));
    assert!(
        String::from_utf8(rollback.stderr)
            .unwrap()
            .contains("artifact digest drifted")
    );

    let pruned = deploy(
        &config,
        &data,
        &[
            "deploy",
            "prune",
            config_path,
            "--service",
            "api",
            "--keep",
            "0",
        ],
    );
    assert_eq!(pruned, "pruned 2 generations\n");

    let listed = deploy(&config, &data, &["deploy", "list", config_path]);
    assert_eq!(listed.lines().count(), 1);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn backup_and_restore_round_trip_operator_data() {
    let config = config();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let restored = directory.join("restored");
    let archive = directory.join("backup");
    let config_path = config.to_str().unwrap();

    let deployed = deploy(&config, &data, &["deploy", config_path]);
    let digest = deployed
        .split_whitespace()
        .nth(2)
        .expect("deployment digest is printed")
        .to_string();

    let backup = deploy(
        &config,
        &data,
        &["backup", config_path, "--output", archive.to_str().unwrap()],
    );
    assert!(backup.contains("backed up "));
    assert!(backup.contains(" files="));

    let restore = deploy(
        &config,
        &restored,
        &["restore", config_path, "--input", archive.to_str().unwrap()],
    );
    assert!(restore.contains("restored "));
    assert!(restore.contains(" files="));

    let listed = deploy(&config, &restored, &["deploy", "list", config_path]);
    assert!(listed.contains(&digest));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn upgrade_check_and_plan_validate_storage() {
    let config = config();
    let config_path = config.to_str().unwrap();

    let check = peren(&["upgrade", "check", config_path, "--target", "0.2.0"]);
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
    let stdout = String::from_utf8(check.stdout).unwrap();
    assert!(stdout.contains("upgrade check current="));
    assert!(stdout.contains("target=0.2.0"));
    assert!(stdout.contains("storage=true"));

    let plan = peren(&["upgrade", "plan", config_path]);
    assert!(
        plan.status.success(),
        "{}",
        String::from_utf8_lossy(&plan.stderr)
    );
    let stdout = String::from_utf8(plan.stdout).unwrap();
    assert!(stdout.contains("upgrade plan current="));
    assert!(stdout.contains("1. backup data directory"));
    assert!(stdout.contains("4. run conformance storage"));

    fs::remove_dir_all(config.parent().unwrap()).unwrap();
}

#[test]
fn uninstall_requires_force_and_supports_dry_run() {
    let config = config();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let config_path = config.to_str().unwrap();

    deploy(&config, &data, &["deploy", config_path]);
    assert!(data.join("deploy/deployments.json").exists());

    let dry = deploy(&config, &data, &["uninstall", config_path, "--dry-run"]);
    assert!(dry.contains("would uninstall "));
    assert!(data.join("deploy/deployments.json").exists());

    let refused = command(&["uninstall", config_path])
        .env("PEREN_DATA_DIR", &data)
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(1));
    assert!(
        String::from_utf8(refused.stderr)
            .unwrap()
            .contains("pass --force")
    );
    assert!(data.join("deploy/deployments.json").exists());

    let removed = deploy(&config, &data, &["uninstall", config_path, "--force"]);
    assert!(removed.contains("uninstalled "));
    assert!(!data.exists());

    fs::remove_dir_all(directory).unwrap();
}
