use clap::Parser;
use peren_operator::Cli;

fn parses(args: &[&str]) {
    Cli::try_parse_from(args).unwrap();
}

#[test]
fn parses_setup_and_status_commands() {
    for args in [
        &["peren", "init", "--output", "fleet.toml"][..],
        &["peren", "config", "validate", "fleet.toml"],
        &["peren", "conformance", "storage", "fleet.toml"],
        &["peren", "config", "migrate", "wrangler.toml"],
        &["peren", "doctor", "fleet.toml"],
        &["peren", "status", "fleet.toml", "--json"],
    ] {
        parses(args);
    }
}

#[test]
fn parses_node_and_process_commands() {
    for args in [
        &["peren", "node", "health", "fleet.toml"][..],
        &[
            "peren",
            "node",
            "join",
            "fleet.toml",
            "--key-dir",
            "keys",
            "--token",
            "signed",
        ],
        &[
            "peren",
            "node",
            "drain",
            "fleet.toml",
            "--node",
            "00000000-0000-0000-0000-000000000001",
        ],
        &[
            "peren",
            "node",
            "remove",
            "fleet.toml",
            "--node",
            "00000000-0000-0000-0000-000000000001",
        ],
        &["peren", "logs", "fleet.toml", "--service", "api"],
        &["peren", "serve", "fleet.toml", "--socket-fd", "public=3"],
    ] {
        parses(args);
    }
}

#[test]
fn parses_deploy_and_backup_commands() {
    for args in [
        &["peren", "deploy", "fleet.toml", "--percent", "25"][..],
        &["peren", "deploy", "fleet.toml", "--preview"],
        &[
            "peren",
            "deploy",
            "health",
            "fleet.toml",
            "--service",
            "api",
        ],
        &["peren", "deploy", "prune", "fleet.toml", "--service", "api"],
        &[
            "peren",
            "deploy",
            "verify",
            "fleet.toml",
            "--service",
            "api",
        ],
        &["peren", "backup", "fleet.toml", "--output", "backup"],
        &[
            "peren",
            "restore",
            "fleet.toml",
            "--input",
            "backup",
            "--force",
        ],
    ] {
        parses(args);
    }
}

#[test]
fn parses_data_and_workflow_commands() {
    for args in [
        &[
            "peren",
            "d1",
            "restore",
            "fleet.toml",
            "--service",
            "api",
            "--binding",
            "DB",
            "--into",
            "restored",
            "--bookmark",
            "abc",
        ][..],
        &[
            "peren",
            "workflow",
            "cancel",
            "fleet.toml",
            "--service",
            "api",
            "--binding",
            "FLOW",
            "--instance-id",
            "1",
        ],
        &[
            "peren",
            "kv",
            "bulk-import",
            "fleet.toml",
            "--service",
            "api",
            "--binding",
            "KV",
            "--file",
            "data.ndjson",
        ],
    ] {
        parses(args);
    }
}

#[test]
fn rejects_unsafe_or_ambiguous_values() {
    assert!(Cli::try_parse_from(["peren", "deploy", "fleet.toml", "--percent", "101"]).is_err());
    assert!(
        Cli::try_parse_from(["peren", "serve", "fleet.toml", "--socket-fd", "public=-1"]).is_err()
    );
    assert!(
        Cli::try_parse_from([
            "peren",
            "d1",
            "restore",
            "fleet.toml",
            "--service",
            "api",
            "--binding",
            "DB",
            "--into",
            "copy"
        ])
        .is_err()
    );
}
