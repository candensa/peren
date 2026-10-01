use crate::support::*;
use std::fs;

#[test]
fn devcert_writes_development_mtls_files() {
    let directory = temp("peren-devcert");
    let output = peren(&["devcert", directory.to_str().unwrap()]);

    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    for file in ["ca.pem", "leaf-cert.pem", "leaf-key.pem"] {
        let path = directory.join(file);
        assert!(path.exists(), "missing {path:?}");
        let content = fs::read_to_string(path).unwrap();
        assert!(content.starts_with("-----BEGIN"));
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn credential_mint_prints_verifiable_scoped_token() {
    let directory = temp("peren-credential");
    let output = peren(&[
        "credential",
        "mint",
        directory.to_str().unwrap(),
        "--tenant",
        "tenant-a",
        "--bucket-prefix",
        "tenants/a",
        "--scope",
        "r2:read",
        "--scopes",
        "r2:write,d1:read",
    ]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let token = String::from_utf8(output.stdout).unwrap();
    let claims = peren_security::verify(&directory, token.trim()).unwrap();
    assert_eq!(claims.tenant, "tenant-a");
    assert_eq!(claims.bucket, "tenants/a");
    assert_eq!(claims.scopes, ["d1:read", "r2:read", "r2:write"]);
    assert_eq!(claims.expires_at_ms - claims.issued_at_ms, 15 * 60 * 1_000);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn tenant_delete_records_tombstone_and_blocks_revoke() {
    let config = config_with_tenant();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let mut delete = command(&[
        "tenant",
        "delete",
        config.to_str().unwrap(),
        "--tenant-id",
        "acme",
        "--reason",
        "contract-ended",
    ]);
    delete.env("PEREN_DATA_DIR", &data);
    let output = delete.output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "deleted acme reason=contract-ended
"
    );

    let mut revoke = command(&[
        "tenant",
        "revoke",
        config.to_str().unwrap(),
        "--tenant-id",
        "acme",
    ]);
    revoke.env("PEREN_DATA_DIR", &data);
    let output = revoke.output().unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("has been deleted"));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn tenant_revoke_records_reason() {
    let config = config_with_tenant();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let mut command = command(&[
        "tenant",
        "revoke",
        config.to_str().unwrap(),
        "--tenant-id",
        "acme",
        "--reason",
        "abuse",
    ]);
    command.env("PEREN_DATA_DIR", &data);
    let output = command.output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "revoked acme reason=abuse\n"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn secrets_rotate_versions_without_leaking_value() {
    let config = config_with_secret();
    let directory = config.parent().unwrap();
    let data = directory.join("data");
    let mut command = command(&[
        "secrets",
        "rotate",
        config.to_str().unwrap(),
        "--name",
        "TOKEN",
        "--value",
        "super-secret-value",
    ]);
    command.env("PEREN_DATA_DIR", &data);
    let output = command.output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("TOKEN version=1 digest="));
    assert!(stdout.contains(" created_at_ms="));
    assert!(!stdout.contains("super-secret-value"));
    assert!(
        !String::from_utf8(output.stderr)
            .unwrap()
            .contains("super-secret-value")
    );
    fs::remove_dir_all(directory).unwrap();
}
