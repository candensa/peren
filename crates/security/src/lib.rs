use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use rand::TryRngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

const KEY: &str = "credential.key";
const KEY_BYTES: usize = 32;
const VERSION: &str = "peren-scoped-credential-v1";
const NODE_VERSION: &str = "peren-node-identity-v1";
const CONTROL_VERSION: &str = "peren-control-request-v1";
const TOKEN_TTL_MS: i64 = 15 * 60 * 1_000;
const CONTROL_CLOCK_SKEW_MS: i64 = 5 * 60 * 1_000;

pub struct Devcert {
    pub directory: std::path::PathBuf,
}

pub fn devcert(command: &Devcert) -> Result<(), DevcertError> {
    fs::create_dir_all(&command.directory).map_err(|source| DevcertError::CreateDirectory {
        path: command.directory.clone(),
        source,
    })?;
    let temp = command
        .directory
        .join(format!(".devcert-{}", std::process::id()));
    if temp.exists() {
        fs::remove_dir_all(&temp).map_err(|source| DevcertError::RemoveTemp {
            path: temp.clone(),
            source,
        })?;
    }
    fs::create_dir_all(&temp).map_err(|source| DevcertError::CreateDirectory {
        path: temp.clone(),
        source,
    })?;

    let ca = command.directory.join("ca.pem");
    let leaf = command.directory.join("leaf-cert.pem");
    let key = command.directory.join("leaf-key.pem");
    let cakey = temp.join("ca.key");
    let csr = temp.join("leaf.csr");
    let serial = temp.join("ca.srl");
    let ext = temp.join("leaf.ext");
    fs::write(
        &ext,
        "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )
    .map_err(|source| DevcertError::WriteFile {
        path: ext.clone(),
        source,
    })?;

    openssl(["genrsa", "-out", path(&cakey)?, "2048"])?;
    openssl([
        "req",
        "-x509",
        "-new",
        "-nodes",
        "-key",
        path(&cakey)?,
        "-sha256",
        "-days",
        "3650",
        "-subj",
        "/CN=Peren Development CA",
        "-out",
        path(&ca)?,
    ])?;
    openssl(["genrsa", "-out", path(&key)?, "2048"])?;
    openssl([
        "req",
        "-new",
        "-key",
        path(&key)?,
        "-subj",
        "/CN=localhost",
        "-out",
        path(&csr)?,
    ])?;
    openssl([
        "x509",
        "-req",
        "-in",
        path(&csr)?,
        "-CA",
        path(&ca)?,
        "-CAkey",
        path(&cakey)?,
        "-CAserial",
        path(&serial)?,
        "-CAcreateserial",
        "-out",
        path(&leaf)?,
        "-days",
        "825",
        "-sha256",
        "-extfile",
        path(&ext)?,
    ])?;
    fs::remove_dir_all(&temp).map_err(|source| DevcertError::RemoveTemp { path: temp, source })?;
    Ok(())
}

fn path(value: &Path) -> Result<&str, DevcertError> {
    value
        .to_str()
        .ok_or_else(|| DevcertError::Path(value.to_path_buf()))
}

fn openssl<const N: usize>(args: [&str; N]) -> Result<(), DevcertError> {
    let output = std::process::Command::new("openssl")
        .args(args)
        .output()
        .map_err(DevcertError::OpenSslStart)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(DevcertError::OpenSslStatus {
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }
}

#[derive(Debug, Error)]
pub enum DevcertError {
    #[error("failed to create certificate directory {path:?}")]
    CreateDirectory {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write certificate support file {path:?}")]
    WriteFile {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove temporary certificate directory {path:?}")]
    RemoveTemp {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("certificate path {0:?} is not valid UTF-8")]
    Path(std::path::PathBuf),
    #[error("failed to start openssl")]
    OpenSslStart(#[source] std::io::Error),
    #[error("openssl failed with status {status:?}: {stderr}")]
    OpenSslStatus { status: Option<i32>, stderr: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeMint {
    pub cluster: String,
    pub node: String,
    pub peer_addr: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeClaims {
    pub version: String,
    pub cluster: String,
    pub node: String,
    pub peer_addr: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

pub fn mint_node(directory: &Path, request: NodeMint) -> Result<String, CredentialError> {
    let key = load_or_create_key(directory)?;
    node_token(&key, request, now_ms()?)
}

pub fn verify_node(directory: &Path, token: &str) -> Result<NodeClaims, CredentialError> {
    let key = load_key(directory)?;
    verify_node_with_key(&key, token, now_ms()?)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRequest<'a> {
    pub target_node: &'a str,
    pub method: &'a str,
    pub target: &'a str,
    pub body: &'a [u8],
    pub timestamp_ms: i64,
    pub nonce: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlSignature {
    pub version: &'static str,
    pub signature: String,
}

pub fn sign_control_request(
    directory: &Path,
    request: &ControlRequest<'_>,
) -> Result<ControlSignature, CredentialError> {
    validate_control_request(request)?;
    let key = load_or_create_key(directory)?;
    Ok(ControlSignature {
        version: CONTROL_VERSION,
        signature: sign(&key, control_payload(request).as_bytes()),
    })
}

pub fn verify_control_request(
    directory: &Path,
    request: &ControlRequest<'_>,
    version: &str,
    signature: &str,
) -> Result<(), CredentialError> {
    let key = load_key(directory)?;
    verify_control_request_with_key(&key, request, version, signature, now_ms())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mint {
    pub tenant: String,
    pub bucket: String,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Claims {
    pub version: String,
    pub tenant: String,
    pub bucket: String,
    pub scopes: Vec<String>,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

pub fn mint(directory: &Path, request: Mint) -> Result<String, CredentialError> {
    let key = load_or_create_key(directory)?;
    token(&key, request, now_ms()?)
}

pub fn verify(directory: &Path, token: &str) -> Result<Claims, CredentialError> {
    let key = load_key(directory)?;
    verify_with_key(&key, token, now_ms()?)
}

pub fn authorize(
    directory: &Path,
    token: &str,
    required_scope: &str,
    bucket_prefix: &str,
) -> Result<Claims, CredentialError> {
    let claims = verify(directory, token)?;
    claims.authorize(required_scope, bucket_prefix)?;
    Ok(claims)
}

impl Claims {
    pub fn authorize(
        &self,
        required_scope: &str,
        bucket_prefix: &str,
    ) -> Result<(), CredentialError> {
        if !self.scopes.iter().any(|scope| scope == required_scope) {
            return Err(CredentialError::Permission(required_scope.to_string()));
        }
        if !inside_bucket(&self.bucket, bucket_prefix) {
            return Err(CredentialError::Bucket {
                expected: self.bucket.clone(),
                actual: bucket_prefix.to_string(),
            });
        }
        Ok(())
    }
}

fn inside_bucket(bucket: &str, prefix: &str) -> bool {
    let bucket = bucket.trim_end_matches('/');
    prefix == bucket
        || prefix
            .strip_prefix(bucket)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn verify_with_key(key: &[u8], token: &str, now: i64) -> Result<Claims, CredentialError> {
    let (claims, signature) = token
        .rsplit_once('.')
        .ok_or(CredentialError::MalformedToken)?;
    let expected = sign(key, claims.as_bytes());
    if expected != signature {
        return Err(CredentialError::InvalidSignature);
    }
    let claims = URL_SAFE_NO_PAD.decode(claims)?;
    let claims: Claims = serde_json::from_slice(&claims)?;
    if claims.version != VERSION {
        return Err(CredentialError::Version(claims.version));
    }
    if claims.expires_at_ms <= now {
        return Err(CredentialError::Expired);
    }
    Ok(claims)
}

fn token(key: &[u8], request: Mint, now: i64) -> Result<String, CredentialError> {
    let claims = Claims {
        version: VERSION.into(),
        tenant: request.tenant,
        bucket: request.bucket,
        scopes: normalize(request.scopes)?,
        issued_at_ms: now,
        expires_at_ms: now.checked_add(TOKEN_TTL_MS).ok_or(CredentialError::Time)?,
    };
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    let signature = sign(key, claims.as_bytes());
    Ok(format!("{claims}.{signature}"))
}

fn verify_node_with_key(key: &[u8], token: &str, now: i64) -> Result<NodeClaims, CredentialError> {
    let (claims, signature) = token
        .rsplit_once('.')
        .ok_or(CredentialError::MalformedToken)?;
    let expected = sign(key, claims.as_bytes());
    if expected != signature {
        return Err(CredentialError::InvalidSignature);
    }
    let claims = URL_SAFE_NO_PAD.decode(claims)?;
    let claims: NodeClaims = serde_json::from_slice(&claims)?;
    if claims.version != NODE_VERSION {
        return Err(CredentialError::Version(claims.version));
    }
    if claims.expires_at_ms <= now {
        return Err(CredentialError::Expired);
    }
    Ok(claims)
}

fn node_token(key: &[u8], request: NodeMint, now: i64) -> Result<String, CredentialError> {
    if request.cluster.trim().is_empty()
        || request.node.trim().is_empty()
        || request.peer_addr.trim().is_empty()
    {
        return Err(CredentialError::NodeIdentity);
    }
    let claims = NodeClaims {
        version: NODE_VERSION.into(),
        cluster: request.cluster,
        node: request.node,
        peer_addr: request.peer_addr,
        issued_at_ms: now,
        expires_at_ms: now.checked_add(TOKEN_TTL_MS).ok_or(CredentialError::Time)?,
    };
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    let signature = sign(key, claims.as_bytes());
    Ok(format!("{claims}.{signature}"))
}

fn verify_control_request_with_key(
    key: &[u8],
    request: &ControlRequest<'_>,
    version: &str,
    signature: &str,
    now: Result<i64, CredentialError>,
) -> Result<(), CredentialError> {
    validate_control_request(request)?;
    if version != CONTROL_VERSION {
        return Err(CredentialError::Version(version.to_string()));
    }
    let now = now?;
    if request.timestamp_ms > now.saturating_add(CONTROL_CLOCK_SKEW_MS)
        || request.timestamp_ms < now.saturating_sub(CONTROL_CLOCK_SKEW_MS)
    {
        return Err(CredentialError::Expired);
    }
    let supplied = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| CredentialError::InvalidSignature)?;
    verify_signature(key, control_payload(request).as_bytes(), &supplied)?;
    Ok(())
}

fn validate_control_request(request: &ControlRequest<'_>) -> Result<(), CredentialError> {
    if request.target_node.trim().is_empty()
        || request.method.trim().is_empty()
        || request.target.trim().is_empty()
    {
        return Err(CredentialError::ControlRequest);
    }
    if request.nonce.len() < 16
        || request.nonce.len() > 128
        || !request
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CredentialError::ControlNonce);
    }
    Ok(())
}

fn control_payload(request: &ControlRequest<'_>) -> String {
    let body_hash = sign(b"peren-control-body-sha256", request.body);
    format!(
        "{CONTROL_VERSION}\n{}\n{}\n{}\n{}\n{}\n{}",
        request.target_node,
        request.method.to_ascii_uppercase(),
        request.target,
        request.timestamp_ms,
        request.nonce,
        body_hash
    )
}

fn now_ms() -> Result<i64, CredentialError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CredentialError::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| CredentialError::Time)
}

fn normalize(scopes: Vec<String>) -> Result<Vec<String>, CredentialError> {
    let mut scopes = scopes
        .into_iter()
        .map(|scope| scope.trim().to_string())
        .filter(|scope| !scope.is_empty())
        .collect::<Vec<_>>();
    scopes.sort();
    scopes.dedup();
    if scopes.is_empty() {
        return Err(CredentialError::EmptyScopes);
    }
    if let Some(scope) = scopes.iter().find(|scope| !valid_scope(scope)) {
        return Err(CredentialError::Scope(scope.clone()));
    }
    Ok(scopes)
}

fn valid_scope(scope: &str) -> bool {
    scope
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'-' | b'_' | b'.'))
}

fn sign(key: &[u8], payload: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
    mac.update(payload);
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn verify_signature(key: &[u8], payload: &[u8], signature: &[u8]) -> Result<(), CredentialError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
    mac.update(payload);
    mac.verify_slice(signature)
        .map_err(|_| CredentialError::InvalidSignature)
}

fn load_or_create_key(directory: &Path) -> Result<Vec<u8>, CredentialError> {
    fs::create_dir_all(directory).map_err(|source| CredentialError::CreateDirectory {
        path: directory.to_path_buf(),
        source,
    })?;
    let path = directory.join(KEY);
    match fs::read(&path) {
        Ok(key) => validate_key(path, &key),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut key = vec![0; KEY_BYTES];
            rand::rngs::OsRng
                .try_fill_bytes(&mut key)
                .map_err(|source| CredentialError::Random(source.to_string()))?;
            fs::write(&path, URL_SAFE_NO_PAD.encode(&key)).map_err(|source| {
                CredentialError::WriteKey {
                    path: path.clone(),
                    source,
                }
            })?;
            Ok(key)
        }
        Err(source) => Err(CredentialError::ReadKey { path, source }),
    }
}

fn load_key(directory: &Path) -> Result<Vec<u8>, CredentialError> {
    let path = directory.join(KEY);
    let key = fs::read(&path).map_err(|source| CredentialError::ReadKey {
        path: path.clone(),
        source,
    })?;
    validate_key(path, &key)
}

fn validate_key(path: std::path::PathBuf, encoded: &[u8]) -> Result<Vec<u8>, CredentialError> {
    let value = std::str::from_utf8(encoded)
        .map_err(|_| CredentialError::KeyEncoding(path.clone()))?
        .trim();
    let key = URL_SAFE_NO_PAD.decode(value)?;
    if key.len() != KEY_BYTES {
        return Err(CredentialError::KeyLength(path));
    }
    Ok(key)
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential scope list cannot be empty")]
    EmptyScopes,
    #[error("credential scope {0:?} contains unsupported characters")]
    Scope(String),
    #[error("failed to create credential key directory {path:?}")]
    CreateDirectory {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read credential key {path:?}")]
    ReadKey {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write credential key {path:?}")]
    WriteKey {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("credential key {0:?} is not valid UTF-8")]
    KeyEncoding(std::path::PathBuf),
    #[error("credential key {0:?} must contain 32 bytes")]
    KeyLength(std::path::PathBuf),
    #[error("secure random source failed: {0}")]
    Random(String),
    #[error("credential token is malformed")]
    MalformedToken,
    #[error("credential token signature is invalid")]
    InvalidSignature,
    #[error("credential token has expired")]
    Expired,
    #[error("system clock cannot produce a valid credential timestamp")]
    Time,
    #[error("credential token version {0:?} is unsupported")]
    Version(String),
    #[error("node identity token is missing cluster, node, or peer address")]
    NodeIdentity,
    #[error("control request is missing node, method, or target")]
    ControlRequest,
    #[error("control request nonce is invalid")]
    ControlNonce,
    #[error("credential token does not grant required scope {0:?}")]
    Permission(String),
    #[error("credential bucket prefix {actual:?} is outside allowed prefix {expected:?}")]
    Bucket { expected: String, actual: String },
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_identity_token_is_signed_and_verified() {
        let directory =
            std::env::temp_dir().join(format!("peren-node-token-{}", uuid::Uuid::new_v4()));
        let token = mint_node(
            &directory,
            NodeMint {
                cluster: "prod".into(),
                node: "00000000-0000-0000-0000-000000000001".into(),
                peer_addr: "127.0.0.1:7000".into(),
            },
        )
        .unwrap();

        let claims = verify_node(&directory, &token).unwrap();

        assert_eq!(claims.version, NODE_VERSION);
        assert_eq!(claims.cluster, "prod");
        assert_eq!(claims.peer_addr, "127.0.0.1:7000");
        assert!(verify_node(&directory, &format!("{token}x")).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn control_request_signature_binds_method_target_and_body() {
        let directory =
            std::env::temp_dir().join(format!("peren-control-sign-{}", uuid::Uuid::new_v4()));
        let request = ControlRequest {
            target_node: "00000000-0000-0000-0000-000000000001",
            method: "POST",
            target: "/control/v1/node/drain",
            body: b"",
            timestamp_ms: now_ms().unwrap(),
            nonce: "nonce-0000000001",
        };
        let signed = sign_control_request(&directory, &request).unwrap();

        verify_control_request(&directory, &request, signed.version, &signed.signature).unwrap();

        let tampered = ControlRequest {
            target: "/control/v1/node/retire",
            ..request
        };
        assert!(matches!(
            verify_control_request(&directory, &tampered, signed.version, &signed.signature),
            Err(CredentialError::InvalidSignature)
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn control_request_rejects_stale_timestamps_and_bad_nonces() {
        let directory =
            std::env::temp_dir().join(format!("peren-control-sign-{}", uuid::Uuid::new_v4()));
        let key = load_or_create_key(&directory).unwrap();
        let stale = ControlRequest {
            target_node: "00000000-0000-0000-0000-000000000001",
            method: "POST",
            target: "/control/v1/node/drain",
            body: b"",
            timestamp_ms: 1_000,
            nonce: "nonce-0000000002",
        };
        let signature = sign(&key, control_payload(&stale).as_bytes());

        assert!(matches!(
            verify_control_request_with_key(
                &key,
                &stale,
                CONTROL_VERSION,
                &signature,
                Ok(1_000 + CONTROL_CLOCK_SKEW_MS + 1)
            ),
            Err(CredentialError::Expired)
        ));
        assert!(matches!(
            sign_control_request(
                &directory,
                &ControlRequest {
                    nonce: "short",
                    ..stale
                }
            ),
            Err(CredentialError::ControlNonce)
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn devcert_writes_a_verifiable_chain() {
        let directory =
            std::env::temp_dir().join(format!("peren-devcert-{}", uuid::Uuid::new_v4()));
        devcert(&Devcert {
            directory: directory.clone(),
        })
        .unwrap();

        for file in ["ca.pem", "leaf-cert.pem", "leaf-key.pem"] {
            let path = directory.join(file);
            assert!(path.exists(), "missing {path:?}");
        }
        let output = std::process::Command::new("openssl")
            .args([
                "verify",
                "-CAfile",
                directory.join("ca.pem").to_str().unwrap(),
                directory.join("leaf-cert.pem").to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn minted_token_verifies_and_normalizes_scopes() {
        let directory =
            std::env::temp_dir().join(format!("peren-security-{}", uuid::Uuid::new_v4()));
        let token = mint(
            &directory,
            Mint {
                tenant: "acme".into(),
                bucket: "tenants/acme".into(),
                scopes: vec!["r2:write".into(), "r2:read".into(), "r2:read".into()],
            },
        )
        .unwrap();

        let claims = verify(&directory, &token).unwrap();

        assert_eq!(claims.tenant, "acme");
        assert_eq!(claims.bucket, "tenants/acme");
        assert_eq!(claims.scopes, ["r2:read", "r2:write"]);
        assert_eq!(claims.expires_at_ms - claims.issued_at_ms, TOKEN_TTL_MS);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn authorization_checks_scope_and_bucket_prefix() {
        let directory =
            std::env::temp_dir().join(format!("peren-security-{}", uuid::Uuid::new_v4()));
        let token = mint(
            &directory,
            Mint {
                tenant: "acme".into(),
                bucket: "tenants/acme".into(),
                scopes: vec!["r2:read".into()],
            },
        )
        .unwrap();

        authorize(&directory, &token, "r2:read", "tenants/acme/uploads").unwrap();
        assert!(matches!(
            authorize(&directory, &token, "r2:write", "tenants/acme/uploads"),
            Err(CredentialError::Permission(scope)) if scope == "r2:write"
        ));
        assert!(matches!(
            authorize(&directory, &token, "r2:read", "tenants/other/uploads"),
            Err(CredentialError::Bucket { expected, actual })
                if expected == "tenants/acme" && actual == "tenants/other/uploads"
        ));
        assert!(matches!(
            authorize(&directory, &token, "r2:read", "tenants/acmevil/uploads"),
            Err(CredentialError::Bucket { expected, actual })
                if expected == "tenants/acme" && actual == "tenants/acmevil/uploads"
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn expired_token_is_rejected() {
        let directory =
            std::env::temp_dir().join(format!("peren-security-{}", uuid::Uuid::new_v4()));
        let key = load_or_create_key(&directory).unwrap();
        let token = token(
            &key,
            Mint {
                tenant: "acme".into(),
                bucket: "tenants/acme".into(),
                scopes: vec!["r2:read".into()],
            },
            1_000,
        )
        .unwrap();

        assert!(matches!(
            verify_with_key(&key, &token, 1_000 + TOKEN_TTL_MS),
            Err(CredentialError::Expired)
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn tampered_token_is_rejected() {
        let directory =
            std::env::temp_dir().join(format!("peren-security-{}", uuid::Uuid::new_v4()));
        let mut token = mint(
            &directory,
            Mint {
                tenant: "acme".into(),
                bucket: "tenants/acme".into(),
                scopes: vec!["r2:read".into()],
            },
        )
        .unwrap();
        token.push('x');

        assert!(matches!(
            verify(&directory, &token),
            Err(CredentialError::InvalidSignature)
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
