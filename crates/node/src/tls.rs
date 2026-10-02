use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[cfg(test)]
pub(crate) fn client_with_identity(
    identity: Option<Identity>,
) -> Result<reqwest::Client, TlsError> {
    TrustPolicy::from_env().client_with_identity(identity)
}

pub(crate) fn client_with_identity_and_resolution(
    identity: Option<Identity>,
    resolution: Option<(&str, &[SocketAddr])>,
) -> Result<reqwest::Client, TlsError> {
    TrustPolicy::from_env().client_with_identity_and_resolution(identity, resolution)
}

#[derive(Clone, Debug)]
pub(crate) struct TrustPolicy {
    cert_file: Option<PathBuf>,
}

impl TrustPolicy {
    pub(crate) fn from_env() -> Self {
        Self {
            cert_file: std::env::var_os("SSL_CERT_FILE").map(PathBuf::from),
        }
    }

    #[cfg(test)]
    fn from_cert_file(cert_file: Option<PathBuf>) -> Self {
        Self { cert_file }
    }

    #[cfg(test)]
    pub(crate) fn client(&self) -> Result<reqwest::Client, TlsError> {
        self.client_with_identity(None)
    }

    #[cfg(test)]
    pub(crate) fn client_with_identity(
        &self,
        identity: Option<Identity>,
    ) -> Result<reqwest::Client, TlsError> {
        self.client_with_identity_and_resolution(identity, None)
    }

    pub(crate) fn client_with_identity_and_resolution(
        &self,
        identity: Option<Identity>,
        resolution: Option<(&str, &[SocketAddr])>,
    ) -> Result<reqwest::Client, TlsError> {
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if let Some(path) = &self.cert_file {
            for certificate in certificates(path)? {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(identity) = identity {
            builder = builder.identity(identity.into_reqwest()?);
        }
        if let Some((domain, addresses)) = resolution {
            builder = builder.resolve_to_addrs(domain, addresses);
        }
        builder.build().map_err(TlsError::Client)
    }

    #[cfg(test)]
    fn uses_cert_file(&self) -> bool {
        self.cert_file.is_some()
    }
}

fn certificates(path: &Path) -> Result<Vec<reqwest::Certificate>, TlsError> {
    let bytes = std::fs::read(path).map_err(|source| TlsError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let certificates =
        reqwest::Certificate::from_pem_bundle(&bytes).map_err(|source| TlsError::Certificate {
            path: path.to_path_buf(),
            source,
        })?;
    if certificates.is_empty() {
        return Err(TlsError::Empty {
            path: path.to_path_buf(),
        });
    }
    Ok(certificates)
}

#[derive(Clone)]
pub(crate) struct Identity {
    cert_pem: String,
    key_pem: String,
}

impl Identity {
    #[must_use]
    pub(crate) fn new(cert_pem: String, key_pem: String) -> Self {
        Self { cert_pem, key_pem }
    }

    fn into_reqwest(self) -> Result<reqwest::Identity, TlsError> {
        let pem = format!(
            "{}\n{}",
            self.cert_pem.trim_end(),
            self.key_pem.trim_start()
        );
        reqwest::Identity::from_pem(pem.as_bytes()).map_err(TlsError::Identity)
    }
}

#[derive(Debug, Error)]
pub(crate) enum TlsError {
    #[error("failed to read SSL_CERT_FILE {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("SSL_CERT_FILE {path:?} did not contain any PEM certificates")]
    Empty { path: PathBuf },
    #[error("failed to parse SSL_CERT_FILE {path:?}")]
    Certificate {
        path: PathBuf,
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to parse outbound mTLS identity")]
    Identity(#[source] reqwest::Error),
    #[error("failed to build outbound TLS client")]
    Client(#[source] reqwest::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CA: &str = r"-----BEGIN CERTIFICATE-----
MIIDETCCAfmgAwIBAgIUOZLc9+7vtK2zwcj2vsEWSlogcYgwDQYJKoZIhvcNAQEL
BQAwGDEWMBQGA1UEAwwNcGVyZW4tdGVzdC1jYTAeFw0yNjA5MzAwOTMyNTdaFw0y
NjEwMDEwOTMyNTdaMBgxFjAUBgNVBAMMDXBlcmVuLXRlc3QtY2EwggEiMA0GCSqG
SIb3DQEBAQUAA4IBDwAwggEKAoIBAQCo6VUQdi3K+ZDE8U9dPPS790HB42FxubcI
2BvHJPHFn/GKrDowngsTMHjZRDi7cAuB7QTPzVNXuPYg/9utwNlfjLKzAc/QnIRp
cb+pCQnrTiYkKsNSHlRl1oBKWcCugzL4QJ0lgxSsZHL8Ohl2rEVdbxcyFlF6i39l
9U9kcKWvcqD+rbS8prWTR3Clxyu5/3uFVfT9bV2+as5WZEV7KKNEIk2eUblPGIv9
meecOKE/FRWfDSE320qGK161PzZgLhXMkzjwot3d7s9Y4CLFMPFs5/RqDVEu15EQ
voOqp6geznyUtuSdK0qMMcVIVQggA0FTG+b9nBi1x+ovDuzqpanXAgMBAAGjUzBR
MB0GA1UdDgQWBBSBgHu84bFkI1CYkD3XEcq2qZ5nhDAfBgNVHSMEGDAWgBSBgHu8
4bFkI1CYkD3XEcq2qZ5nhDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA
A4IBAQCJ/3qlRVmM72NrXqSmexaB5o2jxgkhnzcl70ERzHvKBdRVXx/iF+DnlMya
atXMQWGC3oQgUIXDBJR1Oo464l6hsPH8XDGpm3Vt6XMEASSyT36pbIfa95J0zkQH
UbhHFc75HkcEdwbJ9woooLBGrvGzspuKU88diuEqJGzkyhNsDBsKqwqds4C31KR9
+dTjVrrb8gyhGtmS3uZfmbgqJDCBmgizfnNTqB/FvhldD1oPuR7zGE7wDomIazJ9
1ytKYF7aWY0QLI0PhHOEmb90VfhPbaNMTIie1+lueo/eRJT0AVtF4QjNSajULIlY
WEZn1/19Yy4n+d3sR5/WivrpQ1MZ
-----END CERTIFICATE-----
";

    #[test]
    fn private_ca_bundle_builds_outbound_client() {
        let path = std::env::temp_dir().join(format!("peren-ca-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, TEST_CA).unwrap();

        TrustPolicy::from_cert_file(Some(path.clone()))
            .client()
            .unwrap();

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trust_policy_is_reusable_beyond_http_fetch_call_sites() {
        let policy = TrustPolicy::from_cert_file(None);

        assert!(!policy.uses_cert_file());
        policy.client().unwrap();
    }

    #[tokio::test]
    async fn outbound_client_does_not_follow_redirects() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /redirect HTTP/1.1"), "{request}");
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:9/control/v1/node/drain\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let client = TrustPolicy::from_cert_file(None).client().unwrap();

        let response = client
            .get(format!("http://{address}/redirect"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::LOCATION)
                .unwrap()
                .to_str()
                .unwrap(),
            "http://127.0.0.1:9/control/v1/node/drain"
        );
        server.join().unwrap();
    }

    #[test]
    fn missing_cert_file_error_does_not_include_file_contents() {
        let path = std::env::temp_dir().join("peren-missing-ca.pem");
        let error = TrustPolicy::from_cert_file(Some(path))
            .client()
            .unwrap_err()
            .to_string();

        assert!(error.contains("SSL_CERT_FILE"));
        assert!(error.contains("peren-missing-ca.pem"));
    }

    #[test]
    fn invalid_mtls_identity_error_does_not_include_cert_or_key() {
        let error = client_with_identity(Some(Identity::new(
            "not certificate secret material".into(),
            "not private key secret material".into(),
        )))
        .unwrap_err()
        .to_string();

        assert!(error.contains("mTLS identity"));
        assert!(!error.contains("certificate secret material"));
        assert!(!error.contains("private key secret material"));
    }

    #[test]
    fn invalid_cert_file_error_does_not_include_pem_contents() {
        let path =
            std::env::temp_dir().join(format!("peren-invalid-ca-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, "not a certificate secret material").unwrap();
        let error = TrustPolicy::from_cert_file(Some(path.clone()))
            .client()
            .unwrap_err()
            .to_string();

        assert!(error.contains("SSL_CERT_FILE"));
        assert!(!error.contains("secret material"));
        let _ = std::fs::remove_file(path);
    }
}
