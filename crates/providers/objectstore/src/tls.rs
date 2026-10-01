use std::path::{Path, PathBuf};

use object_store::{Certificate, ClientOptions};

use crate::BuildError;

pub(crate) fn client_options() -> Result<ClientOptions, BuildError> {
    let cert = std::env::var_os("SSL_CERT_FILE").map(PathBuf::from);
    client_options_from_cert_file(cert.as_deref())
}

fn client_options_from_cert_file(cert: Option<&Path>) -> Result<ClientOptions, BuildError> {
    let mut options = ClientOptions::new();
    if let Some(path) = cert {
        let bytes = std::fs::read(path).map_err(|source| error(path, &source))?;
        let certificates = Certificate::from_pem_bundle(&bytes).map_err(BuildError)?;
        if certificates.is_empty() {
            let source = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "SSL_CERT_FILE did not contain any PEM certificates",
            );
            return Err(error(path, &source));
        }
        for certificate in certificates {
            options = options.with_root_certificate(certificate);
        }
    }
    Ok(options)
}

fn error(path: &Path, source: &std::io::Error) -> BuildError {
    BuildError(object_store::Error::Generic {
        store: "tls",
        source: Box::new(std::io::Error::new(
            source.kind(),
            format!(
                "failed to load SSL_CERT_FILE {}: {}",
                path.display(),
                source.kind()
            ),
        )),
    })
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
    fn private_ca_bundle_builds_provider_client_options() {
        let path =
            std::env::temp_dir().join(format!("peren-object-ca-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, TEST_CA).unwrap();

        client_options_from_cert_file(Some(&path)).unwrap();

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_cert_file_error_does_not_include_pem_contents() {
        let path =
            std::env::temp_dir().join(format!("peren-object-ca-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, "not a certificate secret material").unwrap();
        let error = client_options_from_cert_file(Some(&path))
            .unwrap_err()
            .to_string();

        assert!(error.contains("object store configuration is invalid"));
        assert!(!error.contains("secret material"));
        let _ = std::fs::remove_file(path);
    }
}
