#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Provider {
    Memory,
    File,
    S3,
    Minio,
    CloudflareR2,
    DigitalOceanSpaces,
    SeaweedFs,
    Gcs,
    Tigris,
    AzureBlob,
}

impl Provider {
    #[must_use]
    pub const fn status(self) -> ProviderStatus {
        match self {
            Self::Memory
            | Self::File
            | Self::S3
            | Self::Minio
            | Self::CloudflareR2
            | Self::DigitalOceanSpaces
            | Self::SeaweedFs
            | Self::Gcs
            | Self::Tigris
            | Self::AzureBlob => ProviderStatus::Supported,
        }
    }

    #[must_use]
    pub const fn requires_conditional_put(self) -> bool {
        matches!(
            self,
            Self::Memory
                | Self::File
                | Self::S3
                | Self::Minio
                | Self::CloudflareR2
                | Self::DigitalOceanSpaces
                | Self::SeaweedFs
                | Self::Gcs
                | Self::Tigris
                | Self::AzureBlob
        )
    }

    #[must_use]
    pub const fn requires_metadata_normalization(self) -> bool {
        matches!(self, Self::AzureBlob)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderStatus {
    Supported,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialMode {
    StaticKeys,
    SessionToken,
    InstanceRole,
    WorkloadIdentity,
    EksPodIdentity,
}

impl CredentialMode {
    #[must_use]
    pub const fn status(self) -> ProviderStatus {
        match self {
            Self::StaticKeys
            | Self::SessionToken
            | Self::InstanceRole
            | Self::WorkloadIdentity
            | Self::EksPodIdentity => ProviderStatus::Supported,
        }
    }
}

pub const PROVIDERS: &[Provider] = &[
    Provider::Memory,
    Provider::File,
    Provider::S3,
    Provider::Minio,
    Provider::CloudflareR2,
    Provider::DigitalOceanSpaces,
    Provider::SeaweedFs,
    Provider::Gcs,
    Provider::Tigris,
    Provider::AzureBlob,
];

pub const CREDENTIALS: &[CredentialMode] = &[
    CredentialMode::StaticKeys,
    CredentialMode::SessionToken,
    CredentialMode::InstanceRole,
    CredentialMode::WorkloadIdentity,
    CredentialMode::EksPodIdentity,
];

pub struct S3Options {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub virtual_hosted: bool,
}

pub struct S3Credentials {
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) token: Option<String>,
}

impl S3Credentials {
    #[must_use]
    pub fn new(access_key: String, secret_key: String, token: Option<String>) -> Self {
        Self {
            access_key,
            secret_key,
            token,
        }
    }
}

pub struct AzureOptions {
    pub account: String,
    pub container: String,
    pub endpoint: Option<String>,
    pub emulator: bool,
}

pub struct AzureCredentials {
    pub(crate) access_key: String,
}

impl AzureCredentials {
    #[must_use]
    pub fn new(access_key: String) -> Self {
        Self { access_key }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("object store configuration is invalid")]
pub struct BuildError(#[from] pub(crate) object_store::Error);

#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    #[error("probe identifier is invalid")]
    InvalidProbe,
    #[error("object store is unavailable during conformance verification")]
    Unavailable,
    #[error("object store does not provide required ETag conditional writes")]
    Unsupported,
    #[error("object store returned an incorrect ranged read")]
    Range,
}

pub(crate) fn validate_probe(probe: &str) -> Result<(), ConformanceError> {
    if probe.is_empty()
        || probe.len() > 64
        || !probe
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(ConformanceError::InvalidProbe);
    }
    Ok(())
}
