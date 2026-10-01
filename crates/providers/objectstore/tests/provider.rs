use std::sync::Arc;

use object_store::memory::InMemory;
use peren_provider_object_store::{
    AzureCredentials, AzureOptions, BucketStore, CredentialMode, PROVIDERS, Provider,
    ProviderStatus, S3Credentials, S3Options,
};
use uuid::Uuid;

#[tokio::test]
async fn conformance_probe_proves_stale_etag_rejection() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    store.verify_cas(&Uuid::new_v4().to_string()).await.unwrap();
}

#[tokio::test]
async fn bucket_store_reads_exact_byte_ranges() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    store
        .write("objects/range.txt", b"abcdefghij".to_vec())
        .await
        .unwrap();

    let bytes = store.read_range("objects/range.txt", 2..7).await.unwrap();
    let missing = store.read_range("objects/missing.txt", 2..7).await.unwrap();

    assert_eq!(bytes.as_deref(), Some(&b"cdefg"[..]));
    assert_eq!(missing, None);
}

#[tokio::test]
async fn conformance_probe_proves_range_reads() {
    let store = BucketStore::new(Arc::new(InMemory::new()));
    store
        .verify_range_read(&Uuid::new_v4().to_string())
        .await
        .unwrap();
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set for the object-store provider proof"))
}

fn azure_store() -> BucketStore {
    BucketStore::azure(
        AzureOptions {
            account: required_env("PEREN_AZURE_ACCOUNT"),
            container: required_env("PEREN_AZURE_CONTAINER"),
            endpoint: Some(required_env("PEREN_AZURE_ENDPOINT")),
            emulator: std::env::var("PEREN_AZURE_EMULATOR")
                .is_ok_and(|value| value == "1" || value == "true"),
        },
        AzureCredentials::new(required_env("PEREN_AZURE_ACCESS_KEY")),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires an S3-compatible service configured through PEREN_S3_* variables"]
async fn s3_service_rejects_stale_etags() {
    let endpoint = required_env("PEREN_S3_ENDPOINT");
    let bucket = required_env("PEREN_S3_BUCKET");
    let access_key = required_env("PEREN_S3_ACCESS_KEY");
    let secret_key = required_env("PEREN_S3_SECRET_KEY");
    let store = BucketStore::s3(
        S3Options {
            bucket,
            region: "us-east-1".to_owned(),
            endpoint: Some(endpoint),
            allow_http: true,
            virtual_hosted: false,
        },
        S3Credentials::new(access_key, secret_key, None),
    )
    .unwrap();

    store.verify_cas(&Uuid::new_v4().to_string()).await.unwrap();
}

#[tokio::test]
#[ignore = "requires Azure Blob or Azurite configured through PEREN_AZURE_* variables"]
async fn azure_service_rejects_stale_etags() {
    let store = azure_store();
    store.verify_cas(&Uuid::new_v4().to_string()).await.unwrap();
    store
        .verify_range_read(&Uuid::new_v4().to_string())
        .await
        .unwrap();
}

#[test]
fn provider_matrix_names_supported_and_unsupported_backends() {
    let supported = PROVIDERS
        .iter()
        .copied()
        .filter(|provider| provider.status() == ProviderStatus::Supported)
        .collect::<Vec<_>>();
    assert!(supported.contains(&Provider::Memory));
    assert!(supported.contains(&Provider::File));
    assert!(supported.contains(&Provider::S3));
    assert!(supported.contains(&Provider::Minio));
    assert!(supported.contains(&Provider::CloudflareR2));
    assert!(supported.contains(&Provider::DigitalOceanSpaces));
    assert!(supported.contains(&Provider::SeaweedFs));
    assert!(supported.contains(&Provider::Gcs));
    assert!(supported.contains(&Provider::Tigris));
    assert!(supported.contains(&Provider::AzureBlob));
}

#[test]
fn provider_matrix_records_celld_failure_classes() {
    assert!(Provider::DigitalOceanSpaces.requires_conditional_put());
    assert!(Provider::SeaweedFs.requires_conditional_put());
    assert!(Provider::Gcs.requires_conditional_put());
    assert!(Provider::Tigris.requires_conditional_put());
    assert!(Provider::AzureBlob.requires_metadata_normalization());
}

#[test]
fn credential_matrix_supports_ambient_cloud_identity() {
    assert_eq!(
        CredentialMode::StaticKeys.status(),
        ProviderStatus::Supported
    );
    assert_eq!(
        CredentialMode::SessionToken.status(),
        ProviderStatus::Supported
    );
    assert_eq!(
        CredentialMode::InstanceRole.status(),
        ProviderStatus::Supported
    );
    assert_eq!(
        CredentialMode::WorkloadIdentity.status(),
        ProviderStatus::Supported
    );
    assert_eq!(
        CredentialMode::EksPodIdentity.status(),
        ProviderStatus::Supported
    );
}
