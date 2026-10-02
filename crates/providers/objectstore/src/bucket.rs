use std::{ops::Range, sync::Arc};

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path};

use crate::{
    AzureCredentials, AzureOptions, BuildError, ConformanceError, S3Credentials, S3Options,
};
use crate::{config::validate_probe, tls::client_options};

#[derive(Clone)]
pub struct BucketStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) local_conditional_fallback: Option<Arc<tokio::sync::Mutex<()>>>,
}

impl BucketStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            local_conditional_fallback: None,
        }
    }

    pub fn file(root: impl AsRef<std::path::Path>) -> Result<Self, BuildError> {
        let store = LocalFileSystem::new_with_prefix(root).map_err(BuildError)?;
        Ok(Self {
            store: Arc::new(store),
            local_conditional_fallback: Some(Arc::new(tokio::sync::Mutex::new(()))),
        })
    }

    pub fn s3_instance_role(options: S3Options) -> Result<Self, BuildError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(options.bucket)
            .with_region(options.region)
            .with_allow_http(options.allow_http)
            .with_virtual_hosted_style_request(options.virtual_hosted)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(client_options()?);
        if let Some(endpoint) = options.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        Ok(Self::new(Arc::new(builder.build()?)))
    }

    pub fn s3(options: S3Options, credentials: S3Credentials) -> Result<Self, BuildError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(options.bucket)
            .with_region(options.region)
            .with_access_key_id(credentials.access_key)
            .with_secret_access_key(credentials.secret_key)
            .with_allow_http(options.allow_http)
            .with_virtual_hosted_style_request(options.virtual_hosted)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(client_options()?);
        if let Some(endpoint) = options.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(token) = credentials.token {
            builder = builder.with_token(token);
        }
        Ok(Self::new(Arc::new(builder.build()?)))
    }

    pub fn azure(options: AzureOptions, credentials: AzureCredentials) -> Result<Self, BuildError> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(options.account)
            .with_container_name(options.container)
            .with_access_key(credentials.access_key)
            .with_client_options(client_options()?);
        if options.emulator {
            builder = builder.with_use_emulator(true);
        }
        if let Some(endpoint) = options.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        Ok(Self::new(Arc::new(builder.build()?)))
    }

    pub async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, BuildError> {
        match self.store.get(&Path::from(key)).await {
            Ok(result) => result
                .bytes()
                .await
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|error| {
                    BuildError(object_store::Error::Generic {
                        store: "object",
                        source: Box::new(error),
                    })
                }),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(BuildError(error)),
        }
    }

    pub async fn read_range(
        &self,
        key: &str,
        range: Range<usize>,
    ) -> Result<Option<Vec<u8>>, BuildError> {
        match self.store.get_range(&Path::from(key), range).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(BuildError(error)),
        }
    }

    pub async fn write(&self, key: &str, bytes: Vec<u8>) -> Result<(), BuildError> {
        self.store
            .put(&Path::from(key), PutPayload::from(bytes))
            .await
            .map(|_| ())
            .map_err(BuildError)
    }

    pub async fn verify_cas(&self, probe: &str) -> Result<(), ConformanceError> {
        validate_probe(probe)?;
        let key = Path::from(format!("probe/{probe}.bin"));
        let created = self
            .store
            .put_opts(
                &key,
                PutPayload::from_static(b"created"),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .map_err(|_| ConformanceError::Unavailable)?;
        let version = created.e_tag.ok_or(ConformanceError::Unsupported)?;
        let update = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some(version.clone()),
                version: None,
            }),
            ..PutOptions::default()
        };
        let updated = self
            .store
            .put_opts(&key, PutPayload::from_static(b"updated"), update)
            .await
            .map_err(|_| ConformanceError::Unavailable)?;
        if updated.e_tag.is_none() {
            self.store.delete(&key).await.ok();
            return Err(ConformanceError::Unsupported);
        }
        let stale = self
            .store
            .put_opts(
                &key,
                PutPayload::from_static(b"stale"),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(version),
                        version: None,
                    }),
                    ..PutOptions::default()
                },
            )
            .await;
        self.store
            .delete(&key)
            .await
            .map_err(|_| ConformanceError::Unavailable)?;
        match stale {
            Err(object_store::Error::Precondition { .. }) => Ok(()),
            Err(_) => Err(ConformanceError::Unavailable),
            Ok(_) => Err(ConformanceError::Unsupported),
        }
    }

    pub async fn verify_range_read(&self, probe: &str) -> Result<(), ConformanceError> {
        validate_probe(probe)?;
        let key = format!("probe/{probe}-range.bin");
        let path = Path::from(key.as_str());
        self.store
            .put(&path, PutPayload::from_static(b"0123456789abcdef"))
            .await
            .map_err(|_| ConformanceError::Unavailable)?;
        let bytes = self
            .read_range(&key, 4..10)
            .await
            .map_err(|_| ConformanceError::Unavailable)?
            .ok_or(ConformanceError::Unavailable)?;
        self.store
            .delete(&path)
            .await
            .map_err(|_| ConformanceError::Unavailable)?;
        if bytes == b"456789" {
            Ok(())
        } else {
            Err(ConformanceError::Range)
        }
    }
}
