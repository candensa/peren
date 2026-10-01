use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::{
    Attribute, AttributeValue, Attributes, ObjectStore, PutOptions, PutPayload, path::Path,
};

use crate::{
    AzureCredentials, AzureOptions, BucketStore, BuildError, S3Credentials, S3Options,
    tls::client_options,
};

#[derive(Clone)]
pub struct R2Store {
    store: Arc<dyn ObjectStore>,
}

impl R2Store {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn file(root: impl AsRef<std::path::Path>) -> Result<Self, BuildError> {
        let store = LocalFileSystem::new_with_prefix(root).map_err(BuildError)?;
        Ok(Self::new(Arc::new(store)))
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
        BucketStore::s3(options, credentials).map(|store| Self { store: store.store })
    }

    pub fn azure(options: AzureOptions, credentials: AzureCredentials) -> Result<Self, BuildError> {
        BucketStore::azure(options, credentials).map(|store| Self { store: store.store })
    }
}

#[async_trait::async_trait]
impl peren_runtime::R2BucketHost for R2Store {
    async fn put(&self, object: peren_runtime::R2Put) -> Result<(), peren_runtime::HostError> {
        let mut attributes = Attributes::new();
        if let Some(content_type) = object.content_type {
            attributes.insert(Attribute::ContentType, AttributeValue::from(content_type));
        }
        for (key, value) in object.custom_metadata {
            attributes.insert(Attribute::Metadata(key.into()), AttributeValue::from(value));
        }
        self.store
            .put_opts(
                &Path::from(object.key),
                PutPayload::from(object.body),
                PutOptions {
                    attributes,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(|_| peren_runtime::HostError)
    }

    async fn get(
        &self,
        object: peren_runtime::R2Get,
    ) -> Result<Option<peren_runtime::R2Object>, peren_runtime::HostError> {
        let key = object.key;
        let result = match self.store.get(&Path::from(key.clone())).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(peren_runtime::HostError),
        };
        let content_type = result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_string());
        let custom_metadata = custom_metadata(&result.attributes);
        let body = result
            .bytes()
            .await
            .map_err(|_| peren_runtime::HostError)?
            .to_vec();
        Ok(Some(peren_runtime::R2Object {
            key,
            size: body.len(),
            body,
            content_type,
            custom_metadata,
        }))
    }

    async fn delete(
        &self,
        object: peren_runtime::R2Delete,
    ) -> Result<(), peren_runtime::HostError> {
        match self.store.delete(&Path::from(object.key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(_) => Err(peren_runtime::HostError),
        }
    }

    async fn list(
        &self,
        request: peren_runtime::R2List,
    ) -> Result<peren_runtime::R2ListPage, peren_runtime::HostError> {
        let limit = request.limit.unwrap_or(1000).max(1);
        let prefix = Path::from(request.prefix.unwrap_or_default());
        let cursor = request.cursor.unwrap_or_default();
        let mut stream = self.store.list(Some(&prefix));
        let mut objects = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item.map_err(|_| peren_runtime::HostError)?;
            let key = item.location.to_string();
            if key <= cursor {
                continue;
            }
            objects.push(peren_runtime::R2ObjectEntry {
                key,
                size: item.size,
                custom_metadata: BTreeMap::new(),
            });
            if objects.len() > limit {
                break;
            }
        }
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        let cursor = if objects.len() > limit {
            objects.truncate(limit);
            objects.last().map(|object| object.key.clone())
        } else {
            None
        };
        Ok(peren_runtime::R2ListPage {
            objects,
            list_complete: cursor.is_none(),
            cursor,
        })
    }
}

fn custom_metadata(attributes: &Attributes) -> BTreeMap<String, String> {
    attributes
        .iter()
        .filter_map(|(attribute, value)| match attribute {
            Attribute::Metadata(key) => Some((key.to_string(), value.as_ref().to_string())),
            _ => None,
        })
        .collect()
}
