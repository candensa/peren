use std::sync::Arc;

use object_store::{ObjectStore, PutPayload, path::Path};
use peren_replication::RepositoryError as ReplicaError;

#[derive(Clone)]
pub struct TransactionalBlobStore {
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BlobManifest {
    pub key: String,
    pub transaction: String,
    pub parts: Vec<BlobPart>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BlobPart {
    pub index: u32,
    pub size: usize,
}

impl TransactionalBlobStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn stage(
        &self,
        key: &str,
        transaction: &str,
        index: u32,
        bytes: &[u8],
    ) -> Result<BlobPart, ReplicaError> {
        let part = BlobPart {
            index,
            size: bytes.len(),
        };
        self.store
            .put(
                &blob_part_key(key, transaction, index),
                PutPayload::from(bytes.to_vec()),
            )
            .await
            .map_err(|_| ReplicaError::Unavailable)?;
        Ok(part)
    }

    pub async fn commit(
        &self,
        key: &str,
        transaction: &str,
        mut parts: Vec<BlobPart>,
    ) -> Result<BlobManifest, ReplicaError> {
        parts.sort_by_key(|part| part.index);
        for part in &parts {
            let bytes = self
                .store
                .get(&blob_part_key(key, transaction, part.index))
                .await
                .map_err(|_| ReplicaError::Unavailable)?
                .bytes()
                .await
                .map_err(|_| ReplicaError::Unavailable)?;
            if bytes.len() != part.size {
                return Err(ReplicaError::Malformed);
            }
        }
        let manifest = BlobManifest {
            key: key.to_string(),
            transaction: transaction.to_string(),
            parts,
        };
        let bytes = serde_json::to_vec(&manifest).map_err(|_| ReplicaError::Malformed)?;
        self.store
            .put(&blob_manifest_key(key), PutPayload::from(bytes))
            .await
            .map_err(|_| ReplicaError::Unavailable)?;
        Ok(manifest)
    }

    pub async fn manifest(&self, key: &str) -> Result<Option<BlobManifest>, ReplicaError> {
        match self.store.get(&blob_manifest_key(key)).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|_| ReplicaError::Unavailable)?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|_| ReplicaError::Malformed)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(_) => Err(ReplicaError::Unavailable),
        }
    }

    pub async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, ReplicaError> {
        let Some(manifest) = self.manifest(key).await? else {
            return Ok(None);
        };
        let mut blob = Vec::new();
        for part in manifest.parts {
            let bytes = self
                .store
                .get(&blob_part_key(key, &manifest.transaction, part.index))
                .await
                .map_err(|_| ReplicaError::Unavailable)?
                .bytes()
                .await
                .map_err(|_| ReplicaError::Unavailable)?;
            if bytes.len() != part.size {
                return Err(ReplicaError::Malformed);
            }
            blob.extend_from_slice(&bytes);
        }
        Ok(Some(blob))
    }
}

fn blob_manifest_key(key: &str) -> Path {
    Path::from(format!("blobs/{key}/manifest.json"))
}

fn blob_part_key(key: &str, transaction: &str, index: u32) -> Path {
    Path::from(format!(
        "blobs/{key}/staging/{transaction}/{index:020}.part"
    ))
}
