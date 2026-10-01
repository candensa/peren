use peren_primitives::{CellId, DurabilityReceipt, StorageRevision};
use uuid::Uuid;

use crate::StorageError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectDraft {
    pub id: Uuid,
    pub destination: String,
    pub inbox_key: String,
    pub payload: Vec<u8>,
    pub due_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectRecord {
    pub id: Uuid,
    pub destination: String,
    pub inbox_key: String,
    pub payload: Vec<u8>,
    pub status: EffectStatus,
    pub attempts: u32,
    pub due_at_ms: i64,
    pub lease_token: Option<String>,
    pub leased_until_ms: Option<i64>,
    pub revision: StorageRevision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectStatus {
    Pending,
    Leased,
    Acknowledged,
}

impl EffectStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Acknowledged => "acknowledged",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "acknowledged" => Ok(Self::Acknowledged),
            _ => Err(StorageError::MalformedEffectStatus(value.to_string())),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationRecord {
    pub id: Uuid,
    pub outcome: Vec<u8>,
    pub revision: StorageRevision,
}

pub struct Committed<T> {
    pub value: T,
    pub revision: StorageRevision,
}

impl<T> Committed<T> {
    #[must_use]
    pub const fn receipt(&self, cell: CellId) -> DurabilityReceipt {
        DurabilityReceipt::new(cell, self.revision)
    }
}
pub struct ReplicaBytes {
    pub database: Vec<u8>,
    pub header: Option<[u8; 32]>,
    pub frames: Vec<u8>,
    pub offset: u64,
}
pub struct CheckpointBytes {
    pub database: Vec<u8>,
    pub revision: StorageRevision,
}
