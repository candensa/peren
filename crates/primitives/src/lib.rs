use std::{fmt, sync::Arc};

use thiserror::Error;
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct CellId([u8; 32]);

impl CellId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct NodeId(Uuid);

impl NodeId {
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct OwnershipEpoch(u64);

impl OwnershipEpoch {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self, EpochOverflow> {
        self.0.checked_add(1).map(Self).ok_or(EpochOverflow)
    }
}

#[derive(Debug, Error)]
#[error("ownership epoch is exhausted")]
pub struct EpochOverflow;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct StorageRevision(u64);

impl StorageRevision {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct DurabilityReceipt {
    cell: CellId,
    revision: StorageRevision,
}

impl DurabilityReceipt {
    #[must_use]
    pub const fn new(cell: CellId, revision: StorageRevision) -> Self {
        Self { cell, revision }
    }

    #[must_use]
    pub const fn cell(self) -> CellId {
        self.cell
    }

    #[must_use]
    pub const fn revision(self) -> StorageRevision {
        self.revision
    }

    #[must_use]
    pub fn is_satisfied_by(self, cell: CellId, revision: StorageRevision) -> bool {
        self.cell.0 == cell.0 && revision.0 >= self.revision.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ServiceName(Arc<str>);

impl ServiceName {
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self, InvalidName> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 63
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
        valid.then_some(Self(value)).ok_or(InvalidName)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error)]
#[error("name must contain 1 to 63 ASCII letters, digits, hyphens, or underscores")]
pub struct InvalidName;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_increment_refuses_wraparound() {
        assert!(OwnershipEpoch::new(u64::MAX).next().is_err());
    }

    #[test]
    fn durability_receipt_requires_same_cell_and_minimum_revision() {
        let cell = CellId::from_bytes([1; 32]);
        let other = CellId::from_bytes([2; 32]);
        let receipt = DurabilityReceipt::new(cell, StorageRevision::new(7));

        assert!(receipt.is_satisfied_by(cell, StorageRevision::new(7)));
        assert!(receipt.is_satisfied_by(cell, StorageRevision::new(8)));
        assert!(!receipt.is_satisfied_by(cell, StorageRevision::new(6)));
        assert!(!receipt.is_satisfied_by(other, StorageRevision::new(8)));
    }

    #[test]
    fn service_name_rejects_path_syntax() {
        assert!(ServiceName::parse("billing/worker").is_err());
    }
}
