mod alarm;
mod connection;
mod error;
mod kv;
mod record;
mod replica;
mod schema;
mod sql;
mod store;
mod transaction;
mod vector;

pub use alarm::Alarm;
pub use error::StorageError;
pub use kv::{
    InvalidListLimit, KvEntries, ListOptions, ListPage, MAX_ENTRY_BYTES, MAX_LIST_ENTRIES,
};
pub use record::{
    CheckpointBytes, Committed, EffectDraft, EffectRecord, EffectStatus, MutationRecord,
    ReplicaBytes,
};
pub use sql::{
    MAX_SQL_BYTES, MAX_SQL_COLUMNS, MAX_SQL_PARAMETERS, SqlLimitError, SqlResult, SqlValue,
    validate_sql,
};
pub use store::{AlarmStore, AttachmentStore, CellStore, ListStore, MutationStore, SqlStore};
pub use transaction::StorageTransaction;

pub const MAX_ATTACHMENT_BYTES: usize = 16_384;

mod engine;
pub use engine::CellStorage;
