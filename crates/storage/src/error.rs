#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error(transparent)]
    SqlLimit(#[from] crate::sql::SqlLimitError),
    #[error(transparent)]
    InvalidListLimit(#[from] crate::kv::InvalidListLimit),
    #[error("SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("SQL statement failed: {query}")]
    Sql {
        query: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("key and value use {actual} bytes; the limit is {limit}")]
    EntryTooLarge { actual: usize, limit: usize },
    #[error("WebSocket attachment uses {actual} bytes; the limit is {limit}")]
    AttachmentTooLarge { actual: usize, limit: usize },
    #[error("storage revision is exhausted")]
    RevisionOverflow,
    #[error("mutation outcome {0} already exists")]
    DuplicateMutation(uuid::Uuid),
    #[error("effect status {0:?} is malformed")]
    MalformedEffectStatus(String),
    #[error("a storage transaction is already open")]
    TransactionOpen,
    #[error("no storage transaction is open")]
    TransactionClosed,
    #[error("failed to read SQLite replica bytes")]
    ReadReplica(#[source] std::io::Error),
    #[error("SQLite WAL has an invalid byte layout")]
    MalformedWal,
}
