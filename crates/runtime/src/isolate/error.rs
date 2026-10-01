use crate::{BundleError, LimitError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error(transparent)]
    Limit(#[from] LimitError),
    #[error("JavaScript execution failed: {0}")]
    JavaScript(String),
    #[error("Worker module has no default export")]
    MissingEntrypoint,
    #[error("Worker default export must provide a fetch function")]
    MissingFetch,
    #[error("V8 allocation failed")]
    Allocation,
    #[error("Worker exceeded its heap limit")]
    HeapLimit,
    #[error("Worker exceeded its execution-time limit")]
    ExecutionTime,
    #[error("execution watchdog failed")]
    Watchdog,
    #[error("request serialization failed: {0}")]
    Request(String),
    #[error("Worker response is not JSON-compatible: {0}")]
    Response(String),
}

impl EngineError {
    #[must_use]
    pub fn is_non_retryable(&self) -> bool {
        matches!(self, Self::JavaScript(message) if message.contains("NonRetryableError"))
    }
}
