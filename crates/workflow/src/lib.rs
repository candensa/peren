mod context;
mod error;
pub mod memory;

pub use context::{Context, Sleep};
pub use error::WorkflowError;

use peren_primitives::CellId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    pub cell: CellId,
    pub name: String,
}

impl Instance {
    #[must_use]
    pub fn new(cell: CellId, name: impl Into<String>) -> Self {
        Self {
            cell,
            name: name.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub bundle: [u8; 32],
    pub created: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub sequence: u64,
    pub name: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityDefinition {
    pub name: String,
    pub task: String,
    pub timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityLease {
    pub name: String,
    pub token: String,
    pub leased_until_ms: i64,
    pub heartbeat_deadline_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActivityStatus {
    Pending,
    Leased,
    Completed,
    Canceled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityState {
    pub definition: ActivityDefinition,
    pub status: ActivityStatus,
    pub lease: Option<ActivityLease>,
    pub result: Option<Vec<u8>>,
    pub cancel_reason: Option<String>,
    pub updated_at_ms: i64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Cutover {
    pub instance: Instance,
    pub from: [u8; 32],
    pub to: [u8; 32],
    pub operator: String,
    pub attested: bool,
    pub timestamp: i64,
}

pub trait Journal: Send + Sync {
    fn create(
        &self,
        instance: &Instance,
        bundle: [u8; 32],
        timestamp: i64,
    ) -> impl Future<Output = Result<(), WorkflowError>> + Send;

    fn metadata(
        &self,
        instance: &Instance,
    ) -> impl Future<Output = Result<Option<Metadata>, WorkflowError>> + Send;

    fn entries(
        &self,
        instance: &Instance,
        from: u64,
    ) -> impl Future<Output = Result<Vec<Entry>, WorkflowError>> + Send;

    fn append(
        &self,
        instance: &Instance,
        entry: Entry,
    ) -> impl Future<Output = Result<(), WorkflowError>> + Send;

    fn retain_activity(
        &self,
        instance: &Instance,
        definition: ActivityDefinition,
        timestamp: i64,
    ) -> impl Future<Output = Result<ActivityState, WorkflowError>> + Send;

    fn lease_activity(
        &self,
        instance: &Instance,
        name: &str,
        token: String,
        now_ms: i64,
    ) -> impl Future<Output = Result<ActivityLease, WorkflowError>> + Send;

    fn heartbeat_activity(
        &self,
        instance: &Instance,
        name: &str,
        token: &str,
        now_ms: i64,
    ) -> impl Future<Output = Result<ActivityLease, WorkflowError>> + Send;

    fn complete_activity(
        &self,
        instance: &Instance,
        name: &str,
        token: &str,
        result: Vec<u8>,
        timestamp: i64,
    ) -> impl Future<Output = Result<ActivityState, WorkflowError>> + Send;

    fn cancel_activity(
        &self,
        instance: &Instance,
        name: &str,
        reason: Option<String>,
        timestamp: i64,
    ) -> impl Future<Output = Result<ActivityState, WorkflowError>> + Send;

    fn cutover(
        &self,
        instance: &Instance,
        bundle: [u8; 32],
        operator: &str,
        attested: bool,
        timestamp: i64,
    ) -> impl Future<Output = Result<Cutover, WorkflowError>> + Send;

    fn begin(&self, instance: &Instance) -> impl Future<Output = ()> + Send;

    fn finish(&self, instance: &Instance) -> impl Future<Output = ()> + Send;
}
