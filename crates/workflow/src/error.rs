use crate::Instance;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("workflow instance {0:?} already exists")]
    Exists(Instance),
    #[error("workflow instance {0:?} does not exist")]
    Missing(Instance),
    #[error("workflow instance {instance:?} expected bundle {expected:?}, got {actual:?}")]
    Bundle {
        instance: Instance,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    #[error(
        "workflow replay mismatch for {instance:?} at sequence {sequence}: journal has {recorded:?}, code called {called:?}"
    )]
    Replay {
        instance: Instance,
        sequence: u64,
        recorded: String,
        called: String,
    },
    #[error("workflow instance {0:?} has a step in flight")]
    Busy(Instance),
    #[error("workflow activity {name:?} for {instance:?} does not exist")]
    ActivityMissing { instance: Instance, name: String },
    #[error("workflow activity {name:?} for {instance:?} is canceled")]
    ActivityCanceled { instance: Instance, name: String },
    #[error("workflow activity {name:?} for {instance:?} has no active lease")]
    ActivityLease { instance: Instance, name: String },
    #[error("workflow journal rejected {instance:?}: {reason}")]
    Journal { instance: Instance, reason: String },
    #[error("workflow timer {name:?} for {instance:?} is invalid: {reason}")]
    Timer {
        instance: Instance,
        name: String,
        reason: String,
    },
    #[error("workflow step {name:?} for {instance:?} could not be decoded: {source}")]
    Json {
        instance: Instance,
        name: String,
        #[source]
        source: serde_json::Error,
    },
}
