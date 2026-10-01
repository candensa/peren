use std::time::{SystemTime, UNIX_EPOCH};

use super::DeployError;

pub(super) fn now_ms() -> Result<i64, DeployError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DeployError::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| DeployError::Time)
}
