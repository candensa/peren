use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IsolateLimits {
    heap_bytes: usize,
    execution_time: Duration,
}

impl IsolateLimits {
    #[must_use]
    pub const fn new(heap_bytes: usize, execution_time: Duration) -> Self {
        Self {
            heap_bytes,
            execution_time,
        }
    }

    pub(crate) const fn heap_bytes(self) -> usize {
        self.heap_bytes
    }

    pub(crate) const fn execution_time(self) -> Duration {
        self.execution_time
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvocationLimits {
    body_bytes: u64,
    subrequests: u32,
    cpu_time: Duration,
}

impl InvocationLimits {
    #[must_use]
    pub const fn new(body_bytes: u64, subrequests: u32) -> Self {
        Self {
            body_bytes,
            subrequests,
            cpu_time: Duration::MAX,
        }
    }

    #[must_use]
    pub const fn with_cpu_time(mut self, cpu_time: Duration) -> Self {
        self.cpu_time = cpu_time;
        self
    }

    pub(crate) fn validate_request(self, used: u64) -> Result<(), LimitError> {
        if used > self.body_bytes {
            return Err(LimitError::RequestBytes {
                used,
                limit: self.body_bytes,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_response(self, used: u64) -> Result<(), LimitError> {
        if used > self.body_bytes {
            return Err(LimitError::ResponseBytes {
                used,
                limit: self.body_bytes,
            });
        }
        Ok(())
    }

    pub fn validate_cpu_time(self, used: Duration) -> Result<(), LimitError> {
        if used > self.cpu_time {
            return Err(LimitError::CpuTime {
                used,
                limit: self.cpu_time,
            });
        }
        Ok(())
    }

    pub(crate) const fn cpu_time(self) -> Duration {
        self.cpu_time
    }
}

#[derive(Debug)]
pub struct InvocationBudget {
    limits: InvocationLimits,
    subrequests: u32,
}

impl InvocationBudget {
    pub fn begin(limits: InvocationLimits, request_bytes: u64) -> Result<Self, LimitError> {
        limits.validate_request(request_bytes)?;
        Ok(Self {
            limits,
            subrequests: 0,
        })
    }

    pub fn charge_subrequest(&mut self) -> Result<(), LimitError> {
        let used = self.subrequests.saturating_add(1);
        if used > self.limits.subrequests {
            return Err(LimitError::Subrequests {
                used,
                limit: self.limits.subrequests,
            });
        }
        self.subrequests = used;
        Ok(())
    }

    #[must_use]
    pub const fn subrequests(&self) -> u32 {
        self.subrequests
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LimitError {
    #[error("request used {used} bytes; the limit is {limit}")]
    RequestBytes { used: u64, limit: u64 },
    #[error("response used {used} bytes; the limit is {limit}")]
    ResponseBytes { used: u64, limit: u64 },
    #[error("invocation used {used:?} CPU time; the limit is {limit:?}")]
    CpuTime { used: Duration, limit: Duration },
    #[error("invocation used {used} subrequests; the limit is {limit}")]
    Subrequests { used: u32, limit: u32 },
}
