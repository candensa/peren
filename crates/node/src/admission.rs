use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub(crate) struct Admission {
    capacity: u64,
    permits: Arc<Semaphore>,
    admitted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    refused: Arc<AtomicU64>,
    mode: Arc<AtomicU8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mode {
    Serving,
    Draining,
    ControlOnly,
}

impl Mode {
    fn as_u8(self) -> u8 {
        match self {
            Self::Serving => 0,
            Self::Draining => 1,
            Self::ControlOnly => 2,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Draining,
            2 => Self::ControlOnly,
            _ => Self::Serving,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Permit {
    admission: Admission,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Snapshot {
    pub mode: Mode,
    pub capacity: u64,
    pub available: u64,
    pub active: u64,
    pub admitted: u64,
    pub completed: u64,
    pub refused: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Refused;

impl Admission {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity: u64::try_from(capacity).unwrap_or(u64::MAX),
            permits: Arc::new(Semaphore::new(capacity)),
            admitted: Arc::new(AtomicU64::new(0)),
            completed: Arc::new(AtomicU64::new(0)),
            refused: Arc::new(AtomicU64::new(0)),
            mode: Arc::new(AtomicU8::new(Mode::Serving.as_u8())),
        }
    }

    pub(crate) fn admit(&self) -> Result<Permit, Refused> {
        if self.mode() != Mode::Serving {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Err(Refused);
        }
        if let Ok(permit) = self.permits.clone().try_acquire_owned() {
            self.admitted.fetch_add(1, Ordering::Relaxed);
            Ok(Permit {
                admission: self.clone(),
                _permit: permit,
            })
        } else {
            self.refused.fetch_add(1, Ordering::Relaxed);
            Err(Refused)
        }
    }

    pub(crate) fn set_mode(&self, mode: Mode) {
        self.mode.store(mode.as_u8(), Ordering::Release);
    }

    pub(crate) fn mode(&self) -> Mode {
        Mode::from_u8(self.mode.load(Ordering::Acquire))
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        let available = u64::try_from(self.permits.available_permits()).unwrap_or(u64::MAX);
        Snapshot {
            mode: self.mode(),
            capacity: self.capacity,
            available,
            active: self.capacity.saturating_sub(available),
            admitted: self.admitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.admission.completed.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::Admission;

    #[test]
    fn refuses_when_capacity_is_exhausted_and_recovers_on_drop() {
        let admission = Admission::new(1);
        let permit = admission.admit().expect("first unit is admitted");
        assert!(admission.admit().is_err());
        let full = admission.snapshot();
        assert_eq!(full.mode, super::Mode::Serving);
        assert_eq!(full.capacity, 1);
        assert_eq!(full.active, 1);
        assert_eq!(full.admitted, 1);
        assert_eq!(full.refused, 1);

        drop(permit);
        let recovered = admission.snapshot();
        assert_eq!(recovered.active, 0);
        assert_eq!(recovered.completed, 1);
        assert!(admission.admit().is_ok());
    }

    #[test]
    fn draining_refuses_new_work_without_losing_capacity_state() {
        let admission = Admission::new(2);
        admission.set_mode(super::Mode::Draining);

        assert!(admission.admit().is_err());
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.mode, super::Mode::Draining);
        assert_eq!(snapshot.capacity, 2);
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.refused, 1);
    }
}
