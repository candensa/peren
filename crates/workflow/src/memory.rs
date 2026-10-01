use crate::{
    ActivityDefinition, ActivityLease, ActivityState, ActivityStatus, Cutover, Entry, Instance,
    Journal, Metadata, WorkflowError,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
struct Record {
    metadata: Option<Metadata>,
    entries: Vec<Entry>,
    cutovers: Vec<Cutover>,
    activities: HashMap<String, ActivityState>,
    busy: Arc<AtomicBool>,
}

#[derive(Default)]
pub struct MemoryJournal {
    records: Mutex<HashMap<Instance, Record>>,
}

impl MemoryJournal {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn cutovers(&self, instance: &Instance) -> Vec<Cutover> {
        self.records
            .lock()
            .expect("workflow memory journal lock poisoned")
            .get(instance)
            .map(|record| record.cutovers.clone())
            .unwrap_or_default()
    }
}

impl Journal for MemoryJournal {
    async fn create(
        &self,
        instance: &Instance,
        bundle: [u8; 32],
        timestamp: i64,
    ) -> Result<(), WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records.entry(instance.clone()).or_default();
        if record.metadata.is_some() {
            return Err(WorkflowError::Exists(instance.clone()));
        }
        record.metadata = Some(Metadata {
            bundle,
            created: timestamp,
        });
        Ok(())
    }

    async fn metadata(&self, instance: &Instance) -> Result<Option<Metadata>, WorkflowError> {
        Ok(self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned")
            .get(instance)
            .and_then(|record| record.metadata.clone()))
    }

    async fn entries(&self, instance: &Instance, from: u64) -> Result<Vec<Entry>, WorkflowError> {
        Ok(self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned")
            .get(instance)
            .map(|record| {
                record
                    .entries
                    .iter()
                    .filter(|entry| entry.sequence >= from)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn append(&self, instance: &Instance, entry: Entry) -> Result<(), WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records.entry(instance.clone()).or_default();
        if let Some(existing) = record
            .entries
            .iter()
            .find(|existing| existing.sequence == entry.sequence)
        {
            if existing.name != entry.name {
                return Err(WorkflowError::Replay {
                    instance: instance.clone(),
                    sequence: entry.sequence,
                    recorded: existing.name.clone(),
                    called: entry.name,
                });
            }
            return Ok(());
        }
        if entry.sequence != record.entries.len() as u64 {
            return Err(WorkflowError::Journal {
                instance: instance.clone(),
                reason: format!(
                    "sequence {} leaves a gap after {} entries",
                    entry.sequence,
                    record.entries.len()
                ),
            });
        }
        record.entries.push(entry);
        Ok(())
    }

    async fn retain_activity(
        &self,
        instance: &Instance,
        definition: ActivityDefinition,
        timestamp: i64,
    ) -> Result<ActivityState, WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records.entry(instance.clone()).or_default();
        let state = record
            .activities
            .entry(definition.name.clone())
            .and_modify(|state| {
                if state.status == ActivityStatus::Pending {
                    state.definition = definition.clone();
                    state.updated_at_ms = timestamp;
                }
            })
            .or_insert_with(|| ActivityState {
                definition,
                status: ActivityStatus::Pending,
                lease: None,
                result: None,
                cancel_reason: None,
                updated_at_ms: timestamp,
            })
            .clone();
        Ok(state)
    }

    async fn lease_activity(
        &self,
        instance: &Instance,
        name: &str,
        token: String,
        now_ms: i64,
    ) -> Result<ActivityLease, WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records
            .get_mut(instance)
            .ok_or_else(|| WorkflowError::Missing(instance.clone()))?;
        let state =
            record
                .activities
                .get_mut(name)
                .ok_or_else(|| WorkflowError::ActivityMissing {
                    instance: instance.clone(),
                    name: name.to_string(),
                })?;
        if state.status == ActivityStatus::Canceled {
            return Err(WorkflowError::ActivityCanceled {
                instance: instance.clone(),
                name: name.to_string(),
            });
        }
        if state.status == ActivityStatus::Completed {
            return Err(WorkflowError::Journal {
                instance: instance.clone(),
                reason: format!("activity {name:?} is already completed"),
            });
        }
        if let Some(lease) = &state.lease
            && lease.leased_until_ms > now_ms
        {
            return Ok(lease.clone());
        }
        let timeout =
            i64::try_from(state.definition.timeout_ms).map_err(|_| WorkflowError::Journal {
                instance: instance.clone(),
                reason: format!("activity {name:?} timeout is outside range"),
            })?;
        let heartbeat = i64::try_from(state.definition.heartbeat_timeout_ms).map_err(|_| {
            WorkflowError::Journal {
                instance: instance.clone(),
                reason: format!("activity {name:?} heartbeat timeout is outside range"),
            }
        })?;
        let lease = ActivityLease {
            name: name.to_string(),
            token,
            leased_until_ms: now_ms
                .checked_add(timeout)
                .ok_or_else(|| WorkflowError::Journal {
                    instance: instance.clone(),
                    reason: format!("activity {name:?} lease deadline is outside range"),
                })?,
            heartbeat_deadline_ms: now_ms.checked_add(heartbeat).ok_or_else(|| {
                WorkflowError::Journal {
                    instance: instance.clone(),
                    reason: format!("activity {name:?} heartbeat deadline is outside range"),
                }
            })?,
        };
        state.status = ActivityStatus::Leased;
        state.lease = Some(lease.clone());
        state.updated_at_ms = now_ms;
        Ok(lease)
    }

    async fn heartbeat_activity(
        &self,
        instance: &Instance,
        name: &str,
        token: &str,
        now_ms: i64,
    ) -> Result<ActivityLease, WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records
            .get_mut(instance)
            .ok_or_else(|| WorkflowError::Missing(instance.clone()))?;
        let state =
            record
                .activities
                .get_mut(name)
                .ok_or_else(|| WorkflowError::ActivityMissing {
                    instance: instance.clone(),
                    name: name.to_string(),
                })?;
        if state.status == ActivityStatus::Canceled {
            return Err(WorkflowError::ActivityCanceled {
                instance: instance.clone(),
                name: name.to_string(),
            });
        }
        let lease = state
            .lease
            .as_mut()
            .filter(|lease| lease.token == token)
            .ok_or_else(|| WorkflowError::ActivityLease {
                instance: instance.clone(),
                name: name.to_string(),
            })?;
        let heartbeat = i64::try_from(state.definition.heartbeat_timeout_ms).map_err(|_| {
            WorkflowError::Journal {
                instance: instance.clone(),
                reason: format!("activity {name:?} heartbeat timeout is outside range"),
            }
        })?;
        lease.heartbeat_deadline_ms =
            now_ms
                .checked_add(heartbeat)
                .ok_or_else(|| WorkflowError::Journal {
                    instance: instance.clone(),
                    reason: format!("activity {name:?} heartbeat deadline is outside range"),
                })?;
        state.updated_at_ms = now_ms;
        Ok(lease.clone())
    }

    async fn complete_activity(
        &self,
        instance: &Instance,
        name: &str,
        token: &str,
        result: Vec<u8>,
        timestamp: i64,
    ) -> Result<ActivityState, WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records
            .get_mut(instance)
            .ok_or_else(|| WorkflowError::Missing(instance.clone()))?;
        let state =
            record
                .activities
                .get_mut(name)
                .ok_or_else(|| WorkflowError::ActivityMissing {
                    instance: instance.clone(),
                    name: name.to_string(),
                })?;
        if state.status == ActivityStatus::Canceled {
            return Err(WorkflowError::ActivityCanceled {
                instance: instance.clone(),
                name: name.to_string(),
            });
        }
        state
            .lease
            .as_ref()
            .filter(|lease| lease.token == token)
            .ok_or_else(|| WorkflowError::ActivityLease {
                instance: instance.clone(),
                name: name.to_string(),
            })?;
        state.status = ActivityStatus::Completed;
        state.result = Some(result);
        state.lease = None;
        state.updated_at_ms = timestamp;
        Ok(state.clone())
    }

    async fn cancel_activity(
        &self,
        instance: &Instance,
        name: &str,
        reason: Option<String>,
        timestamp: i64,
    ) -> Result<ActivityState, WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records
            .get_mut(instance)
            .ok_or_else(|| WorkflowError::Missing(instance.clone()))?;
        let state =
            record
                .activities
                .get_mut(name)
                .ok_or_else(|| WorkflowError::ActivityMissing {
                    instance: instance.clone(),
                    name: name.to_string(),
                })?;
        state.status = ActivityStatus::Canceled;
        state.cancel_reason = reason;
        state.lease = None;
        state.updated_at_ms = timestamp;
        Ok(state.clone())
    }

    async fn cutover(
        &self,
        instance: &Instance,
        bundle: [u8; 32],
        operator: &str,
        attested: bool,
        timestamp: i64,
    ) -> Result<Cutover, WorkflowError> {
        let mut records = self
            .records
            .lock()
            .expect("workflow memory journal lock poisoned");
        let record = records.entry(instance.clone()).or_default();
        if record.busy.load(Ordering::SeqCst) {
            return Err(WorkflowError::Busy(instance.clone()));
        }
        let metadata = record
            .metadata
            .as_mut()
            .ok_or_else(|| WorkflowError::Missing(instance.clone()))?;
        let cutover = Cutover {
            instance: instance.clone(),
            from: metadata.bundle,
            to: bundle,
            operator: operator.to_string(),
            attested,
            timestamp,
        };
        metadata.bundle = bundle;
        record.cutovers.push(cutover.clone());
        Ok(cutover)
    }

    async fn begin(&self, instance: &Instance) {
        let busy = {
            let mut records = self
                .records
                .lock()
                .expect("workflow memory journal lock poisoned");
            records.entry(instance.clone()).or_default().busy.clone()
        };
        busy.store(true, Ordering::SeqCst);
    }

    async fn finish(&self, instance: &Instance) {
        let busy = {
            let mut records = self
                .records
                .lock()
                .expect("workflow memory journal lock poisoned");
            records.entry(instance.clone()).or_default().busy.clone()
        };
        busy.store(false, Ordering::SeqCst);
    }
}
