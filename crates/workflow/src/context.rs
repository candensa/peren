use crate::{Entry, Instance, Journal, Metadata, WorkflowError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{future::Future, time::Duration};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Sleep {
    pub name: String,
    pub due_at_ms: i64,
    pub delay_ms: u64,
}

#[derive(Debug)]
pub struct Context<'a, J: Journal> {
    journal: &'a J,
    instance: Instance,
    metadata: Metadata,
    entries: Vec<Entry>,
    sequence: u64,
    replayed: usize,
}

impl<'a, J: Journal> Context<'a, J> {
    pub async fn start(
        journal: &'a J,
        instance: Instance,
        bundle: [u8; 32],
        timestamp: i64,
    ) -> Result<Self, WorkflowError> {
        journal.create(&instance, bundle, timestamp).await?;
        Self::resume(journal, instance, bundle).await
    }

    pub async fn resume(
        journal: &'a J,
        instance: Instance,
        bundle: [u8; 32],
    ) -> Result<Self, WorkflowError> {
        let metadata = journal
            .metadata(&instance)
            .await?
            .ok_or_else(|| WorkflowError::Missing(instance.clone()))?;
        if metadata.bundle != bundle {
            return Err(WorkflowError::Bundle {
                instance,
                expected: metadata.bundle,
                actual: bundle,
            });
        }
        let mut entries = journal.entries(&instance, 0).await?;
        entries.sort_by_key(|entry| entry.sequence);
        let replayed = entries.len();
        Ok(Self {
            journal,
            instance,
            metadata,
            entries,
            sequence: 0,
            replayed,
        })
    }

    #[must_use]
    pub fn instance(&self) -> &Instance {
        &self.instance
    }

    #[must_use]
    pub const fn bundle(&self) -> [u8; 32] {
        self.metadata.bundle
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn replayed(&self) -> usize {
        self.replayed
    }

    pub async fn step<T, F, Fut>(&mut self, name: &str, work: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let sequence = self.sequence;
        if let Some(entry) = self.entries.iter().find(|entry| entry.sequence == sequence) {
            if entry.name != name {
                return Err(WorkflowError::Replay {
                    instance: self.instance.clone(),
                    sequence,
                    recorded: entry.name.clone(),
                    called: name.to_string(),
                });
            }
            let value =
                serde_json::from_slice(&entry.payload).map_err(|source| WorkflowError::Json {
                    instance: self.instance.clone(),
                    name: name.to_string(),
                    source,
                })?;
            self.sequence += 1;
            return Ok(value);
        }

        self.journal.begin(&self.instance).await;
        let result = work().await;
        let payload = serde_json::to_vec(&result).map_err(|source| WorkflowError::Json {
            instance: self.instance.clone(),
            name: name.to_string(),
            source,
        });
        let appended = match payload {
            Ok(payload) => {
                let entry = Entry {
                    sequence,
                    name: name.to_string(),
                    payload,
                };
                let saved = self.journal.append(&self.instance, entry.clone()).await;
                self.journal.finish(&self.instance).await;
                saved.map(|()| entry)
            }
            Err(error) => {
                self.journal.finish(&self.instance).await;
                Err(error)
            }
        }?;
        self.entries.push(appended);
        self.sequence += 1;
        Ok(result)
    }

    pub async fn sleep(
        &mut self,
        name: &str,
        delay: Duration,
        now_ms: i64,
    ) -> Result<Sleep, WorkflowError> {
        let delay_ms = u64::try_from(delay.as_millis()).map_err(|_| WorkflowError::Timer {
            instance: self.instance.clone(),
            name: name.to_string(),
            reason: "delay is outside the supported range".into(),
        })?;
        let delay_i64 = i64::try_from(delay_ms).map_err(|_| WorkflowError::Timer {
            instance: self.instance.clone(),
            name: name.to_string(),
            reason: "delay is outside the supported range".into(),
        })?;
        let due_at_ms = now_ms
            .checked_add(delay_i64)
            .ok_or_else(|| WorkflowError::Timer {
                instance: self.instance.clone(),
                name: name.to_string(),
                reason: "due time is outside the supported range".into(),
            })?;
        self.step(name, || async {
            Sleep {
                name: name.to_string(),
                due_at_ms,
                delay_ms,
            }
        })
        .await
    }
}
