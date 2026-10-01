use std::future::Future;

use thiserror::Error;

const ACTIVITY_EFFECT_PREFIX: &str = "workflow-activity:";
const DEFAULT_ACTIVITY_EFFECT_LEASE_MS: i64 = 30_000;
const DEFAULT_ACTIVITY_EFFECT_RETRY_MS: i64 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityEffect {
    pub id: uuid::Uuid,
    pub instance: peren_workflow::Instance,
    pub definition: peren_workflow::ActivityDefinition,
    pub inbox_key: String,
    pub delay: chrono::Duration,
}

impl ActivityEffect {
    #[must_use]
    pub fn new(
        instance: peren_workflow::Instance,
        definition: peren_workflow::ActivityDefinition,
    ) -> Self {
        let id = uuid::Uuid::new_v4();
        let inbox_key = format!("{}:{}", instance.name, definition.name);
        Self {
            id,
            instance,
            definition,
            inbox_key,
            delay: chrono::Duration::zero(),
        }
    }

    #[must_use]
    pub fn inbox_key(mut self, value: impl Into<String>) -> Self {
        self.inbox_key = value.into();
        self
    }

    #[must_use]
    pub fn delay(mut self, value: chrono::Duration) -> Self {
        self.delay = value;
        self
    }

    pub fn draft(self, now_ms: i64) -> Result<peren_storage::EffectDraft, ActivityEffectError> {
        if self.definition.name.trim().is_empty() {
            return Err(ActivityEffectError::EmptyActivity);
        }
        if self.definition.task.trim().is_empty() {
            return Err(ActivityEffectError::EmptyTask);
        }
        if self.inbox_key.trim().is_empty() {
            return Err(ActivityEffectError::EmptyInbox);
        }
        let due_at_ms = now_ms
            .checked_add(self.delay.num_milliseconds())
            .ok_or(ActivityEffectError::InvalidDelay)?;
        Ok(peren_storage::EffectDraft {
            id: self.id,
            destination: activity_destination(&self.instance),
            inbox_key: self.inbox_key,
            payload: serde_json::to_vec(&ActivityEffectPayload {
                instance: self.instance,
                definition: self.definition,
            })?,
            due_at_ms,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct ActivityEffectPayload {
    instance: peren_workflow::Instance,
    definition: peren_workflow::ActivityDefinition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityEffectPublish {
    pub claimed: usize,
    pub retained: usize,
    pub retried: usize,
    pub skipped: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityRun {
    pub instance: peren_workflow::Instance,
    pub definition: peren_workflow::ActivityDefinition,
    pub token: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityRunReport {
    pub state: peren_workflow::ActivityState,
    pub result: serde_json::Value,
}

pub async fn run_activity<J, F, Fut>(
    journal: &J,
    request: ActivityRun,
    now_ms: i64,
    runner: F,
) -> Result<ActivityRunReport, ActivityEffectError>
where
    J: peren_workflow::Journal,
    F: FnOnce(peren_runtime::WorkflowActivityEvent) -> Fut,
    Fut: Future<Output = Result<serde_json::Value, peren_runtime::EngineError>>,
{
    journal
        .lease_activity(
            &request.instance,
            &request.definition.name,
            request.token.clone(),
            now_ms,
        )
        .await?;
    let event = peren_runtime::WorkflowActivityEvent {
        instance: request.instance.name.clone(),
        name: request.definition.name.clone(),
        task: request.definition.task.clone(),
        payload: request.payload,
    };
    let result = runner(event).await?;
    let completed = journal
        .complete_activity(
            &request.instance,
            &request.definition.name,
            &request.token,
            serde_json::to_vec(&result)?,
            now_ms,
        )
        .await?;
    Ok(ActivityRunReport {
        state: completed,
        result,
    })
}

pub async fn publish_activity_effects<J: peren_workflow::Journal>(
    storage: &mut peren_storage::CellStorage,
    journal: &J,
    now_ms: i64,
    limit: usize,
) -> Result<ActivityEffectPublish, ActivityEffectError> {
    publish_activity_effects_with_lease(
        storage,
        journal,
        now_ms,
        DEFAULT_ACTIVITY_EFFECT_LEASE_MS,
        DEFAULT_ACTIVITY_EFFECT_RETRY_MS,
        limit,
        uuid::Uuid::new_v4().to_string(),
    )
    .await
}

pub async fn publish_activity_effects_with_lease<J: peren_workflow::Journal>(
    storage: &mut peren_storage::CellStorage,
    journal: &J,
    now_ms: i64,
    lease_ms: i64,
    retry_ms: i64,
    limit: usize,
    lease_token: String,
) -> Result<ActivityEffectPublish, ActivityEffectError> {
    let lease_until = now_ms
        .checked_add(lease_ms)
        .ok_or(ActivityEffectError::InvalidDelay)?;
    let retry_at = now_ms
        .checked_add(retry_ms)
        .ok_or(ActivityEffectError::InvalidDelay)?;
    let effects = storage.claim_effects(now_ms, &lease_token, lease_until, limit)?;
    let claimed = effects.len();
    let mut report = ActivityEffectPublish {
        claimed,
        retained: 0,
        retried: 0,
        skipped: 0,
    };
    for effect in effects {
        match publish_activity_effect(journal, &effect, now_ms).await {
            Ok(PublishActivity::Retained) => {
                if storage.acknowledge_effect(effect.id, &lease_token)? {
                    report.retained += 1;
                } else {
                    report.skipped += 1;
                }
            }
            Ok(PublishActivity::Ignored) => {
                if storage.acknowledge_effect(effect.id, &lease_token)? {
                    report.skipped += 1;
                }
            }
            Err(error) if error.retryable() => {
                if storage.retry_effect(effect.id, &lease_token, retry_at)? {
                    report.retried += 1;
                } else {
                    report.skipped += 1;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(report)
}

enum PublishActivity {
    Retained,
    Ignored,
}

async fn publish_activity_effect<J: peren_workflow::Journal>(
    journal: &J,
    effect: &peren_storage::EffectRecord,
    now_ms: i64,
) -> Result<PublishActivity, ActivityEffectError> {
    if !effect.destination.starts_with(ACTIVITY_EFFECT_PREFIX) {
        return Ok(PublishActivity::Ignored);
    }
    let payload: ActivityEffectPayload = serde_json::from_slice(&effect.payload)?;
    journal
        .retain_activity(&payload.instance, payload.definition, now_ms)
        .await?;
    Ok(PublishActivity::Retained)
}

fn activity_destination(instance: &peren_workflow::Instance) -> String {
    format!("{ACTIVITY_EFFECT_PREFIX}{}", instance.name)
}

#[derive(Debug, Error)]
pub enum ActivityEffectError {
    #[error("workflow activity effect has an empty activity name")]
    EmptyActivity,
    #[error("workflow activity effect has an empty task")]
    EmptyTask,
    #[error("workflow activity effect has an empty inbox key")]
    EmptyInbox,
    #[error("workflow activity effect delay is invalid")]
    InvalidDelay,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] peren_storage::StorageError),
    #[error(transparent)]
    Workflow(#[from] peren_workflow::WorkflowError),
    #[error(transparent)]
    Runtime(#[from] peren_runtime::EngineError),
}

impl ActivityEffectError {
    const fn retryable(&self) -> bool {
        matches!(self, Self::Workflow(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peren_workflow::Journal;

    fn instance() -> peren_workflow::Instance {
        peren_workflow::Instance::new(peren_primitives::CellId::from_bytes([55; 32]), "deploy-1")
    }

    fn definition(name: &str) -> peren_workflow::ActivityDefinition {
        peren_workflow::ActivityDefinition {
            name: name.into(),
            task: "send-email".into(),
            timeout_ms: 30_000,
            heartbeat_timeout_ms: 5_000,
        }
    }

    #[tokio::test]
    async fn workflow_activity_runner_leases_and_completes_with_worker_result() {
        let journal = peren_workflow::memory::MemoryJournal::new();
        let instance = instance();
        let definition = definition("notify");
        journal
            .retain_activity(&instance, definition.clone(), 100)
            .await
            .unwrap();

        let report = run_activity(
            &journal,
            ActivityRun {
                instance: instance.clone(),
                definition: definition.clone(),
                token: "runner-token".into(),
                payload: serde_json::json!({ "order": 7 }),
            },
            150,
            |event| async move {
                assert_eq!(event.instance, "deploy-1");
                assert_eq!(event.name, "notify");
                assert_eq!(event.task, "send-email");
                Ok(serde_json::json!({ "sent": event.payload["order"] }))
            },
        )
        .await
        .unwrap();

        assert_eq!(
            report.state.status,
            peren_workflow::ActivityStatus::Completed
        );
        assert_eq!(report.result, serde_json::json!({ "sent": 7 }));
        assert_eq!(
            report.state.result,
            Some(serde_json::to_vec(&serde_json::json!({ "sent": 7 })).unwrap())
        );
        assert!(
            journal
                .heartbeat_activity(&instance, "notify", "runner-token", 200)
                .await
                .is_err(),
            "completed activity should clear the active lease"
        );
    }

    #[tokio::test]
    async fn activity_effect_draft_is_deduped_by_instance_and_activity() {
        let mut storage =
            peren_storage::CellStorage::open(std::path::Path::new(":memory:")).unwrap();
        let first = ActivityEffect::new(instance(), definition("notify"))
            .draft(100)
            .unwrap();
        let second = ActivityEffect::new(instance(), definition("notify"))
            .draft(100)
            .unwrap();

        storage
            .transaction_with_effects(
                |transaction| transaction.put("state", b"a", b"1"),
                vec![first],
            )
            .unwrap();
        storage
            .transaction_with_effects(
                |transaction| transaction.put("state", b"b", b"2"),
                vec![second],
            )
            .unwrap();

        let claimed = storage.claim_effects(100, "lease", 200, 10).unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(claimed[0].destination.starts_with("workflow-activity:"));
    }

    #[tokio::test]
    async fn activity_effect_retains_journal_activity_and_acks_effect() {
        let mut storage =
            peren_storage::CellStorage::open(std::path::Path::new(":memory:")).unwrap();
        let instance = instance();
        let effect = ActivityEffect::new(instance.clone(), definition("notify"))
            .draft(100)
            .unwrap();
        let id = effect.id;
        storage
            .transaction_with_effects(
                |transaction| transaction.put("state", b"a", b"1"),
                vec![effect],
            )
            .unwrap();
        let journal = peren_workflow::memory::MemoryJournal::new();

        let report = publish_activity_effects_with_lease(
            &mut storage,
            &journal,
            100,
            1_000,
            100,
            10,
            "lease-a".into(),
        )
        .await
        .unwrap();

        assert_eq!(report.claimed, 1);
        assert_eq!(report.retained, 1);
        assert_eq!(
            storage.effect(id).unwrap().unwrap().status,
            peren_storage::EffectStatus::Acknowledged
        );
        let lease = journal
            .lease_activity(&instance, "notify", "activity-lease".into(), 100)
            .await
            .unwrap();
        assert_eq!(lease.name, "notify");
    }

    #[tokio::test]
    async fn activity_effect_ack_is_fenced_by_storage_lease_token() {
        let mut storage =
            peren_storage::CellStorage::open(std::path::Path::new(":memory:")).unwrap();
        let instance = instance();
        let effect = ActivityEffect::new(instance.clone(), definition("notify"))
            .draft(100)
            .unwrap();
        let id = effect.id;
        storage
            .transaction_with_effects(
                |transaction| transaction.put("state", b"a", b"1"),
                vec![effect],
            )
            .unwrap();
        let first = storage.claim_effects(100, "lease-a", 110, 10).unwrap();
        assert_eq!(first.len(), 1);
        assert!(!storage.acknowledge_effect(id, "stale").unwrap());
        let journal = peren_workflow::memory::MemoryJournal::new();

        let report = publish_activity_effects_with_lease(
            &mut storage,
            &journal,
            111,
            1_000,
            100,
            10,
            "lease-b".into(),
        )
        .await
        .unwrap();

        assert_eq!(report.retained, 1);
        assert_eq!(
            storage.effect(id).unwrap().unwrap().status,
            peren_storage::EffectStatus::Acknowledged
        );
        assert!(
            journal
                .lease_activity(&instance, "notify", "activity".into(), 111)
                .await
                .is_ok()
        );
    }
}
