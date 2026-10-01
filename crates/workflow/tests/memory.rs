use peren_primitives::CellId;
use std::sync::atomic::{AtomicU32, Ordering};

use peren_workflow::{
    ActivityDefinition, ActivityStatus, Context, Instance, Journal, WorkflowError,
    memory::MemoryJournal,
};

const BUNDLE: [u8; 32] = [7; 32];

fn instance() -> Instance {
    Instance::new(CellId::from_bytes([3; 32]), "order-1")
}

#[tokio::test]
async fn replay_returns_journaled_steps_without_rerunning_work() {
    let journal = MemoryJournal::new();
    let instance = instance();
    let calls = AtomicU32::new(0);
    {
        let mut context = Context::start(&journal, instance.clone(), BUNDLE, 1)
            .await
            .unwrap();
        let first: String = context
            .step("charge", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                "charged".to_string()
            })
            .await
            .unwrap();
        assert_eq!(first, "charged");
    }

    let mut context = Context::resume(&journal, instance, BUNDLE).await.unwrap();
    let second: String = context
        .step("charge", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            "wrong".to_string()
        })
        .await
        .unwrap();

    assert_eq!(second, "charged");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(context.replayed(), 1);
}

#[tokio::test]
async fn sleep_records_timer_and_replays_without_new_due_time() {
    let journal = MemoryJournal::new();
    let instance = instance();
    {
        let mut context = Context::start(&journal, instance.clone(), BUNDLE, 1)
            .await
            .unwrap();
        let sleep = context
            .sleep("wait", std::time::Duration::from_millis(250), 1_000)
            .await
            .unwrap();
        assert_eq!(sleep.due_at_ms, 1_250);
        assert_eq!(sleep.delay_ms, 250);
    }

    let mut context = Context::resume(&journal, instance, BUNDLE).await.unwrap();
    let replayed = context
        .sleep("wait", std::time::Duration::from_millis(999), 5_000)
        .await
        .unwrap();

    assert_eq!(replayed.due_at_ms, 1_250);
    assert_eq!(replayed.delay_ms, 250);
    assert_eq!(context.replayed(), 1);
}

#[tokio::test]
async fn replay_rejects_changed_step_order() {
    let journal = MemoryJournal::new();
    let instance = instance();
    let mut context = Context::start(&journal, instance.clone(), BUNDLE, 1)
        .await
        .unwrap();
    let _: String = context
        .step("first", || async { "ok".to_string() })
        .await
        .unwrap();

    let mut context = Context::resume(&journal, instance, BUNDLE).await.unwrap();
    let error = context
        .step::<String, _, _>("renamed", || async { "bad".to_string() })
        .await
        .unwrap_err();

    assert!(matches!(error, WorkflowError::Replay { .. }));
}

#[tokio::test]
async fn activities_retain_definition_lease_heartbeat_and_complete() {
    let journal = MemoryJournal::new();
    let instance = instance();
    Context::start(&journal, instance.clone(), BUNDLE, 1)
        .await
        .unwrap();
    let definition = ActivityDefinition {
        name: "ship".into(),
        task: "fulfillment.ship".into(),
        timeout_ms: 1_000,
        heartbeat_timeout_ms: 100,
    };

    let retained = journal
        .retain_activity(&instance, definition.clone(), 10)
        .await
        .unwrap();
    let lease = journal
        .lease_activity(&instance, "ship", "token-a".into(), 20)
        .await
        .unwrap();
    let heartbeat = journal
        .heartbeat_activity(&instance, "ship", "token-a", 40)
        .await
        .unwrap();
    let completed = journal
        .complete_activity(&instance, "ship", "token-a", b"done".to_vec(), 60)
        .await
        .unwrap();

    assert_eq!(retained.definition, definition);
    assert_eq!(lease.leased_until_ms, 1_020);
    assert_eq!(lease.heartbeat_deadline_ms, 120);
    assert_eq!(heartbeat.heartbeat_deadline_ms, 140);
    assert_eq!(completed.status, ActivityStatus::Completed);
    assert_eq!(completed.result.as_deref(), Some(&b"done"[..]));
    assert!(completed.lease.is_none());
}

#[tokio::test]
async fn activity_leases_fence_tokens_and_allow_takeover_after_expiry() {
    let journal = MemoryJournal::new();
    let instance = instance();
    Context::start(&journal, instance.clone(), BUNDLE, 1)
        .await
        .unwrap();
    journal
        .retain_activity(
            &instance,
            ActivityDefinition {
                name: "charge".into(),
                task: "billing.charge".into(),
                timeout_ms: 50,
                heartbeat_timeout_ms: 20,
            },
            10,
        )
        .await
        .unwrap();
    let first = journal
        .lease_activity(&instance, "charge", "token-a".into(), 20)
        .await
        .unwrap();
    let still_first = journal
        .lease_activity(&instance, "charge", "token-b".into(), 30)
        .await
        .unwrap();
    let second = journal
        .lease_activity(&instance, "charge", "token-b".into(), 80)
        .await
        .unwrap();

    assert_eq!(first.token, "token-a");
    assert_eq!(still_first.token, "token-a");
    assert_eq!(second.token, "token-b");
    assert!(matches!(
        journal
            .complete_activity(&instance, "charge", "token-a", Vec::new(), 90)
            .await,
        Err(WorkflowError::ActivityLease { .. })
    ));
}

#[tokio::test]
async fn activity_cancel_removes_lease_and_refuses_completion() {
    let journal = MemoryJournal::new();
    let instance = instance();
    Context::start(&journal, instance.clone(), BUNDLE, 1)
        .await
        .unwrap();
    journal
        .retain_activity(
            &instance,
            ActivityDefinition {
                name: "email".into(),
                task: "notify.email".into(),
                timeout_ms: 500,
                heartbeat_timeout_ms: 50,
            },
            10,
        )
        .await
        .unwrap();
    journal
        .lease_activity(&instance, "email", "token".into(), 20)
        .await
        .unwrap();
    let canceled = journal
        .cancel_activity(&instance, "email", Some("operator".into()), 30)
        .await
        .unwrap();

    assert_eq!(canceled.status, ActivityStatus::Canceled);
    assert_eq!(canceled.cancel_reason.as_deref(), Some("operator"));
    assert!(canceled.lease.is_none());
    assert!(matches!(
        journal
            .complete_activity(&instance, "email", "token", Vec::new(), 40)
            .await,
        Err(WorkflowError::ActivityCanceled { .. })
    ));
}

#[tokio::test]
async fn cutover_is_audited_and_refused_during_a_step() {
    let journal = MemoryJournal::new();
    let instance = instance();
    journal.create(&instance, BUNDLE, 1).await.unwrap();
    journal.begin(&instance).await;

    assert!(matches!(
        journal
            .cutover(&instance, [8; 32], "operator", true, 2)
            .await,
        Err(WorkflowError::Busy(_))
    ));

    journal.finish(&instance).await;
    let cutover = journal
        .cutover(&instance, [8; 32], "operator", true, 3)
        .await
        .unwrap();

    assert_eq!(cutover.from, BUNDLE);
    assert_eq!(journal.cutovers(&instance).len(), 1);
    assert!(matches!(
        Context::resume(&journal, instance.clone(), BUNDLE).await,
        Err(WorkflowError::Bundle { .. })
    ));
    assert!(Context::resume(&journal, instance, [8; 32]).await.is_ok());
}
