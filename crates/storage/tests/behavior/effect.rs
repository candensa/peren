use super::*;
use std::path::Path;

#[test]
fn effects_commit_atomically_with_state_and_are_claimed_by_due_time() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    let effect_id = uuid::Uuid::new_v4();

    let committed = storage
        .transaction_with_effects(
            |tx| tx.put("do", b"state", b"ready"),
            vec![EffectDraft {
                id: effect_id,
                destination: "queue:jobs".into(),
                inbox_key: "job-1".into(),
                payload: b"payload".to_vec(),
                due_at_ms: 100,
            }],
        )
        .unwrap();

    assert_eq!(
        storage.get("do", b"state").unwrap(),
        Some(b"ready".to_vec())
    );
    assert_eq!(
        committed.revision,
        storage.effect(effect_id).unwrap().unwrap().revision
    );
    assert!(
        storage
            .claim_effects(99, "lease-a", 150, 10)
            .unwrap()
            .is_empty()
    );
    let claimed = storage.claim_effects(100, "lease-a", 150, 10).unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].status, EffectStatus::Leased);
    assert_eq!(claimed[0].attempts, 1);
    assert_eq!(claimed[0].lease_token.as_deref(), Some("lease-a"));
}

#[test]
fn effects_are_deduped_by_destination_inbox_key() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    let first = uuid::Uuid::new_v4();
    let second = uuid::Uuid::new_v4();

    storage
        .transaction_with_effects(
            |_| Ok(()),
            vec![EffectDraft {
                id: first,
                destination: "service:mail".into(),
                inbox_key: "welcome:user-1".into(),
                payload: b"one".to_vec(),
                due_at_ms: 0,
            }],
        )
        .unwrap();
    storage
        .transaction_with_effects(
            |_| Ok(()),
            vec![EffectDraft {
                id: second,
                destination: "service:mail".into(),
                inbox_key: "welcome:user-1".into(),
                payload: b"two".to_vec(),
                due_at_ms: 0,
            }],
        )
        .unwrap();

    assert!(storage.effect(first).unwrap().is_some());
    assert!(storage.effect(second).unwrap().is_none());
}

#[test]
fn effect_ack_retry_and_reclaim_are_lease_token_fenced() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    let effect_id = uuid::Uuid::new_v4();
    storage
        .transaction_with_effects(
            |_| Ok(()),
            vec![EffectDraft {
                id: effect_id,
                destination: "queue:jobs".into(),
                inbox_key: "job-2".into(),
                payload: b"payload".to_vec(),
                due_at_ms: 0,
            }],
        )
        .unwrap();

    let first = storage.claim_effects(0, "lease-a", 10, 10).unwrap();
    assert_eq!(first[0].id, effect_id);
    assert!(!storage.acknowledge_effect(effect_id, "wrong").unwrap());
    assert!(!storage.retry_effect(effect_id, "wrong", 20).unwrap());
    assert!(storage.retry_effect(effect_id, "lease-a", 20).unwrap());
    assert!(
        storage
            .claim_effects(19, "lease-b", 30, 10)
            .unwrap()
            .is_empty()
    );
    let second = storage.claim_effects(20, "lease-b", 30, 10).unwrap();
    assert_eq!(second[0].attempts, 2);
    assert_eq!(second[0].lease_token.as_deref(), Some("lease-b"));
    assert!(storage.acknowledge_effect(effect_id, "lease-b").unwrap());
    assert!(
        storage
            .claim_effects(100, "lease-c", 110, 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage.effect(effect_id).unwrap().unwrap().status,
        EffectStatus::Acknowledged
    );
}
