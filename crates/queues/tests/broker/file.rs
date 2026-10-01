use super::*;
use chrono::{Duration, Utc};

#[test]
fn producer_deduplication_returns_existing_message_id_and_rejects_conflicts() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    let command = Send {
        dedup_id: Some("deploy-1".into()),
        ..send("jobs", b"work")
    };

    let first = broker.send(command.clone(), now).unwrap();
    let second = broker.send(command, now + Duration::seconds(1)).unwrap();
    let conflict = broker.send(
        Send {
            dedup_id: Some("deploy-1".into()),
            ..send("jobs", b"different")
        },
        now,
    );

    assert_eq!(first, second);
    assert!(matches!(conflict, Err(QueueError::DedupConflict)));
    assert_eq!(broker.queued("jobs"), 1);
}

#[test]
fn file_broker_persists_producer_deduplication() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("broker.json");
    let now = Utc::now();
    let command = Send {
        dedup_id: Some("once".into()),
        ..send("jobs", b"work")
    };
    let first = {
        let mut broker = FileBroker::open(&path).unwrap();
        broker.send(command.clone(), now).unwrap()
    };
    let second = {
        let mut broker = FileBroker::open(&path).unwrap();
        broker.send(command, now + Duration::seconds(1)).unwrap()
    };

    assert_eq!(first, second);
    assert_eq!(FileBroker::open(&path).unwrap().queued("jobs"), 1);
}

#[test]
fn file_broker_honors_explicit_retry_delay() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("broker.json");
    let now = Utc::now();
    let lease = {
        let mut broker = FileBroker::open(&path).unwrap();
        broker.send(send("jobs", b"work"), now).unwrap();
        broker.lease("jobs", 1, now).unwrap().remove(0)
    };

    {
        let mut broker = FileBroker::open(&path).unwrap();
        assert_eq!(
            broker
                .conclude_with_delay(
                    lease.id,
                    Outcome::Retry,
                    &policy(),
                    now,
                    Some(Duration::seconds(30)),
                )
                .unwrap(),
            Conclusion::Retried
        );
    }

    let mut broker = FileBroker::open(&path).unwrap();
    assert!(
        broker
            .lease("jobs", 1, now + Duration::seconds(29))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        broker
            .lease("jobs", 1, now + Duration::seconds(30))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn file_broker_persists_depth_and_dead_letters() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("broker.json");
    let now = Utc::now();
    let lease = {
        let mut broker = FileBroker::open(&path).unwrap();
        broker.send(send("jobs", b"work"), now).unwrap();
        assert_eq!(broker.queued("jobs"), 1);
        broker.lease("jobs", 1, now).unwrap().remove(0)
    };

    {
        let mut broker = FileBroker::open(&path).unwrap();
        assert_eq!(broker.queued("jobs"), 0);
        assert_eq!(broker.inspect("jobs", 10), Vec::new());
        assert_eq!(
            broker
                .conclude(lease.id, Outcome::Fail, &policy(), now)
                .unwrap(),
            Conclusion::DeadLettered
        );
    }

    let broker = FileBroker::open(&path).unwrap();
    assert_eq!(broker.queued("dead"), 1);
    assert_eq!(broker.inspect("dead", 1)[0].body, b"work");
}
