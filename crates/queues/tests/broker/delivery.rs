use super::*;
use chrono::{Duration, Utc};

#[test]
fn leases_only_ready_messages_in_order() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"a"), now).unwrap();
    broker
        .send(
            Send {
                delay: Duration::seconds(10),
                ..send("jobs", b"b")
            },
            now,
        )
        .unwrap();
    broker.send(send("jobs", b"c"), now).unwrap();

    let leases = broker.lease("jobs", 10, now);

    assert_eq!(leases.len(), 2);
    assert_eq!(leases[0].message.body, b"a");
    assert_eq!(leases[1].message.body, b"c");
    assert_eq!(broker.queued("jobs"), 1);
}

#[test]
fn send_enforces_cloudflare_style_queue_limits() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();

    assert!(matches!(
        broker.send(send("jobs", &vec![0; MAX_MESSAGE_BYTES + 1]), now),
        Err(QueueError::MessageTooLarge { .. })
    ));
    assert!(matches!(
        broker.send(
            Send {
                delay: Duration::seconds(86_401),
                ..send("jobs", b"work")
            },
            now,
        ),
        Err(QueueError::InvalidDelay)
    ));
    assert!(matches!(
        validate_batch(&vec![send("jobs", b"a"); MAX_BATCH_MESSAGES + 1]),
        Err(QueueError::BatchTooLarge { .. })
    ));
    assert!(matches!(
        validate_batch(&[
            send("jobs", &vec![0; MAX_MESSAGE_BYTES]),
            send("jobs", &vec![0; MAX_MESSAGE_BYTES]),
            send("jobs", &vec![0; MAX_MESSAGE_BYTES]),
        ]),
        Err(QueueError::BatchBytesTooLarge { .. })
    ));
}

#[test]
fn retention_sweeps_old_queued_and_leased_messages() {
    let now = Utc::now();
    let old = now - RETENTION - Duration::seconds(1);
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"queued"), old).unwrap();
    assert_eq!(broker.lease("jobs", 1, now).len(), 0);

    broker.send(send("jobs", b"leased"), old).unwrap();
    let lease = broker.lease("jobs", 1, old).remove(0);
    assert!(matches!(
        broker.conclude(lease.id, Outcome::Ack, &policy(), now),
        Err(QueueError::UnknownLease)
    ));
}

#[test]
fn purge_marks_live_leases_and_drops_them_on_settlement_or_expiry() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"leased"), now).unwrap();
    let lease = broker.lease("jobs", 1, now).remove(0);

    let purged = broker.purge("jobs");

    assert_eq!(purged.queued, 0);
    assert_eq!(purged.leased, 1);
    assert_eq!(
        broker
            .conclude(lease.id, Outcome::Ack, &policy(), now)
            .unwrap(),
        Conclusion::Dropped
    );
    assert_eq!(
        broker.lease("jobs", 1, now + Duration::seconds(31)).len(),
        0
    );
}

#[test]
fn expired_leases_are_redelivered_and_stale_settlement_is_fenced() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"work"), now).unwrap();
    let stale = broker.lease("jobs", 1, now).remove(0);

    let redelivered = broker
        .lease("jobs", 1, now + chrono::Duration::seconds(31))
        .remove(0);

    assert_eq!(stale.generation, 1);
    assert_eq!(redelivered.message.id, stale.message.id);
    assert_eq!(redelivered.generation, stale.generation + 1);
    assert_eq!(redelivered.message.generation, redelivered.generation);
    assert_ne!(redelivered.id, stale.id);
    assert!(matches!(
        broker.conclude(
            stale.id,
            Outcome::Ack,
            &policy(),
            now + chrono::Duration::seconds(31)
        ),
        Err(QueueError::UnknownLease)
    ));
    assert_eq!(
        broker
            .conclude(
                redelivered.id,
                Outcome::Ack,
                &policy(),
                now + chrono::Duration::seconds(31)
            )
            .unwrap(),
        Conclusion::Acked
    );
}

#[test]
fn retry_requeues_until_dead_letter_policy_is_reached() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"work"), now).unwrap();
    let first = broker.lease("jobs", 1, now).remove(0);

    let retried = broker
        .conclude(first.id, Outcome::Retry, &policy(), now)
        .unwrap();
    assert_eq!(retried, Conclusion::Retried);
    assert_eq!(broker.queued("jobs"), 1);

    let second = broker
        .lease("jobs", 1, now + Duration::seconds(5))
        .remove(0);
    let dead = broker
        .conclude(second.id, Outcome::Retry, &policy(), now)
        .unwrap();

    assert_eq!(dead, Conclusion::DeadLettered);
    assert_eq!(broker.queued("jobs"), 0);
    assert_eq!(broker.queued("dead"), 1);
    let dead = broker.inspect("dead", 10);
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].body, b"work");
    assert_eq!(dead[0].attempts, 1);
}

#[test]
fn stats_split_ready_delayed_and_leased_messages() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"ready"), now).unwrap();
    broker
        .send(
            Send {
                delay: Duration::seconds(10),
                ..send("jobs", b"later")
            },
            now,
        )
        .unwrap();
    let _lease = broker.lease("jobs", 1, now);

    assert_eq!(
        broker.stats("jobs", now),
        Stats {
            ready: 0,
            delayed: 1,
            leased: 1,
            paused: false,
            oldest_ready_at: None,
        }
    );
}
