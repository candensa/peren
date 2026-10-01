use super::*;
use chrono::Utc;

#[test]
fn shard_leasing_isolates_partitioned_messages() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    for tenant in ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"] {
        broker
            .send(
                Send {
                    partition: Some(tenant.into()),
                    ..send("jobs", tenant.as_bytes())
                },
                now,
            )
            .unwrap();
    }

    let left = broker.lease_shard("jobs", 10, now, Shard::new(0, 2).unwrap());
    let right = broker.lease_shard("jobs", 10, now, Shard::new(1, 2).unwrap());

    assert!(!left.is_empty());
    assert!(!right.is_empty());
    assert!(left.iter().all(|lease| lease.generation == 1));
    assert!(
        right
            .iter()
            .all(|lease| lease.message.generation == lease.generation)
    );
    assert_eq!(left.len() + right.len(), 6);
    assert_eq!(broker.queued("jobs"), 0);
}

#[test]
fn poison_messages_dead_letter_without_hiding_later_messages() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"poison"), now).unwrap();
    broker.send(send("jobs", b"healthy"), now).unwrap();
    let leases = broker.lease("jobs", 2, now);

    assert_eq!(leases.len(), 2);
    assert_eq!(
        broker
            .conclude(leases[0].id, Outcome::Fail, &policy(), now)
            .unwrap(),
        Conclusion::DeadLettered
    );
    assert_eq!(
        broker
            .conclude(leases[1].id, Outcome::Ack, &policy(), now)
            .unwrap(),
        Conclusion::Acked
    );
    assert_eq!(broker.queued("jobs"), 0);
    assert_eq!(broker.inspect("dead", 10)[0].body, b"poison");
}

#[test]
fn ack_consumes_the_lease_once() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"work"), now).unwrap();
    let lease = broker.lease("jobs", 1, now).remove(0);

    assert!(matches!(
        broker.conclude(lease.id, Outcome::Ack, &policy(), now),
        Ok(Conclusion::Acked)
    ));
    assert!(matches!(
        broker.conclude(lease.id, Outcome::Ack, &policy(), now),
        Err(QueueError::UnknownLease)
    ));
}
