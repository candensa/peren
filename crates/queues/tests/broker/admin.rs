use super::*;
use chrono::Utc;

#[test]
fn pause_stops_leasing_until_resume() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"work"), now).unwrap();

    assert!(broker.pause("jobs").unwrap());
    assert_eq!(
        broker.stats("jobs", now),
        Stats {
            ready: 1,
            delayed: 0,
            leased: 0,
            paused: true,
            oldest_ready_at: Some(now),
        }
    );
    assert!(broker.lease("jobs", 1, now).is_empty());
    assert!(broker.resume("jobs").unwrap());
    assert_eq!(broker.lease("jobs", 1, now).len(), 1);
}

#[test]
fn purge_removes_queued_messages_and_marks_live_leases() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"queued"), now).unwrap();
    broker.send(send("jobs", b"leased"), now).unwrap();
    let _lease = broker.lease("jobs", 1, now);

    assert_eq!(
        broker.purge("jobs"),
        Purge {
            queued: 1,
            leased: 1
        }
    );
    assert_eq!(
        broker.stats("jobs", now),
        Stats {
            ready: 0,
            delayed: 0,
            leased: 1,
            paused: false,
            oldest_ready_at: None,
        }
    );
}

#[test]
fn redrive_moves_dead_letter_messages_to_target_queue() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("dead", b"work"), now).unwrap();

    assert_eq!(broker.redrive("dead", "jobs", now).unwrap(), 1);
    assert_eq!(broker.queued("dead"), 0);
    assert_eq!(broker.queued("jobs"), 1);
    assert_eq!(broker.inspect("jobs", 1)[0].queue, "jobs");
}

#[test]
fn inspect_limits_visible_messages_without_leasing() {
    let now = Utc::now();
    let mut broker = MemoryBroker::new();
    broker.send(send("jobs", b"a"), now).unwrap();
    broker.send(send("jobs", b"b"), now).unwrap();

    let visible = broker.inspect("jobs", 1);

    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].body, b"a");
    assert_eq!(broker.queued("jobs"), 2);
}
