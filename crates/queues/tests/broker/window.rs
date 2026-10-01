use super::*;
use chrono::{Duration, Utc};

#[test]
fn batch_window_dispatches_at_size_or_oldest_ready_timeout() {
    let now = Utc::now();
    let window = BatchWindow::new(10, Duration::seconds(5));

    assert!(!window.ready(
        &Stats {
            ready: 0,
            ..Stats::default()
        },
        now,
    ));
    assert!(window.ready(
        &Stats {
            ready: 10,
            oldest_ready_at: Some(now),
            ..Stats::default()
        },
        now,
    ));
    assert!(!window.ready(
        &Stats {
            ready: 3,
            oldest_ready_at: Some(now - Duration::seconds(4)),
            ..Stats::default()
        },
        now,
    ));
    assert!(window.ready(
        &Stats {
            ready: 3,
            oldest_ready_at: Some(now - Duration::seconds(5)),
            ..Stats::default()
        },
        now,
    ));
    assert!(!window.ready(
        &Stats {
            ready: 10,
            paused: true,
            oldest_ready_at: Some(now - Duration::seconds(5)),
            ..Stats::default()
        },
        now,
    ));
}
