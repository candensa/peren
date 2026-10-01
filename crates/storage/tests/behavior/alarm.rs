use super::*;
use std::path::Path;

#[test]
fn stale_alarm_retry_does_not_touch_new_generation() {
    let mut storage = CellStorage::open(Path::new(":memory:")).unwrap();
    storage.set_alarm("do", 10).unwrap();
    let old = storage.alarm("do").unwrap().unwrap().generation;
    storage.set_alarm("do", 20).unwrap();
    let revision = storage.revision();
    assert!(!storage.retry_alarm("do", old).unwrap().value);
    assert_eq!(storage.revision(), revision);
    assert_eq!(storage.alarm("do").unwrap().unwrap().retry, 0);
}
