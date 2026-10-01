use peren_storage::*;
use std::path::PathBuf;

fn database() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cell.sqlite");
    (dir, path)
}

#[path = "behavior/alarm.rs"]
mod alarm;
#[path = "behavior/attachment.rs"]
mod attachment;
#[path = "behavior/effect.rs"]
mod effect;
#[path = "behavior/kv.rs"]
mod kv;
#[path = "behavior/mutation.rs"]
mod mutation;
#[path = "behavior/replica.rs"]
mod replica;
#[path = "behavior/sql.rs"]
mod sql;
#[path = "behavior/transaction.rs"]
mod transaction;
#[path = "behavior/vector.rs"]
mod vector;
