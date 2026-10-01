use std::path::{Path, PathBuf};

pub struct TestWorker {
    root: PathBuf,
    entry: PathBuf,
}

impl TestWorker {
    #[must_use]
    pub fn from_source(source: &str) -> Self {
        let root = std::env::temp_dir().join(format!("peren-worker-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test worker directory");
        let entry = root.join("worker.js");
        std::fs::write(&entry, source).expect("write test worker source");
        Self { root, entry }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.entry
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
