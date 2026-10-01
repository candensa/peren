use std::{collections::BTreeMap, path::Path};

use peren_node::Environment;

pub struct DataEnv {
    data: tempfile::TempDir,
    values: BTreeMap<String, String>,
}

impl DataEnv {
    pub fn new() -> Self {
        Self {
            data: tempfile::tempdir().unwrap(),
            values: BTreeMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn with(mut self, name: &str, value: &str) -> Self {
        self.values.insert(name.into(), value.into());
        self
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        self.data.path()
    }
}

impl Environment for DataEnv {
    fn get(&self, name: &str) -> Option<String> {
        if name == "PEREN_DATA_DIR" {
            Some(self.data.path().display().to_string())
        } else {
            self.values.get(name).cloned()
        }
    }
}
