use std::{collections::BTreeMap, path::PathBuf};

use crate::Environment;

pub(crate) struct DataEnv {
    data: PathBuf,
    variables: BTreeMap<String, String>,
}

impl DataEnv {
    pub(crate) fn new(data: impl Into<PathBuf>) -> Self {
        Self {
            data: data.into(),
            variables: BTreeMap::new(),
        }
    }

    pub(crate) fn temp(prefix: &str) -> Self {
        Self::new(std::env::temp_dir().join(format!("peren-{prefix}-{}", uuid::Uuid::new_v4())))
    }

    pub(crate) fn with(mut self, name: &str, value: &str) -> Self {
        self.variables.insert(name.into(), value.into());
        self
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.data
    }
}

impl Environment for DataEnv {
    fn get(&self, name: &str) -> Option<String> {
        if name == "PEREN_DATA_DIR" {
            Some(self.data.display().to_string())
        } else {
            self.variables.get(name).cloned()
        }
    }
}
