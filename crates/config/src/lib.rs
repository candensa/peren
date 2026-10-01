mod descriptor;
mod error;
mod feature;
mod model;
mod provider;
mod queue;
mod service;
mod validate;

use std::path::Path;

pub use descriptor::*;
pub use error::{ConfigError, Problem};
pub use feature::*;
pub use model::*;
pub use provider::*;
pub use queue::*;
pub use service::*;

impl FleetConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&source)
    }

    pub fn from_toml(source: &str) -> Result<Self, ConfigError> {
        toml::from_str(source).map_err(ConfigError::Parse)
    }

    pub fn validate(self) -> Result<ValidatedConfig, ConfigError> {
        validate::validate(self)
    }
}
