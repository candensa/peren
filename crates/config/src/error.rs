use std::{io, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration at {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("configuration is not valid TOML")]
    Parse(#[source] toml::de::Error),
    #[error("configuration has {0} validation problem(s)")]
    Invalid(usize, Vec<Problem>),
}

impl ConfigError {
    #[must_use]
    pub fn problems(&self) -> &[Problem] {
        match self {
            Self::Invalid(_, problems) => problems,
            Self::Read { .. } | Self::Parse(_) => &[],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Problem {
    pub field: String,
    pub message: String,
}

impl Problem {
    pub(crate) fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}
