use chrono::NaiveDate;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompatibilityDate(NaiveDate);

impl CompatibilityDate {
    pub fn parse(value: &str) -> Result<Self, CompatibilityError> {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(Self)
            .map_err(|_| CompatibilityError::Date(value.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeCompatibility {
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedCompatibility {
    date: CompatibilityDate,
    node: NodeCompatibility,
}

impl ResolvedCompatibility {
    pub fn resolve(date: &str, flags: &[String]) -> Result<Self, CompatibilityError> {
        let date = CompatibilityDate::parse(date)?;
        let mut seen = BTreeSet::new();
        for flag in flags {
            if !seen.insert(flag.as_str()) {
                return Err(CompatibilityError::Duplicate(flag.clone()));
            }
            match flag.as_str() {
                "no_nodejs_compat" | "no_nodejs_compat_v2" => {}
                "nodejs_compat"
                | "nodejs_compat_v2"
                | "nodejs_als"
                | "durable_object_fetch_requires_full_url"
                | "streams_enable_constructors" => {
                    return Err(CompatibilityError::Unavailable(flag.clone()));
                }
                _ => return Err(CompatibilityError::Unknown(flag.clone())),
            }
        }
        Ok(Self {
            date,
            node: NodeCompatibility::Disabled,
        })
    }

    #[must_use]
    pub const fn date(&self) -> CompatibilityDate {
        self.date
    }

    #[must_use]
    pub const fn node(&self) -> NodeCompatibility {
        self.node
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CompatibilityError {
    #[error("compatibility date {0:?} is not a valid YYYY-MM-DD date")]
    Date(String),
    #[error("compatibility flag {0:?} is duplicated")]
    Duplicate(String),
    #[error("compatibility flag {0:?} is unknown")]
    Unknown(String),
    #[error("compatibility flag {0:?} is recognized but not implemented")]
    Unavailable(String),
}
