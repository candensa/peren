use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use peren_node::Environment;
use thiserror::Error;

use super::ProcessEnvironment;

pub(super) struct DevEnvironment {
    values: BTreeMap<String, String>,
}

impl DevEnvironment {
    pub(super) fn load(config: &Path) -> Result<Self, DevVarsError> {
        let path = config.parent().map_or_else(
            || PathBuf::from(".dev.vars"),
            |parent| parent.join(".dev.vars"),
        );
        if !path.exists() {
            return Ok(Self {
                values: BTreeMap::new(),
            });
        }
        let content = std::fs::read_to_string(&path).map_err(|source| DevVarsError::Read {
            path: path.clone(),
            source,
        })?;
        let values = parse(&content)?;
        Ok(Self { values })
    }
}

impl Environment for DevEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        self.values
            .get(name)
            .cloned()
            .or_else(|| ProcessEnvironment.get(name))
    }
}

#[derive(Debug, Error)]
pub enum DevVarsError {
    #[error("failed to read {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid .dev.vars entry on line {line}: {reason}")]
    Entry { line: usize, reason: &'static str },
}

fn parse(content: &str) -> Result<BTreeMap<String, String>, DevVarsError> {
    let mut values = BTreeMap::new();
    for (index, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, value) = line.split_once('=').ok_or(DevVarsError::Entry {
            line: index + 1,
            reason: "expected KEY=value",
        })?;
        let name = name.trim();
        if !valid_name(name) {
            return Err(DevVarsError::Entry {
                line: index + 1,
                reason: "invalid variable name",
            });
        }
        values.insert(name.to_string(), parse_value(value.trim(), index + 1)?);
    }
    Ok(values)
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|c| matches!(c, '_' | 'A'..='Z' | 'a'..='z' | '0'..='9'))
}

fn parse_value(value: &str, line: usize) -> Result<String, DevVarsError> {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        return unescape(&value[1..value.len() - 1], line);
    }
    if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        return Ok(value[1..value.len() - 1].to_string());
    }
    if value.starts_with('"')
        || value.starts_with('\'')
        || value.ends_with('"')
        || value.ends_with('\'')
    {
        return Err(DevVarsError::Entry {
            line,
            reason: "unterminated quoted value",
        });
    }
    Ok(value.to_string())
}

fn unescape(value: &str, line: usize) -> Result<String, DevVarsError> {
    let mut output = String::new();
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            output.push(c);
            continue;
        }
        let Some(next) = chars.next() else {
            return Err(DevVarsError::Entry {
                line,
                reason: "unfinished escape sequence",
            });
        };
        match next {
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            '\\' => output.push('\\'),
            '"' => output.push('"'),
            _ => {
                return Err(DevVarsError::Entry {
                    line,
                    reason: "unsupported escape sequence",
                });
            }
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_dev_vars_without_exposing_values() {
        let values =
            parse("# local\nTOKEN=plain\nQUOTED=\"hello\\nworld\"\nSINGLE='kept literal'\n")
                .unwrap();
        assert_eq!(values.get("TOKEN").unwrap(), "plain");
        assert_eq!(values.get("QUOTED").unwrap(), "hello\nworld");
        assert_eq!(values.get("SINGLE").unwrap(), "kept literal");
    }

    #[test]
    fn rejects_invalid_entries_without_value_in_error() {
        let error = parse("TOKEN=super-secret\nBAD-NAME=also-secret\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("line 2"));
        assert!(!error.contains("super-secret"));
        assert!(!error.contains("also-secret"));
    }
}
