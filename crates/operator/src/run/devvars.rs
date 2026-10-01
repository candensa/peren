use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use peren_node::Environment;
use thiserror::Error;

use super::ProcessEnvironment;

pub(super) struct LocalDevEnvironment {
    values: BTreeMap<String, String>,
    report: LocalEnvReport,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct LocalEnvReport {
    pub(super) profile: LocalEnvProfile,
    pub(super) loaded: Vec<PathBuf>,
    pub(super) ignored: Vec<PathBuf>,
    pub(super) filtered: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum LocalEnvProfile {
    #[default]
    Peren,
    Wrangler,
}

impl LocalDevEnvironment {
    pub(super) fn load(
        config: &Path,
        profile: LocalEnvProfile,
        environment: Option<&str>,
    ) -> Result<Self, DevVarsError> {
        let base = config.parent().unwrap_or_else(|| Path::new("."));
        let loaded = match profile {
            LocalEnvProfile::Peren => load_peren(base)?,
            LocalEnvProfile::Wrangler => load_wrangler(base, environment)?,
        };
        Ok(Self {
            values: loaded.values,
            report: loaded.report,
        })
    }

    pub(super) const fn report(&self) -> &LocalEnvReport {
        &self.report
    }
}

impl Environment for LocalDevEnvironment {
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
    #[error("invalid {path:?} entry on line {line}: {reason}")]
    Entry {
        path: PathBuf,
        line: usize,
        reason: &'static str,
    },
}

struct LoadedEnvironment {
    values: BTreeMap<String, String>,
    report: LocalEnvReport,
}

fn load_peren(base: &Path) -> Result<LoadedEnvironment, DevVarsError> {
    let profile = LocalEnvProfile::Peren;
    let path = base.join(".dev.vars");
    if !path.exists() {
        return Ok(LoadedEnvironment {
            values: BTreeMap::new(),
            report: LocalEnvReport {
                profile,
                ..LocalEnvReport::default()
            },
        });
    }
    let values = read_vars_file(&path)?;
    Ok(LoadedEnvironment {
        values,
        report: LocalEnvReport {
            profile,
            loaded: vec![path],
            ..LocalEnvReport::default()
        },
    })
}

fn load_wrangler(
    base: &Path,
    environment: Option<&str>,
) -> Result<LoadedEnvironment, DevVarsError> {
    let profile = LocalEnvProfile::Wrangler;
    let mut report = LocalEnvReport {
        profile,
        ..LocalEnvReport::default()
    };
    for path in wrangler_dev_var_candidates(base, environment) {
        if path.exists() {
            report.loaded.push(path.clone());
            report.ignored = existing_paths(wrangler_dotenv_candidates(base, environment));
            return Ok(LoadedEnvironment {
                values: read_vars_file(&path)?,
                report,
            });
        }
    }

    let mut values = BTreeMap::new();
    for path in wrangler_dotenv_candidates(base, environment) {
        if !path.exists() {
            continue;
        }
        for (name, value) in read_vars_file(&path)? {
            if is_wrangler_tool_variable(&name) {
                report.filtered += 1;
                continue;
            }
            values.insert(name, value);
        }
        report.loaded.push(path);
    }
    Ok(LoadedEnvironment { values, report })
}

fn read_vars_file(path: &Path) -> Result<BTreeMap<String, String>, DevVarsError> {
    let content = std::fs::read_to_string(path).map_err(|source| DevVarsError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse(path, &content)
}

fn wrangler_dev_var_candidates(base: &Path, environment: Option<&str>) -> Vec<PathBuf> {
    match environment {
        Some(environment) => vec![
            base.join(format!(".dev.vars.{environment}")),
            base.join(".dev.vars"),
        ],
        None => vec![base.join(".dev.vars")],
    }
}

fn wrangler_dotenv_candidates(base: &Path, environment: Option<&str>) -> Vec<PathBuf> {
    let mut candidates = vec![base.join(".env")];
    if let Some(environment) = environment {
        candidates.push(base.join(format!(".env.{environment}")));
    }
    candidates.push(base.join(".env.local"));
    if let Some(environment) = environment {
        candidates.push(base.join(format!(".env.{environment}.local")));
    }
    candidates
}

fn existing_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.into_iter().filter(|path| path.exists()).collect()
}

fn is_wrangler_tool_variable(name: &str) -> bool {
    ["CLOUDFLARE_", "WRANGLER_", "MINIFLARE_"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

fn parse(path: &Path, content: &str) -> Result<BTreeMap<String, String>, DevVarsError> {
    let mut values = BTreeMap::new();
    for (index, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, value) = line.split_once('=').ok_or_else(|| DevVarsError::Entry {
            path: path.to_path_buf(),
            line: index + 1,
            reason: "expected KEY=value",
        })?;
        let name = name.trim();
        if !valid_name(name) {
            return Err(DevVarsError::Entry {
                path: path.to_path_buf(),
                line: index + 1,
                reason: "invalid variable name",
            });
        }
        values.insert(
            name.to_string(),
            parse_value(path, value.trim(), index + 1)?,
        );
    }
    Ok(values)
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|c| matches!(c, '_' | 'A'..='Z' | 'a'..='z' | '0'..='9'))
}

fn parse_value(path: &Path, value: &str, line: usize) -> Result<String, DevVarsError> {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        return unescape(path, &value[1..value.len() - 1], line);
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
            path: path.to_path_buf(),
            line,
            reason: "unterminated quoted value",
        });
    }
    Ok(value.to_string())
}

fn unescape(path: &Path, value: &str, line: usize) -> Result<String, DevVarsError> {
    let mut output = String::new();
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            output.push(c);
            continue;
        }
        let Some(next) = chars.next() else {
            return Err(DevVarsError::Entry {
                path: path.to_path_buf(),
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
                    path: path.to_path_buf(),
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
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{LocalDevEnvironment, LocalEnvProfile, parse};
    use peren_node::Environment;

    #[test]
    fn parses_dev_vars_without_exposing_values() {
        let path = std::path::Path::new(".dev.vars");
        let values = parse(
            path,
            "# local\nTOKEN=plain\nQUOTED=\"hello\\nworld\"\nSINGLE='kept literal'\n",
        )
        .unwrap();
        assert_eq!(values.get("TOKEN").unwrap(), "plain");
        assert_eq!(values.get("QUOTED").unwrap(), "hello\nworld");
        assert_eq!(values.get("SINGLE").unwrap(), "kept literal");
    }

    #[test]
    fn rejects_invalid_entries_without_value_in_error() {
        let path = std::path::Path::new(".dev.vars");
        let error = parse(path, "TOKEN=super-secret\nBAD-NAME=also-secret\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("line 2"));
        assert!(!error.contains("super-secret"));
        assert!(!error.contains("also-secret"));
    }

    #[test]
    fn wrangler_loads_dev_vars_before_dotenv_files() {
        let temp = temp_dir();
        write(&temp.join("wrangler.toml"), "");
        write(&temp.join(".dev.vars"), "VALUE=dev\n");
        write(&temp.join(".env"), "VALUE=env\n");
        let env =
            LocalDevEnvironment::load(&temp.join("wrangler.toml"), LocalEnvProfile::Wrangler, None)
                .unwrap();
        assert_eq!(env.get("VALUE").as_deref(), Some("dev"));
    }

    #[test]
    fn wrangler_falls_back_to_ordered_dotenv_files() {
        let temp = temp_dir();
        write(&temp.join("wrangler.toml"), "");
        write(&temp.join(".env"), "VALUE=base\nA=1\n");
        write(&temp.join(".env.local"), "VALUE=local\n");
        let env =
            LocalDevEnvironment::load(&temp.join("wrangler.toml"), LocalEnvProfile::Wrangler, None)
                .unwrap();
        assert_eq!(env.get("VALUE").as_deref(), Some("local"));
        assert_eq!(env.get("A").as_deref(), Some("1"));
    }

    #[test]
    fn wrangler_environment_specific_files_have_defined_precedence() {
        let temp = temp_dir();
        write(&temp.join("wrangler.toml"), "");
        write(&temp.join(".env"), "VALUE=base\n");
        write(&temp.join(".env.preview"), "VALUE=preview\n");
        write(&temp.join(".env.local"), "VALUE=local\n");
        write(&temp.join(".env.preview.local"), "VALUE=preview-local\n");
        let env = LocalDevEnvironment::load(
            &temp.join("wrangler.toml"),
            LocalEnvProfile::Wrangler,
            Some("preview"),
        )
        .unwrap();
        assert_eq!(env.get("VALUE").as_deref(), Some("preview-local"));
    }

    #[test]
    fn wrangler_filters_tool_variables_from_dotenv() {
        let temp = temp_dir();
        write(&temp.join("wrangler.toml"), "");
        write(
            &temp.join(".env"),
            "CLOUDFLARE_API_TOKEN=secret\nWRANGLER_LOG=debug\nMINIFLARE_TEST=1\nUSER_VALUE=ok\n",
        );
        let env =
            LocalDevEnvironment::load(&temp.join("wrangler.toml"), LocalEnvProfile::Wrangler, None)
                .unwrap();
        assert_eq!(env.get("USER_VALUE").as_deref(), Some("ok"));
        assert_eq!(env.get("CLOUDFLARE_API_TOKEN"), None);
        assert_eq!(env.get("WRANGLER_LOG"), None);
        assert_eq!(env.get("MINIFLARE_TEST"), None);
    }

    #[test]
    fn peren_profile_keeps_legacy_dev_vars_behavior() {
        let temp = temp_dir();
        write(&temp.join("fleet.toml"), "");
        write(&temp.join(".dev.vars"), "VALUE=dev\n");
        write(&temp.join(".env"), "VALUE=env\n");
        let env = LocalDevEnvironment::load(&temp.join("fleet.toml"), LocalEnvProfile::Peren, None)
            .unwrap();
        assert_eq!(env.get("VALUE").as_deref(), Some("dev"));
    }

    fn temp_dir() -> std::path::PathBuf {
        for attempt in 0..100 {
            let name = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("peren-devvars-{name}-{attempt}"));
            if fs::create_dir(&path).is_ok() {
                return path;
            }
        }
        panic!("failed to create temporary devvars directory");
    }

    fn write(path: &std::path::Path, content: &str) {
        fs::write(path, content).unwrap();
    }
}
