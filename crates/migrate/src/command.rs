use std::{collections::BTreeMap, fs, path::Path};

use peren_config::{
    Assets, CronTrigger, Entrypoint, FleetConfig, QueueConsumer, RunWorkerFirst, Service,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    MigrateError, MigratedDeployment, MigratedRunWorkerFirst, MigrationWarning, WranglerFormat,
    import_sam_template, import_serverless_yml, import_wrangler,
};

pub struct Command {
    pub source: std::path::PathBuf,
    pub output: Option<std::path::PathBuf>,
    pub format: Option<Source>,
    pub compatibility_date: Option<String>,
}

#[derive(Clone, Copy)]
pub enum Source {
    Wrangler,
    Sam,
    Serverless,
}

pub struct Imported<T> {
    pub value: T,
    pub notices: Vec<Notice>,
}

pub struct Receipt {
    pub stdout: Option<String>,
    pub notices: Vec<Notice>,
}

pub struct Notice {
    pub service: String,
    pub warning: MigrationWarning,
}

pub fn load_fleet(
    config: &Path,
    wrangler: &[std::path::PathBuf],
) -> Result<Imported<peren_config::ValidatedConfig>, Error> {
    let imported = load_raw(config, wrangler)?;
    Ok(Imported {
        value: imported.value.validate()?,
        notices: imported.notices,
    })
}

pub fn load_raw(
    config: &Path,
    wrangler: &[std::path::PathBuf],
) -> Result<Imported<FleetConfig>, Error> {
    let source = read(config)?;
    let mut fleet = FleetConfig::from_toml(&source)?;
    let mut notices = Vec::new();
    for path in wrangler {
        let source = read(path)?;
        let migrated = import_wrangler(&source, wrangler_format(path))?;
        notices.extend(notices_from(&migrated));
        fleet.services.push(service(migrated)?);
    }
    Ok(Imported {
        value: fleet,
        notices,
    })
}

pub fn run(command: Command) -> Result<Receipt, Error> {
    let source = read(&command.source)?;
    let format = match command.format {
        Some(format) => format,
        None => detect(&command.source, &source)?,
    };
    let mut migrated = match format {
        Source::Wrangler => vec![import_wrangler(&source, wrangler_format(&command.source))?],
        Source::Sam => import_sam_template(&source)?,
        Source::Serverless => import_serverless_yml(&source)?,
    };
    if migrated.is_empty() {
        return Err(Error::Empty(command.source));
    }
    if let Some(date) = command.compatibility_date {
        for deployment in &mut migrated {
            deployment.compatibility_date = Some(date.clone());
        }
    }
    let notices = migrated.iter().flat_map(notices_from).collect();
    let services = migrated
        .into_iter()
        .map(service)
        .collect::<Result<Vec<_>, _>>()?;
    let output = toml::to_string_pretty(&Fragment { services })?;
    if let Some(path) = command.output {
        fs::write(&path, output).map_err(|source| Error::Write { path, source })?;
        Ok(Receipt {
            stdout: None,
            notices,
        })
    } else {
        Ok(Receipt {
            stdout: Some(output),
            notices,
        })
    }
}

fn service(migrated: MigratedDeployment) -> Result<Service, Error> {
    migrated.validate_ready_to_deploy()?;
    let compatibility_date = migrated
        .compatibility_date
        .ok_or_else(|| Error::CompatibilityDate(migrated.worker_name.clone()))?;
    Ok(Service {
        name: migrated.worker_name,
        worker_bundle_path: migrated.main_module_path.into(),
        compatibility_date,
        compatibility_flags: migrated.compatibility_flags,
        entrypoint: Entrypoint::Stateless,
        cron_triggers: migrated
            .cron_expressions
            .into_iter()
            .map(|expression| CronTrigger { expression })
            .collect(),
        tail_consumers: Vec::new(),
        consumes_queues: migrated
            .queue_consumers
            .into_iter()
            .map(QueueConsumer::Name)
            .collect(),
        bindings: migrated.bindings.into_iter().collect(),
        vars: migrated.plain_vars.into_iter().collect(),
        expose_node_id: false,
        secrets: BTreeMap::default(),
        secrets_store_refs: BTreeMap::default(),
        additional_modules: BTreeMap::default(),
        source_maps: BTreeMap::default(),
        assets: migrated.assets_directory.map(|directory| Assets {
            directory: directory.into(),
            run_worker_first: migrated.assets_run_worker_first.map(|value| match value {
                MigratedRunWorkerFirst::Always(value) => RunWorkerFirst::Always(value),
                MigratedRunWorkerFirst::Patterns(patterns) => RunWorkerFirst::Patterns(patterns),
            }),
        }),
        isolate_fair_share_percent: None,
        checkpoint_threshold_bytes: None,
        max_cpu_time_ms: None,
        max_subrequests_per_invocation: None,
        max_heap_bytes: None,
        max_execution_time_ms: None,
        workflow_retention_days: None,
        deploy_max_resident_age_secs: None,
        max_steps_per_instance: None,
        tenant_id: None,
        project_id: None,
        placement_regions: Vec::new(),
    })
}

fn notices_from(migrated: &MigratedDeployment) -> impl Iterator<Item = Notice> + '_ {
    migrated.warnings.iter().cloned().map(|warning| Notice {
        service: migrated.worker_name.clone(),
        warning,
    })
}

fn detect(path: &Path, source: &str) -> Result<Source, Error> {
    match extension(path) {
        Some("toml" | "json" | "jsonc") => Ok(Source::Wrangler),
        Some("yaml" | "yml")
            if source.contains("AWS::Serverless")
                || source.contains("AWSTemplateFormatVersion") =>
        {
            Ok(Source::Sam)
        }
        Some("yaml" | "yml")
            if source.lines().any(|line| {
                let line = line.trim_start();
                line.starts_with("provider:") || line.starts_with("functions:")
            }) =>
        {
            Ok(Source::Serverless)
        }
        _ => Err(Error::UnknownFormat(path.to_path_buf())),
    }
}

fn wrangler_format(path: &Path) -> WranglerFormat {
    match extension(path) {
        Some("json") => WranglerFormat::Json,
        Some("jsonc") => WranglerFormat::Jsonc,
        _ => WranglerFormat::Toml,
    }
}

fn extension(path: &Path) -> Option<&str> {
    path.extension().and_then(std::ffi::OsStr::to_str)
}

fn read(path: &Path) -> Result<String, Error> {
    fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })
}

#[derive(Serialize)]
struct Fragment {
    services: Vec<Service>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to read {path}")]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path}")]
    Write {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Config(#[from] peren_config::ConfigError),
    #[error(transparent)]
    Migration(#[from] MigrateError),
    #[error("migration source {0} contains no deployable service")]
    Empty(std::path::PathBuf),
    #[error("service {0:?} has no compatibility_date")]
    CompatibilityDate(String),
    #[error(
        "cannot determine the format of {0}; pass --from wrangler, --from sam, or --from serverless"
    )]
    UnknownFormat(std::path::PathBuf),
    #[error("failed to serialize migrated service configuration")]
    Serialize(#[from] toml::ser::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLEET: &str = r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "hello"
"#;

    const WRANGLER: &str = r#"
name = "hello"
main = "dist/index.js"
compatibility_date = "2026-01-01"
[[kv_namespaces]]
binding = "CACHE"
id = "cloudflare-id"
"#;

    #[test]
    fn startup_imports_wrangler_before_fleet_validation() {
        let directory = tempfile::tempdir().unwrap();
        let fleet = directory.path().join("fleet.toml");
        let wrangler = directory.path().join("wrangler.toml");
        fs::write(&fleet, FLEET).unwrap();
        fs::write(&wrangler, WRANGLER).unwrap();

        let config = load_fleet(&fleet, &[wrangler]).unwrap().value;
        assert_eq!(config.raw.services[0].name, "hello");
        assert!(matches!(
            config.raw.services[0].bindings.get("CACHE"),
            Some(peren_config::Binding::Kv { .. })
        ));
    }

    #[test]
    fn migrate_writes_a_reusable_service_fragment() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("wrangler.toml");
        let output = directory.path().join("services.toml");
        fs::write(&source, WRANGLER).unwrap();

        let receipt = run(Command {
            source,
            output: Some(output.clone()),
            format: None,
            compatibility_date: None,
        })
        .unwrap();

        assert!(receipt.stdout.is_none());
        let fragment = fs::read_to_string(output).unwrap();
        assert!(fragment.contains("[[services]]"));
        assert!(fragment.contains("name = \"hello\""));
        assert!(fragment.contains("type = \"kv\""));
    }

    #[test]
    fn command_supplies_the_runtime_date_missing_from_lambda_formats() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("serverless.yml");
        fs::write(
            &source,
            "service: jobs\nfunctions:\n  worker:\n    handler: index.js\n",
        )
        .unwrap();

        let receipt = run(Command {
            source,
            output: None,
            format: Some(Source::Serverless),
            compatibility_date: Some("2026-01-01".to_string()),
        })
        .unwrap();

        let fragment = receipt.stdout.unwrap();
        assert!(fragment.contains("compatibility_date = \"2026-01-01\""));
    }
}
