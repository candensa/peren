use std::path::Path;

use crate::{
    cli,
    kubernetes::{Backend, CheckReport, HelmBackend, Options},
    run::CliError,
};

pub(super) fn run(command: cli::Kubernetes) -> Result<(), CliError> {
    match command {
        cli::Kubernetes::Render(command) => render(command),
        cli::Kubernetes::Check(command) => check(&command),
    }
}

fn check(command: &cli::KubernetesCheck) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(&command.config)?.validate()?;
    let report = CheckReport::new(&config.raw, command.replicas);
    if command.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        if report.problems.is_empty() {
            println!("kubernetes conformance ok");
        } else {
            for problem in &report.problems {
                eprintln!("error: {}: {}", problem.field, problem.message);
            }
        }
        for warning in &report.warnings {
            eprintln!("warning: {}: {}", warning.field, warning.message);
        }
    }
    if report.problems.is_empty() {
        Ok(())
    } else {
        Err(CliError::Kubernetes(format!(
            "{} problem(s)",
            report.problems.len()
        )))
    }
}

fn render(command: cli::KubernetesRender) -> Result<(), CliError> {
    let config_text =
        std::fs::read_to_string(&command.config).map_err(|source| CliError::Read {
            path: command.config.clone(),
            source,
        })?;
    let config = peren_config::FleetConfig::from_toml(&config_text)?.validate()?;
    let plan = HelmBackend.plan(
        &config.raw,
        Options {
            image: command.image,
            tag: command.tag,
            replicas: command.replicas,
            storage: command.storage,
            annotations: command.service_account_annotations,
            config: config_text,
        },
    );

    match command.output {
        Some(path) => {
            write(&path, plan.values.as_bytes())?;
            println!("rendered {}", path.display());
        }
        None => print!("{}", plan.values),
    }
    Ok(())
}

fn write(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    std::fs::write(path, bytes).map_err(|source| CliError::Write {
        path: path.to_path_buf(),
        source,
    })
}
