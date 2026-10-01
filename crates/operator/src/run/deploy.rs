use crate::cli;

use super::{CliError, ProcessEnvironment};

pub(super) fn run(command: cli::Deploy) -> Result<(), CliError> {
    match command.command {
        Some(cli::DeployCommand::Health { config, service }) => deploy_health(config, service)?,
        Some(cli::DeployCommand::List { config, service }) => deploy_list(config, service)?,
        Some(cli::DeployCommand::Verify { config, service }) => deploy_verify(config, service)?,
        Some(cli::DeployCommand::Prune {
            config,
            service,
            keep,
            dry_run,
        }) => deploy_prune(config, service, keep, dry_run)?,
        None => deploy_record(command)?,
    }
    Ok(())
}

fn deploy_health(config: std::path::PathBuf, service: Option<String>) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(config)?.validate()?;
    let deployment = peren_node::Deployment::new(&config, &ProcessEnvironment);
    let report = deployment.health(&peren_node::DeployHealth { service })?;
    for service in report.services {
        println!(
            "{} {} healthy percent={}",
            service.service, service.digest, service.percent
        );
    }
    Ok(())
}

fn deploy_list(config: std::path::PathBuf, service: Option<String>) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(config)?.validate()?;
    let deployment = peren_node::Deployment::new(&config, &ProcessEnvironment);
    let report = deployment.list(&peren_node::DeployList { service })?;
    for generation in report.generations {
        println!(
            "{} {} active={} preview={} percent={} entry={} modules={} maps={}",
            generation.service,
            generation.digest,
            generation.active,
            generation.preview,
            generation.percent,
            generation.entry,
            generation.modules,
            generation.source_maps
        );
    }
    Ok(())
}

fn deploy_verify(config: std::path::PathBuf, service: Option<String>) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(config)?.validate()?;
    let deployment = peren_node::Deployment::new(&config, &ProcessEnvironment);
    let report = deployment.verify(&peren_node::DeployVerify { service })?;
    for item in report.generations {
        println!(
            "{} {} verified active={} percent={}",
            item.generation.service,
            item.generation.digest,
            item.generation.active,
            item.generation.percent
        );
    }
    Ok(())
}

fn deploy_prune(
    config: std::path::PathBuf,
    service: String,
    keep: usize,
    dry: bool,
) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(config)?.validate()?;
    let deployment = peren_node::Deployment::new(&config, &ProcessEnvironment);
    let report = deployment.prune(&peren_node::DeployPrune { service, keep, dry })?;
    if report.dry {
        println!(
            "would prune {} generation{}",
            report.removed.len(),
            suffix(report.removed.len())
        );
    } else {
        println!(
            "pruned {} generation{}",
            report.removed.len(),
            suffix(report.removed.len())
        );
    }
    Ok(())
}

fn deploy_record(command: cli::Deploy) -> Result<(), CliError> {
    let config = command
        .config
        .ok_or(CliError::Unsupported("deploy requires a config path"))?;
    let config = peren_config::FleetConfig::from_path(config)?.validate()?;
    let deployment = peren_node::Deployment::new(&config, &ProcessEnvironment);
    let report = deployment.record(&peren_node::DeployRecord {
        percent: command.percent,
        preview: command.preview,
    })?;
    for generation in report.generations {
        println!(
            "deployed {} {} preview={} percent={} maps={}",
            generation.service,
            generation.digest,
            generation.preview,
            generation.percent,
            generation.source_maps
        );
    }
    Ok(())
}

fn suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}
