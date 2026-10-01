use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn rollback(command: cli::Rollback) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(command.config)?.validate()?;
    let deployment = peren_node::Deployment::new(&config, &ProcessEnvironment);
    let report = deployment.rollback(&peren_node::DeployRollback {
        service: command.service,
        digest: command.digest,
    })?;
    println!(
        "rolled back {} to {}",
        report.generation.service, report.generation.digest
    );
    Ok(())
}

pub(super) fn backup(command: cli::Backup) -> Result<(), CliError> {
    peren_config::FleetConfig::from_path(command.config)?.validate()?;
    let report = peren_node::backup(
        &ProcessEnvironment,
        &peren_node::Backup {
            output: command.output,
        },
    )?;
    println!(
        "backed up {} to {} files={}",
        report.source.display(),
        report.output.display(),
        report.files
    );
    Ok(())
}

pub(super) fn restore(command: cli::Restore) -> Result<(), CliError> {
    peren_config::FleetConfig::from_path(command.config)?.validate()?;
    let report = peren_node::restore(
        &ProcessEnvironment,
        &peren_node::Restore {
            input: command.input,
            force: command.force,
        },
    )?;
    println!(
        "restored {} to {} files={}",
        report.input.display(),
        report.output.display(),
        report.files
    );
    Ok(())
}

pub(super) fn uninstall(command: cli::Uninstall) -> Result<(), CliError> {
    peren_config::FleetConfig::from_path(command.config)?.validate()?;
    let report = peren_node::uninstall(
        &ProcessEnvironment,
        &peren_node::Uninstall {
            force: command.force,
            dry: command.dry_run,
        },
    )?;
    if report.dry {
        println!(
            "would uninstall {} files={}",
            report.path.display(),
            report.files
        );
    } else {
        println!(
            "uninstalled {} files={}",
            report.path.display(),
            report.files
        );
    }
    Ok(())
}
