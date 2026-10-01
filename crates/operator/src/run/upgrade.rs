use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) async fn run(command: cli::Upgrade) -> Result<(), CliError> {
    match command {
        cli::Upgrade::Check { config, target } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::check_upgrade(
                config,
                &ProcessEnvironment,
                peren_node::UpgradeCheck { target },
            )
            .await?;
            println!(
                "upgrade check current={} target={} storage={} services={} sockets={}",
                report.current, report.target, report.storage, report.services, report.sockets
            );
        }
        cli::Upgrade::Plan { config, target } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::plan_upgrade(
                config,
                &ProcessEnvironment,
                peren_node::UpgradePlan { target },
            )
            .await?;
            println!(
                "upgrade plan current={} target={} steps={}",
                report.check.current,
                report.check.target,
                report.steps.len()
            );
            for step in report.steps {
                println!("{}. {}", step.order, step.action);
            }
        }
    }
    Ok(())
}
