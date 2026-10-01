use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) async fn run(command: cli::Console) -> Result<(), CliError> {
    match command {
        cli::Console::Bootstrap {
            config,
            workspace_name,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::bootstrap_console(
                &config,
                &ProcessEnvironment,
                &peren_node::ConsoleBootstrap {
                    workspace: workspace_name,
                },
            )
            .await?;
            println!("workspace: {}", report.workspace);
            println!("token: {}", report.token);
            println!("expires_in_minutes: {}", report.expires_in_minutes);
        }
        cli::Console::Register {
            config,
            token,
            email,
            name,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::register_console(
                &config,
                &ProcessEnvironment,
                &peren_node::ConsoleRegister { token, email, name },
            )
            .await?;
            println!("user: {}", report.user);
            if let Some(workspace) = report.workspace {
                println!("workspace: {workspace}");
            }
        }
    }
    Ok(())
}
