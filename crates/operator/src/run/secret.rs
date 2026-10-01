use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn run(command: cli::Secrets) -> Result<(), CliError> {
    match command {
        cli::Secrets::Put {
            config,
            name,
            value,
            node: _,
        }
        | cli::Secrets::Rotate {
            config,
            name,
            value,
            node: _,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::rotate_secret(
                &config,
                &ProcessEnvironment,
                peren_node::SecretRotate { name, value },
            )?;
            print_secret(&report.secret);
        }
        cli::Secrets::List { config, node: _ } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            for secret in peren_node::list_secrets(&config, &ProcessEnvironment)?.secrets {
                print_secret(&secret);
            }
        }
        cli::Secrets::Get {
            config,
            name,
            node: _,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::get_secret(&config, &ProcessEnvironment, &name)?;
            print_secret(&report.secret);
        }
        cli::Secrets::Delete {
            config,
            name,
            node: _,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::delete_secret(&config, &ProcessEnvironment, &name)?;
            println!("deleted {name} active={}", report.deleted);
        }
    }
    Ok(())
}

fn print_secret(secret: &peren_node::SecretMetadata) {
    println!(
        "{} version={} digest={} created_at_ms={}",
        secret.name, secret.version, secret.digest, secret.created_at_ms
    );
}
