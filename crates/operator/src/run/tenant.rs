use crate::{
    cli,
    run::{CliError, ProcessEnvironment},
};

pub(super) fn run(command: cli::Tenant) -> Result<(), CliError> {
    match command {
        cli::Tenant::Revoke {
            config,
            tenant_id,
            reason,
            node: _,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::revoke_tenant(
                &config,
                &ProcessEnvironment,
                peren_node::TenantRevoke {
                    tenant: tenant_id,
                    reason,
                },
            )?;
            print!("revoked {}", report.revocation.tenant);
            if let Some(reason) = report.revocation.reason {
                print!(" reason={reason}");
            }
            println!();
        }
        cli::Tenant::Delete {
            config,
            tenant_id,
            reason,
            node: _,
        } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::delete_tenant(
                &config,
                &ProcessEnvironment,
                peren_node::TenantDelete {
                    tenant: tenant_id,
                    reason,
                },
            )?;
            print!("deleted {}", report.deletion.tenant);
            if let Some(reason) = report.deletion.reason {
                print!(" reason={reason}");
            }
            println!();
        }
    }
    Ok(())
}
