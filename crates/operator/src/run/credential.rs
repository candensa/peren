use crate::{cli, run::CliError};

pub(super) fn devcert(output: std::path::PathBuf) -> Result<(), CliError> {
    peren_security::devcert(&peren_security::Devcert { directory: output })?;
    Ok(())
}

pub(super) fn run(command: cli::Credential) -> Result<(), CliError> {
    match command {
        cli::Credential::Mint {
            key_dir,
            tenant,
            bucket_prefix,
            scope,
            scopes,
        } => {
            let mut requested = Vec::with_capacity(scopes.len() + 1);
            requested.push(scope);
            requested.extend(scopes);
            let token = peren_security::mint(
                &key_dir,
                peren_security::Mint {
                    tenant,
                    bucket: bucket_prefix,
                    scopes: requested,
                },
            )?;
            println!("{token}");
        }
        cli::Credential::Node {
            key_dir,
            cluster,
            node,
            peer_addr,
        } => {
            let token = peren_security::mint_node(
                &key_dir,
                peren_security::NodeMint {
                    cluster,
                    node: node.to_string(),
                    peer_addr,
                },
            )?;
            println!("{token}");
        }
    }
    Ok(())
}
