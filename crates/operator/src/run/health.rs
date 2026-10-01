use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use crate::cli;

use super::{CliError, ProcessEnvironment};

pub(super) async fn conformance(command: cli::Conformance) -> Result<(), CliError> {
    match command {
        cli::Conformance::Storage { config } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::conformance_storage(
                config,
                &ProcessEnvironment,
                peren_node::ConformanceStorage,
            )
            .await?;
            println!(
                "storage node={} cell={} epoch={} revision={} bytes={} range_read={}",
                report.node.as_uuid(),
                report.cell,
                report.epoch.get(),
                report.revision.get(),
                report.bytes,
                report.range_read
            );
        }
    }
    Ok(())
}

pub(super) fn node(command: cli::Node) -> Result<(), CliError> {
    match command {
        cli::Node::Health { config } => {
            let config = peren_config::FleetConfig::from_path(config)?.validate()?;
            let mut targets = BTreeMap::new();
            targets.insert("peer".to_string(), config.peer_listen);
            targets.extend(config.sockets);
            for (name, address) in targets {
                probe(&name, address)?;
                println!("{name} {address} ready");
            }
        }
        cli::Node::Join {
            config,
            key_dir,
            token,
        } => {
            peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::join_node(
                &ProcessEnvironment,
                &peren_node::NodeJoin { key_dir, token },
            )?;
            println!(
                "node {} joined peer={}",
                report.node.as_uuid(),
                report.peer_addr.as_deref().unwrap_or("")
            );
        }
        cli::Node::Drain {
            config,
            node,
            reason,
        } => {
            peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::drain_node(
                &ProcessEnvironment,
                peren_node::NodeDrain {
                    node: peren_primitives::NodeId::from_uuid(node),
                    reason,
                },
            )?;
            println!("node {} draining", report.node.as_uuid());
        }
        cli::Node::Remove {
            config,
            node,
            force,
        } => {
            peren_config::FleetConfig::from_path(config)?.validate()?;
            let report = peren_node::remove_node(
                &ProcessEnvironment,
                &peren_node::NodeRemove {
                    node: peren_primitives::NodeId::from_uuid(node),
                    force,
                },
            )?;
            println!("node {} removed", report.node.as_uuid());
        }
    }
    Ok(())
}

fn probe(name: &str, address: SocketAddr) -> Result<(), CliError> {
    let timeout = Duration::from_secs(2);
    let mut stream =
        TcpStream::connect_timeout(&address, timeout).map_err(|error| CliError::NodeHealth {
            name: name.to_string(),
            address,
            reason: error.to_string(),
        })?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    stream
        .write_all(b"GET /readyz HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
        .map_err(|error| CliError::NodeHealth {
            name: name.to_string(),
            address,
            reason: error.to_string(),
        })?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| CliError::NodeHealth {
            name: name.to_string(),
            address,
            reason: error.to_string(),
        })?;
    if response.starts_with("HTTP/1.1 200") {
        Ok(())
    } else {
        Err(CliError::NodeHealth {
            name: name.to_string(),
            address,
            reason: response
                .lines()
                .next()
                .unwrap_or("empty response")
                .to_string(),
        })
    }
}

pub(super) async fn doctor(command: cli::Doctor) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(&command.config)?.validate()?;
    let report = peren_node::diagnose(
        config,
        &ProcessEnvironment,
        peren_node::Diagnose {
            storage: command.storage_test,
            readonly: command.read_only,
        },
    )
    .await?;
    print_diagnose(&report, false);
    Ok(())
}

pub(super) async fn status(command: cli::Status) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(&command.config)?.validate()?;
    let report = peren_node::diagnose(
        config,
        &ProcessEnvironment,
        peren_node::Diagnose {
            storage: command.storage_test,
            readonly: !command.storage_test,
        },
    )
    .await?;
    print_status(&report, command.json);
    Ok(())
}

fn print_status(report: &peren_node::DiagnoseReport, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ready": true,
                "node": report.node.as_uuid(),
                "services": report.services,
                "sockets": report.sockets,
                "storage": storage_name(&report.storage),
            })
        );
    } else {
        println!("ready: true");
        println!("node: {}", report.node.as_uuid());
        println!("services: {}", report.services);
        println!("sockets: {}", report.sockets);
        println!("storage: {}", storage_name(&report.storage));
    }
}

pub(super) async fn diagnose(command: cli::Diagnose) -> Result<(), CliError> {
    let config = peren_config::FleetConfig::from_path(&command.config)?.validate()?;
    let report = peren_node::diagnose(
        config,
        &ProcessEnvironment,
        peren_node::Diagnose {
            storage: command.storage_test,
            readonly: command.read_only,
        },
    )
    .await?;
    print_diagnose(&report, command.json);
    Ok(())
}

fn print_diagnose(report: &peren_node::DiagnoseReport, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "node": report.node.as_uuid(),
                "services": report.services,
                "sockets": report.sockets,
                "storage": storage_name(&report.storage),
            })
        );
    } else {
        println!("node: {}", report.node.as_uuid());
        println!("services: {}", report.services);
        println!("sockets: {}", report.sockets);
        println!("storage: {}", storage_name(&report.storage));
    }
}

fn storage_name(storage: &peren_node::DiagnoseStorage) -> &'static str {
    match storage {
        peren_node::DiagnoseStorage::Skipped => "skipped",
        peren_node::DiagnoseStorage::ReadOnly => "readonly",
        peren_node::DiagnoseStorage::Writable => "writable",
    }
}
