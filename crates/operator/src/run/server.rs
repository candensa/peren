use std::{collections::BTreeMap, io::Write};

use peren_node::{Process, prepare_test_server};

use crate::{
    cli,
    progress::{Progress, Style},
    run::{CliError, ProcessEnvironment, develop::Session, emit_notices, shutdown_signal},
};

pub(super) async fn serve(command: cli::Server) -> Result<(), CliError> {
    let mut inherited = BTreeMap::new();
    let mut descriptors = std::collections::BTreeSet::new();
    for socket in command.socket_fds {
        if !descriptors.insert(socket.fd) {
            return Err(CliError::DuplicateDescriptor(socket.fd));
        }
        if inherited.insert(socket.name.clone(), socket.fd).is_some() {
            return Err(CliError::DuplicateSocket(socket.name));
        }
    }
    let progress = Progress::start(Style::Arc, "Starting Peren");
    let imported = peren_migrate::load_fleet(&command.config, &command.wrangler)?;
    emit_notices(imported.notices);
    let config = imported.value;
    let process = start_with_inherited(config, inherited).await?;
    progress.success("Peren is running");
    shutdown_signal().await?;
    let progress = Progress::start(Style::Line, "Stopping Peren");
    process.shutdown().await?;
    progress.success("Peren stopped");

    Ok(())
}

#[cfg(unix)]
async fn start_with_inherited(
    config: peren_config::ValidatedConfig,
    inherited: BTreeMap<String, std::os::fd::RawFd>,
) -> Result<Process, CliError> {
    if let Some(name) = inherited.keys().next() {
        return Err(CliError::InheritedListenerUnsupported(name.clone()));
    }
    Process::start(config, &ProcessEnvironment)
        .await
        .map_err(CliError::Process)
}

#[cfg(not(unix))]
async fn start_with_inherited(
    config: peren_config::ValidatedConfig,
    inherited: BTreeMap<String, i32>,
) -> Result<Process, CliError> {
    if let Some(name) = inherited.keys().next() {
        return Err(CliError::InheritedListenerUnsupported(name.clone()));
    }
    Process::start(config, &ProcessEnvironment)
        .await
        .map_err(CliError::Process)
}

pub(super) async fn dev(command: cli::Worker) -> Result<(), CliError> {
    let session = Session::load(&command)?;
    session.print_topology(command.json)?;
    let tail_path = session.tail_path();
    let config = prepare_test_server(session.raw)?;
    let progress = Progress::start(Style::Dots, "Starting local Peren");
    let process = Process::start_development(config, &session.environment).await?;
    progress.success("Local Peren is running");
    for (name, address) in process.listeners() {
        println!("{name}: http://{address}");
    }
    for (name, address) in process.listeners() {
        println!("dev inspector {name}: http://{address}/__peren/dev");
    }
    std::io::stdout().flush().map_err(CliError::ReadyOutput)?;
    let live_tail = (!command.json).then(|| tokio::spawn(super::develop::follow_tail(tail_path)));
    shutdown_signal().await?;
    if let Some(task) = live_tail {
        task.abort();
    }
    let progress = Progress::start(Style::Line, "Stopping local Peren");
    process.shutdown().await?;
    progress.success("Local Peren stopped");
    Ok(())
}

pub(super) async fn test(command: cli::Worker) -> Result<(), CliError> {
    let session = Session::load(&command)?;
    let topology = session.topology();
    let config = prepare_test_server(session.raw)?;
    let process = Process::start_development(config, &session.environment).await?;
    println!(
        "{}",
        serde_json::json!({ "ready": true, "sockets": process.listeners(), "topology": topology })
    );
    std::io::stdout().flush().map_err(CliError::ReadyOutput)?;
    shutdown_signal().await?;
    process.shutdown().await?;
    Ok(())
}
