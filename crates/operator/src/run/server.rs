use std::{collections::BTreeMap, io::Write, path::PathBuf};

use peren_node::{Process, prepare_test_server};
use tokio::task::JoinHandle;

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
    let mut running = start_dev_session(&command).await?;
    loop {
        let mut reload = tokio::spawn(super::develop::watch_reload(running.watch_paths.clone()));
        tokio::select! {
            signal = shutdown_signal() => {
                signal?;
                running.abort_tail();
                reload.abort();
                let progress = Progress::start(Style::Line, "Stopping local Peren");
                running.process.shutdown().await?;
                progress.success("Local Peren stopped");
                return Ok(());
            }
            changed = &mut reload => {
                let changed = changed.map_err(|source| CliError::Reload(source.to_string()))?;
                let progress = Progress::start(Style::Line, "Reloading local Peren");
                match start_dev_session(&command).await {
                    Ok(next) => {
                        running.abort_tail();
                        running.process.shutdown().await?;
                        let message = format!("Reloaded after {}", changed.display());
                        progress.success(&message);
                        running = next;
                    }
                    Err(error) => {
                        progress.success("Reload failed; keeping previous local Peren running");
                        eprintln!("reload failed after {}: {error}", changed.display());
                    }
                }
            }
        }
    }
}

struct RunningDev {
    process: Process,
    live_tail: Option<JoinHandle<()>>,
    watch_paths: Vec<PathBuf>,
}

impl RunningDev {
    fn abort_tail(&mut self) {
        if let Some(task) = self.live_tail.take() {
            task.abort();
        }
    }
}

async fn start_dev_session(command: &cli::Worker) -> Result<RunningDev, CliError> {
    let session = Session::load(command)?;
    session.print_topology(command.json)?;
    let tail_path = session.tail_path();
    let watch_paths = session.watch_paths(&command.config);
    let config = prepare_test_server(session.raw.clone())?;
    let progress = Progress::start(Style::Dots, "Starting local Peren");
    let process = Process::start_development(config, &session.environment).await?;
    progress.success("Local Peren is running");
    for (name, address) in process.listeners() {
        println!("{name}: http://{address}");
    }
    for (name, address) in process.listeners() {
        println!("dev inspector {name}: http://{address}/__peren/dev");
    }
    println!("watching {} path(s) for reload", watch_paths.len());
    std::io::stdout().flush().map_err(CliError::ReadyOutput)?;
    let live_tail = (!command.json).then(|| tokio::spawn(super::develop::follow_tail(tail_path)));
    Ok(RunningDev {
        process,
        live_tail,
        watch_paths,
    })
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
