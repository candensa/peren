use std::{net::SocketAddr, path::Path};

pub(crate) use peren_node::ProcessEnvironment;
use thiserror::Error;

use crate::{
    cli::{self, Cli, Command},
    progress::{Progress, Style},
};

pub async fn run(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Init(command) => init(command)?,
        Command::Config { command } => config(command)?,
        Command::Conformance { command } => health::conformance(command).await?,
        Command::Doctor(command) => health::doctor(command).await?,
        Command::Status(command) => health::status(command).await?,
        Command::Node { command } => health::node(command)?,
        Command::Serve(command) => server::serve(command).await?,
        Command::Dev(command) => server::dev(command).await?,
        Command::Devcert { output_dir } => credential::devcert(output_dir)?,
        Command::TestServer(command) => server::test(command).await?,
        Command::Diagnose(command) => health::diagnose(command).await?,
        Command::Credential { command } => credential::run(command)?,
        Command::D1 { command } => crate::data::d1(command)?,
        Command::Storage { command } => crate::data::storage(command).await?,
        Command::Kv { command } => crate::data::kv(command)?,
        Command::Queue { command } => crate::data::queue(command).await?,
        Command::Kubernetes { command } => kubernetes::run(command)?,
        Command::Migrate(command) => migrate(command)?,
        Command::Deploy(command) => deploy::run(command)?,
        Command::Rollback(command) => backup::rollback(command)?,
        Command::Backup(command) => backup::backup(command)?,
        Command::Restore(command) => backup::restore(command)?,
        Command::Uninstall(command) => backup::uninstall(command)?,
        Command::Upgrade { command } => upgrade::run(command).await?,
        Command::Logs(command) | Command::Tail(command) => tail::run(command)?,
        Command::Trace(command) => trace::run(command)?,
        Command::Tenant { command } => tenant::run(command)?,
        Command::Secrets { command } => secret::run(command)?,
        Command::Workflow { command } => workflow::run(command)?,
        Command::Console { command } => console::run(command).await?,
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Process(#[from] peren_node::ProcessError),
    #[error(transparent)]
    Development(#[from] peren_node::DevelopmentError),
    #[error("{0}")]
    Unsupported(&'static str),
    #[error("socket descriptor {0:?} was supplied more than once")]
    DuplicateSocket(String),
    #[error("file descriptor {0} was supplied for more than one listener")]
    DuplicateDescriptor(i32),
    #[error(transparent)]
    Migrate(#[from] peren_migrate::CommandError),
    #[error(transparent)]
    Deploy(#[from] peren_node::DeployError),
    #[error(transparent)]
    Backup(#[from] peren_node::BackupError),
    #[error(transparent)]
    Config(#[from] peren_config::ConfigError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Diagnose(#[from] peren_node::DiagnoseError),
    #[error(transparent)]
    Conformance(#[from] peren_node::ConformanceError),
    #[error(transparent)]
    Fleet(#[from] peren_node::FleetError),
    #[error(transparent)]
    Console(#[from] peren_node::ConsoleError),
    #[error(transparent)]
    Credential(#[from] peren_security::CredentialError),
    #[error(transparent)]
    Devcert(#[from] peren_security::DevcertError),
    #[error(transparent)]
    D1(#[from] peren_node::D1QueryError),
    #[error(transparent)]
    Secret(#[from] peren_node::SecretError),
    #[error(transparent)]
    Tenant(#[from] peren_node::TenantError),
    #[error(transparent)]
    Tail(#[from] peren_node::TailError),
    #[error(transparent)]
    Trace(#[from] peren_node::TraceError),
    #[error(transparent)]
    Workflow(#[from] peren_node::WorkflowError),
    #[error(transparent)]
    Queue(#[from] peren_queues::QueueError),
    #[error(transparent)]
    Kv(#[from] peren_node::KvImportError),
    #[error(transparent)]
    Storage(#[from] peren_node::StorageError),
    #[error(transparent)]
    DevVars(#[from] devvars::DevVarsError),
    #[error("kubernetes conformance failed: {0}")]
    Kubernetes(String),
    #[error("node health check failed for {name} at {address}: {reason}")]
    NodeHealth {
        name: String,
        address: SocketAddr,
        reason: String,
    },
    #[error("refusing to overwrite existing file {0:?}; pass --force to replace it")]
    Exists(std::path::PathBuf),
    #[error("failed to read {path:?}")]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write {path:?}")]
    Write {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to install the shutdown signal handler")]
    Signal(#[source] std::io::Error),
    #[error("failed to write the test-server readiness record")]
    ReadyOutput(#[source] std::io::Error),
    #[error("inherited listener {0:?} is unsupported in this build")]
    InheritedListenerUnsupported(String),
}

fn init(command: cli::Init) -> Result<(), CliError> {
    if command.output.exists() && !command.force {
        return Err(CliError::Exists(command.output));
    }
    let node = uuid::Uuid::new_v4();
    let worker = command.worker.display();
    let text = format!(
        r#"[node]
node_id = "{node}"
advertise_addr = "{peer}"
listen = "{peer}"

[bucket]
kind = "file"
path = "./data"

[mtls]
ca_cert_path = "./certs/ca.pem"
leaf_cert_path = "./certs/leaf.pem"
leaf_key_path = "./certs/leaf.key"

[[services]]
name = "{service}"
worker_bundle_path = "{worker}"
compatibility_date = "2026-01-01"

[[sockets]]
name = "public"
listen = "{public}"
service = "{service}"
"#,
        peer = command.peer_addr,
        public = command.public_addr,
        service = command.service,
    );
    write(&command.output, text.as_bytes())?;
    println!("created {}", command.output.display());
    Ok(())
}

fn config(command: cli::Config) -> Result<(), CliError> {
    match command {
        cli::Config::Validate { config } => {
            peren_config::FleetConfig::from_path(&config)?.validate()?;
            println!("valid {}", config.display());
        }
        cli::Config::Migrate(command) => migrate(command)?,
    }
    Ok(())
}

fn migrate(command: cli::Migrate) -> Result<(), CliError> {
    let progress = Progress::start(Style::Dots, "Migrating configuration");
    let receipt = peren_migrate::run(peren_migrate::Command {
        source: command.source,
        output: command.output,
        format: command.format.map(|format| match format {
            cli::MigrationSource::Wrangler => peren_migrate::Source::Wrangler,
            cli::MigrationSource::Sam => peren_migrate::Source::Sam,
            cli::MigrationSource::Serverless => peren_migrate::Source::Serverless,
        }),
        compatibility_date: command.compatibility_date,
    })?;
    progress.success("Configuration migrated");
    emit_notices(receipt.notices);
    if let Some(output) = receipt.stdout {
        print!("{output}");
    }
    Ok(())
}

fn write(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    std::fs::write(path, bytes).map_err(|source| CliError::Write {
        path: path.to_path_buf(),
        source,
    })
}

mod backup;
mod console;
mod credential;
mod deploy;
mod develop;
mod devvars;
mod health;
pub(crate) mod kubernetes;
mod secret;
mod server;
mod tail;
mod tenant;
mod trace;
mod upgrade;
mod workflow;

fn emit_notices(notices: Vec<peren_migrate::Notice>) {
    for notice in notices {
        eprintln!("warning: service {:?}: {}", notice.service, notice.warning);
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<(), CliError> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(CliError::Signal)?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(CliError::Signal),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<(), CliError> {
    tokio::signal::ctrl_c().await.map_err(CliError::Signal)
}
