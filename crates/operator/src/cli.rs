use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "peren", version, about = "Run and operate a Peren fleet")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Init(Init),
    Config {
        #[command(subcommand)]
        command: Config,
    },
    Conformance {
        #[command(subcommand)]
        command: Conformance,
    },
    Doctor(Doctor),
    Status(Status),
    Node {
        #[command(subcommand)]
        command: Node,
    },
    Serve(Server),
    Dev(Worker),
    #[command(name = "test-server")]
    TestServer(Worker),
    Devcert {
        output_dir: PathBuf,
    },
    Credential {
        #[command(subcommand)]
        command: Credential,
    },
    Deploy(Deploy),
    Rollback(Rollback),
    Backup(Backup),
    Restore(Restore),
    Uninstall(Uninstall),
    Upgrade {
        #[command(subcommand)]
        command: Upgrade,
    },
    D1 {
        #[command(subcommand)]
        command: D1,
    },
    Diagnose(Diagnose),
    Logs(Tail),
    Tail(Tail),
    Trace(Trace),
    Tenant {
        #[command(subcommand)]
        command: Tenant,
    },
    Secrets {
        #[command(subcommand)]
        command: Secrets,
    },
    Workflow {
        #[command(subcommand)]
        command: Workflow,
    },
    Console {
        #[command(subcommand)]
        command: Console,
    },
    Migrate(Migrate),
    Kv {
        #[command(subcommand)]
        command: Kv,
    },
    Queue {
        #[command(subcommand)]
        command: Queue,
    },
    Kubernetes {
        #[command(subcommand)]
        command: Kubernetes,
    },
}

#[derive(Debug, Args)]
pub struct Init {
    #[arg(long, default_value = "fleet.toml")]
    pub output: PathBuf,
    #[arg(long, default_value = "api")]
    pub service: String,
    #[arg(long, default_value = "127.0.0.1:7000")]
    pub peer_addr: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub public_addr: String,
    #[arg(long, default_value = "worker.js")]
    pub worker: PathBuf,
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Subcommand)]
pub enum Config {
    Validate { config: PathBuf },
    Migrate(Migrate),
}

#[derive(Debug, Subcommand)]
pub enum Conformance {
    Storage { config: PathBuf },
}

#[derive(Debug, Args)]
pub struct Doctor {
    pub config: PathBuf,
    #[arg(long)]
    pub storage_test: bool,
    #[arg(long, requires = "storage_test")]
    pub read_only: bool,
}

#[derive(Debug, Args)]
pub struct Status {
    pub config: PathBuf,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub storage_test: bool,
}

#[derive(Debug, Subcommand)]
pub enum Node {
    Health {
        config: PathBuf,
    },
    Join {
        config: PathBuf,
        #[arg(long)]
        key_dir: PathBuf,
        #[arg(long)]
        token: String,
    },
    Drain {
        config: PathBuf,
        #[arg(long)]
        node: uuid::Uuid,
        #[arg(long)]
        reason: Option<String>,
    },
    Remove {
        config: PathBuf,
        #[arg(long)]
        node: uuid::Uuid,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Args)]
pub struct Server {
    pub config: PathBuf,
    #[arg(long)]
    pub wrangler: Vec<PathBuf>,
    #[arg(long = "socket-fd", value_parser = parse_socket_fd)]
    pub socket_fds: Vec<SocketFd>,
}

#[derive(Debug, Args)]
pub struct Worker {
    pub config: PathBuf,
    #[arg(long)]
    pub wrangler: Vec<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Debug)]
pub struct SocketFd {
    pub name: String,
    pub fd: i32,
}

fn parse_socket_fd(value: &str) -> Result<SocketFd, String> {
    let (name, fd) = value.split_once('=').ok_or("expected <name>=<fd>")?;
    if name.is_empty() {
        return Err("socket name cannot be empty".into());
    }
    let fd = fd
        .parse()
        .map_err(|_| "file descriptor must be an integer")?;
    if fd < 0 {
        return Err("file descriptor cannot be negative".into());
    }
    Ok(SocketFd {
        name: name.into(),
        fd,
    })
}

#[derive(Debug, Subcommand)]
pub enum Credential {
    Mint {
        key_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        bucket_prefix: String,
        #[arg(long)]
        scope: String,
        #[arg(long, value_delimiter = ',')]
        scopes: Vec<String>,
    },
    Node {
        key_dir: PathBuf,
        #[arg(long)]
        cluster: String,
        #[arg(long)]
        node: uuid::Uuid,
        #[arg(long)]
        peer_addr: String,
    },
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
pub struct Deploy {
    pub config: Option<PathBuf>,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u8).range(0..=100))]
    pub percent: u8,
    #[arg(long)]
    pub preview: bool,
    #[arg(long)]
    pub skip_verify: bool,
    #[command(subcommand)]
    pub command: Option<DeployCommand>,
}

#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    Health {
        config: PathBuf,
        #[arg(long)]
        service: Option<String>,
    },
    List {
        config: PathBuf,
        #[arg(long)]
        service: Option<String>,
    },
    Verify {
        config: PathBuf,
        #[arg(long)]
        service: Option<String>,
    },
    Prune {
        config: PathBuf,
        #[arg(long)]
        service: String,
        #[arg(long, default_value_t = 5)]
        keep: usize,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Args)]
pub struct Rollback {
    pub config: PathBuf,
    #[arg(long)]
    pub service: String,
    #[arg(long = "to")]
    pub digest: Option<String>,
    #[arg(long)]
    pub skip_verify: bool,
}

#[derive(Debug, Args)]
pub struct Backup {
    pub config: PathBuf,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct Restore {
    pub config: PathBuf,
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct Uninstall {
    pub config: PathBuf,
    #[arg(long)]
    pub force: bool,
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Subcommand)]
pub enum Upgrade {
    Check {
        config: PathBuf,
        #[arg(long)]
        target: Option<String>,
    },
    Plan {
        config: PathBuf,
        #[arg(long)]
        target: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum D1 {
    Restore(D1Restore),
    #[command(name = "prune-history")]
    PruneHistory {
        #[command(flatten)]
        database: Database,
        #[arg(long)]
        retention_days: Option<u64>,
        #[arg(long)]
        dry_run: bool,
    },
    Migrate {
        #[command(flatten)]
        database: Database,
        #[arg(long)]
        dir: PathBuf,
    },
    Query {
        #[command(flatten)]
        database: Database,
        #[arg(long)]
        sql: String,
    },
}

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("target").required(true).multiple(false).args(["to", "bookmark"])))]
pub struct D1Restore {
    #[command(flatten)]
    pub database: Database,
    #[arg(long)]
    pub into: String,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long)]
    pub bookmark: Option<String>,
}

#[derive(Debug, Args)]
pub struct Database {
    pub config: PathBuf,
    #[arg(long)]
    pub service: String,
    #[arg(long)]
    pub binding: String,
}

#[derive(Debug, Args)]
pub struct Diagnose {
    pub config: PathBuf,
    #[arg(long)]
    pub storage_test: bool,
    #[arg(long, requires = "storage_test")]
    pub read_only: bool,
    #[arg(long, requires = "storage_test")]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum Queue {
    Depth {
        config: PathBuf,
        #[arg(long)]
        queue: String,
    },
    Pause {
        config: PathBuf,
        #[arg(long)]
        queue: String,
    },
    Resume {
        config: PathBuf,
        #[arg(long)]
        queue: String,
    },
    Purge {
        config: PathBuf,
        #[arg(long)]
        queue: String,
    },
    Redrive {
        config: PathBuf,
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TailLevel {
    Log,
    Warn,
    Error,
}

#[derive(Debug, Args)]
pub struct Tail {
    pub config: PathBuf,
    #[arg(long)]
    pub service: String,
    #[arg(long)]
    pub level: Option<TailLevel>,
    #[arg(long)]
    pub node: Option<String>,
}

#[derive(Debug, Args)]
pub struct Trace {
    pub config: PathBuf,
    #[arg(long)]
    pub service: Option<String>,
    #[arg(long = "trace-id")]
    pub id: Option<String>,
    #[arg(long)]
    pub request_id: Option<String>,
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum Tenant {
    Revoke {
        config: PathBuf,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        node: Option<String>,
    },
    Delete {
        config: PathBuf,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        node: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum Secrets {
    Put {
        config: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        value: String,
        #[arg(long)]
        node: Option<String>,
    },
    Rotate {
        config: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        value: String,
        #[arg(long)]
        node: Option<String>,
    },
    List {
        config: PathBuf,
        #[arg(long)]
        node: Option<String>,
    },
    Get {
        config: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        node: Option<String>,
    },
    Delete {
        config: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        node: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum Workflow {
    Delete(WorkflowId),
    Status(WorkflowId),
    Cancel {
        #[command(flatten)]
        workflow: WorkflowId,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Debug, Args)]
pub struct WorkflowId {
    pub config: PathBuf,
    #[arg(long)]
    pub service: String,
    #[arg(long)]
    pub binding: String,
    #[arg(long)]
    pub instance_id: String,
}

#[derive(Debug, Subcommand)]
pub enum Console {
    Bootstrap {
        config: PathBuf,
        #[arg(long)]
        workspace_name: Option<String>,
    },
    Register {
        config: PathBuf,
        #[arg(long)]
        token: String,
        #[arg(long)]
        email: String,
        #[arg(long)]
        name: String,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum MigrationSource {
    Wrangler,
    Sam,
    Serverless,
}

#[derive(Debug, Args)]
pub struct Migrate {
    pub source: PathBuf,
    pub output: Option<PathBuf>,
    #[arg(long = "from")]
    pub format: Option<MigrationSource>,
    #[arg(long)]
    pub compatibility_date: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum Kubernetes {
    Render(KubernetesRender),
    Check(KubernetesCheck),
}

#[derive(Debug, Args)]
pub struct KubernetesCheck {
    pub config: PathBuf,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub replicas: u16,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct KubernetesRender {
    pub config: PathBuf,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long, default_value = "ghcr.io/candensa/peren")]
    pub image: String,
    #[arg(long, default_value = "0.1.0")]
    pub tag: String,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub replicas: u16,
    #[arg(long, default_value = "10Gi")]
    pub storage: String,
    #[arg(long = "service-account-annotation", value_parser = parse_key_value)]
    pub service_account_annotations: Vec<KeyValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

fn parse_key_value(value: &str) -> Result<KeyValue, String> {
    let (key, value) = value.split_once('=').ok_or("expected <key>=<value>")?;
    if key.trim().is_empty() {
        return Err("key cannot be empty".into());
    }
    Ok(KeyValue {
        key: key.into(),
        value: value.into(),
    })
}

#[derive(Debug, Subcommand)]
pub enum Kv {
    #[command(name = "bulk-import")]
    BulkImport {
        config: PathBuf,
        #[arg(long)]
        service: String,
        #[arg(long)]
        binding: String,
        #[arg(long)]
        file: PathBuf,
    },
}
