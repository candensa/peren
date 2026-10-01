use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use peren_config::{Binding, Entrypoint, FleetConfig, QueueConsumer, Service, Socket};
use peren_migrate::Source;
use peren_node::Environment;
use serde::Serialize;

use crate::cli;

use super::devvars::{LocalDevEnvironment, LocalEnvProfile};

pub(super) struct Session {
    pub(super) raw: FleetConfig,
    pub(super) environment: LocalDevEnvironment,
    base: PathBuf,
    source_profile: SourceProfile,
    env_profile: LocalEnvProfile,
    requested_environment: Option<String>,
}

#[derive(Serialize)]
pub(super) struct Topology {
    pub(super) source_profile: &'static str,
    pub(super) env_profile: &'static str,
    pub(super) environment: Option<String>,
    pub(super) data_dir: String,
    pub(super) bucket: String,
    pub(super) services: Vec<ServiceView>,
    pub(super) sockets: Vec<SocketView>,
    pub(super) queues: Vec<QueueView>,
    pub(super) diagnostics: Vec<Diagnostic>,
    pub(super) env_files: EnvFiles,
}

#[derive(Serialize)]
pub(super) struct ServiceView {
    pub(super) name: String,
    pub(super) worker: String,
    pub(super) entrypoint: String,
    pub(super) bindings: Vec<String>,
    pub(super) vars: usize,
    pub(super) secrets: usize,
}

#[derive(Serialize)]
pub(super) struct SocketView {
    pub(super) name: String,
    pub(super) service: String,
    pub(super) listen: String,
}

#[derive(Serialize)]
pub(super) struct QueueView {
    pub(super) service: String,
    pub(super) queue: String,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Diagnostic {
    pub(super) level: DiagnosticLevel,
    pub(super) message: String,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DiagnosticLevel {
    Info,
    Warning,
}

#[derive(Serialize)]
pub(super) struct EnvFiles {
    pub(super) loaded: Vec<String>,
    pub(super) ignored: Vec<String>,
    pub(super) filtered: usize,
}

impl Session {
    pub(super) fn load(command: &cli::Worker) -> Result<Self, super::CliError> {
        let source_profile = source_profile(command);
        let env_profile = env_profile(command);
        let imported = match source_profile {
            SourceProfile::Peren => peren_migrate::load_raw(&command.config, &command.wrangler)?,
            SourceProfile::Wrangler => {
                if !command.wrangler.is_empty() {
                    return Err(super::CliError::Unsupported(
                        "--profile wrangler does not accept --wrangler overlays",
                    ));
                }
                load_wrangler_config(&command.config)?
            }
        };
        super::emit_notices(imported.notices);
        let base = command
            .config
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let mut raw = imported.value;
        resolve_paths(&mut raw, &base);
        let environment = LocalDevEnvironment::load(
            &command.config,
            env_profile,
            command.environment.as_deref(),
        )?;
        Ok(Self {
            raw,
            environment,
            base,
            source_profile,
            env_profile,
            requested_environment: command.environment.clone(),
        })
    }

    pub(super) fn topology(&self) -> Topology {
        Topology {
            source_profile: source_profile_name(self.source_profile),
            env_profile: env_profile_name(self.env_profile),
            environment: self.requested_environment.clone(),
            data_dir: self.data_dir().display().to_string(),
            bucket: format!("{:?}", self.raw.bucket.kind).to_lowercase(),
            services: self
                .raw
                .services
                .iter()
                .map(service_view)
                .collect::<Vec<_>>(),
            sockets: self
                .raw
                .sockets
                .iter()
                .map(|socket| SocketView {
                    name: socket.name.clone(),
                    service: socket.service.clone(),
                    listen: socket.listen.clone(),
                })
                .collect(),
            queues: self
                .raw
                .services
                .iter()
                .flat_map(|service| {
                    service.consumes_queues.iter().map(|consumer| QueueView {
                        service: service.name.clone(),
                        queue: match consumer {
                            peren_config::QueueConsumer::Name(name) => name.clone(),
                            peren_config::QueueConsumer::Settings(settings) => {
                                settings.queue.clone()
                            }
                        },
                    })
                })
                .collect(),
            diagnostics: self.diagnostics(),
            env_files: self.env_files(),
        }
    }

    pub(super) fn print_topology(&self, json: bool) -> Result<(), super::CliError> {
        let topology = self.topology();
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&topology).map_err(super::CliError::Json)?
            );
            return Ok(());
        }
        println!("config: {}", self.base.display());
        println!(
            "dev: source={} env_profile={} data={} bucket={}",
            topology.source_profile, topology.env_profile, topology.data_dir, topology.bucket
        );
        if let Some(environment) = &topology.environment {
            println!("env: {environment}");
        }
        if !topology.env_files.loaded.is_empty() {
            println!("env files: {}", topology.env_files.loaded.join(", "));
        }
        if !topology.env_files.ignored.is_empty() {
            println!(
                "ignored env files: {}",
                topology.env_files.ignored.join(", ")
            );
        }
        if topology.env_files.filtered > 0 {
            println!(
                "filtered {} Wrangler tool variable{}",
                topology.env_files.filtered,
                suffix(topology.env_files.filtered)
            );
        }
        for service in &topology.services {
            println!(
                "service {} entrypoint={} worker={}",
                service.name, service.entrypoint, service.worker
            );
            if !service.bindings.is_empty() {
                println!("  bindings: {}", service.bindings.join(", "));
            }
            if service.vars > 0 || service.secrets > 0 {
                println!("  vars={} secrets={}", service.vars, service.secrets);
            }
        }
        for socket in &topology.sockets {
            println!(
                "socket {} service={} listen={}",
                socket.name, socket.service, socket.listen
            );
        }
        for queue in &topology.queues {
            println!("queue {} consumer={}", queue.queue, queue.service);
        }
        for diagnostic in &topology.diagnostics {
            println!(
                "{}: {}",
                match diagnostic.level {
                    DiagnosticLevel::Info => "info",
                    DiagnosticLevel::Warning => "warning",
                },
                diagnostic.message
            );
        }
        Ok(())
    }

    pub(super) fn data_dir(&self) -> PathBuf {
        self.environment
            .get("PEREN_DATA_DIR")
            .map_or_else(default_data_dir, PathBuf::from)
    }

    pub(super) fn tail_path(&self) -> PathBuf {
        self.data_dir().join("tail").join("events.jsonl")
    }

    fn env_files(&self) -> EnvFiles {
        let report = self.environment.report();
        EnvFiles {
            loaded: display_paths(&report.loaded),
            ignored: display_paths(&report.ignored),
            filtered: report.filtered,
        }
    }

    fn diagnostics(&self) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        diagnostics.push(Diagnostic::info(
            "dev uses loopback listeners with ephemeral ports and an isolated in-memory bucket",
        ));
        self.worker_diagnostics(&mut diagnostics);
        self.socket_diagnostics(&mut diagnostics);
        self.queue_diagnostics(&mut diagnostics);
        self.secret_diagnostics(&mut diagnostics);
        if self.environment.report().loaded.is_empty() {
            diagnostics.push(Diagnostic::info("no local env file was loaded"));
        }
        diagnostics
    }

    fn worker_diagnostics(&self, diagnostics: &mut Vec<Diagnostic>) {
        for service in &self.raw.services {
            if !service.worker_bundle_path.exists() {
                diagnostics.push(Diagnostic::warning(format!(
                    "service {} worker bundle does not exist: {}",
                    service.name,
                    service.worker_bundle_path.display()
                )));
            }
            for (name, path) in &service.additional_modules {
                if !path.exists() {
                    diagnostics.push(Diagnostic::warning(format!(
                        "service {} additional module {name} does not exist: {}",
                        service.name,
                        path.display()
                    )));
                }
            }
            if let Some(assets) = &service.assets
                && !assets.directory.exists()
            {
                diagnostics.push(Diagnostic::warning(format!(
                    "service {} assets directory does not exist: {}",
                    service.name,
                    assets.directory.display()
                )));
            }
        }
    }

    fn socket_diagnostics(&self, diagnostics: &mut Vec<Diagnostic>) {
        let public_services = self
            .raw
            .sockets
            .iter()
            .map(|socket| socket.service.as_str())
            .collect::<BTreeSet<_>>();
        for service in &self.raw.services {
            if !public_services.contains(service.name.as_str()) {
                diagnostics.push(Diagnostic::info(format!(
                    "service {} has no direct socket; it is reachable through bindings, queues, scheduled events, or Durable Objects",
                    service.name
                )));
            }
        }
    }

    fn queue_diagnostics(&self, diagnostics: &mut Vec<Diagnostic>) {
        let consumers = self
            .raw
            .services
            .iter()
            .flat_map(|service| service.consumes_queues.iter().map(queue_name))
            .collect::<BTreeSet<_>>();
        let producers = self
            .raw
            .services
            .iter()
            .flat_map(|service| {
                service
                    .bindings
                    .values()
                    .filter_map(|binding| match binding {
                        Binding::Queue { queue_name } => Some(queue_name.as_str()),
                        _ => None,
                    })
            })
            .collect::<BTreeSet<_>>();
        for queue in producers.difference(&consumers) {
            diagnostics.push(Diagnostic::warning(format!(
                "queue {queue} has a producer binding but no local consumer"
            )));
        }
        for queue in consumers.difference(&producers) {
            diagnostics.push(Diagnostic::info(format!(
                "queue {queue} has a local consumer but no producer binding in this fleet"
            )));
        }
    }

    fn secret_diagnostics(&self, diagnostics: &mut Vec<Diagnostic>) {
        for service in &self.raw.services {
            for (binding, secret) in &service.secrets {
                if let Some(name) = secret.env_variable()
                    && self.environment.get(name).is_none()
                {
                    diagnostics.push(Diagnostic::warning(format!(
                        "service {} secret {} expects missing env var {}",
                        service.name, binding, name
                    )));
                }
            }
            for (binding, config) in &service.bindings {
                for env in binding_env_vars(config) {
                    if self.environment.get(env).is_none() {
                        diagnostics.push(Diagnostic::warning(format!(
                            "service {} binding {} expects missing env var {}",
                            service.name, binding, env
                        )));
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SourceProfile {
    Peren,
    Wrangler,
}

impl Diagnostic {
    fn info(message: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Info,
            message: message.into(),
        }
    }

    fn warning(message: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Warning,
            message: message.into(),
        }
    }
}

pub(super) async fn follow_tail(path: PathBuf) {
    let mut offset = 0_u64;
    loop {
        if let Ok(metadata) = tokio::fs::metadata(&path).await {
            if metadata.len() < offset {
                offset = 0;
            }
            if metadata.len() > offset
                && let Ok(bytes) = read_from(&path, offset).await
            {
                offset = metadata.len();
                for line in String::from_utf8_lossy(&bytes)
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                {
                    if let Ok(event) = serde_json::from_str::<peren_node::TailLogEvent>(line) {
                        print_tail_event(&event);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn read_from(path: &Path, offset: u64) -> Result<Vec<u8>, std::io::Error> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

fn print_tail_event(event: &peren_node::TailLogEvent) {
    match event {
        peren_node::TailLogEvent::Request(event) => println!(
            "{} {} {} status={} outcome={} wall_time_ms={}{}{}{}",
            event.service,
            event.method,
            event.path,
            event.status,
            event.outcome,
            event.wall_time_ms,
            event
                .request_id
                .as_deref()
                .map_or(String::new(), |id| format!(" request_id={id}")),
            event
                .dispatch_id
                .as_deref()
                .map_or(String::new(), |id| format!(" dispatch_id={id}")),
            event
                .traceparent
                .as_deref()
                .map_or(String::new(), |id| format!(" traceparent={id}")),
        ),
        peren_node::TailLogEvent::Console(event) => println!(
            "{} console {:?} {}{}{}",
            event.service,
            event.level,
            event.message,
            event
                .cell
                .as_deref()
                .map_or(String::new(), |cell| format!(" cell={cell}")),
            event
                .request_id
                .as_deref()
                .map_or(String::new(), |id| format!(" request_id={id}")),
        ),
    }
}

fn display_paths(paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect()
}

fn default_data_dir() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("data")
}

fn suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn source_profile_name(profile: SourceProfile) -> &'static str {
    match profile {
        SourceProfile::Peren => "peren",
        SourceProfile::Wrangler => "wrangler",
    }
}

fn env_profile_name(profile: LocalEnvProfile) -> &'static str {
    match profile {
        LocalEnvProfile::Peren => "peren",
        LocalEnvProfile::Wrangler => "wrangler",
    }
}

fn source_profile(command: &cli::Worker) -> SourceProfile {
    match command.profile {
        Some(cli::DevProfile::Wrangler) => SourceProfile::Wrangler,
        Some(cli::DevProfile::Peren) | None => SourceProfile::Peren,
    }
}

fn env_profile(command: &cli::Worker) -> LocalEnvProfile {
    match command.profile {
        Some(cli::DevProfile::Peren) => LocalEnvProfile::Peren,
        None if command.wrangler.is_empty() => LocalEnvProfile::Peren,
        Some(cli::DevProfile::Wrangler) | None => LocalEnvProfile::Wrangler,
    }
}

fn load_wrangler_config(
    config: &Path,
) -> Result<peren_migrate::Imported<FleetConfig>, peren_migrate::CommandError> {
    let receipt = peren_migrate::run(peren_migrate::Command {
        source: config.to_path_buf(),
        output: None,
        format: Some(Source::Wrangler),
        compatibility_date: None,
    })?;
    let source = format!(
        "{}\n{}",
        local_dev_fleet_prelude(),
        receipt.stdout.unwrap_or_default()
    );
    let mut value = FleetConfig::from_toml(&source)?;
    if value.sockets.is_empty() && value.services.len() == 1 {
        value.sockets.push(Socket {
            name: "public".to_string(),
            listen: "127.0.0.1:8080".to_string(),
            service: value.services[0].name.clone(),
        });
    }
    Ok(peren_migrate::Imported {
        value,
        notices: receipt.notices,
    })
}

fn local_dev_fleet_prelude() -> &'static str {
    r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"

[bucket]
kind = "memory"

[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "leaf.key"
"#
}

fn service_view(service: &Service) -> ServiceView {
    let mut bindings = service
        .bindings
        .iter()
        .map(|(name, binding)| format!("{name}:{}", binding_kind(binding)))
        .collect::<Vec<_>>();
    bindings.sort();
    ServiceView {
        name: service.name.clone(),
        worker: service.worker_bundle_path.display().to_string(),
        entrypoint: entrypoint_name(&service.entrypoint).to_string(),
        bindings,
        vars: service.vars.len(),
        secrets: service.secrets.len() + service.secrets_store_refs.len(),
    }
}

fn entrypoint_name(entrypoint: &Entrypoint) -> &'static str {
    match entrypoint {
        Entrypoint::Stateless => "stateless",
        Entrypoint::DurableObject { .. } => "durable_object",
    }
}

fn binding_kind(binding: &Binding) -> &'static str {
    match binding {
        Binding::Ai { .. } => "ai",
        Binding::AnalyticsEngine { .. } => "analytics_engine",
        Binding::AwsSigv4 { .. } => "aws_sigv4",
        Binding::Container { .. } => "container",
        Binding::D1Database { .. } => "d1",
        Binding::Dispatcher { .. } => "dispatcher",
        Binding::DurableObjectNamespace { .. } => "durable_object_namespace",
        Binding::Hyperdrive { .. } => "hyperdrive",
        Binding::Images { .. } => "images",
        Binding::Kv { .. } => "kv",
        Binding::Loader => "loader",
        Binding::MtlsCertificate { .. } => "mtls_certificate",
        Binding::Queue { .. } => "queue",
        Binding::R2Bucket { .. } => "r2",
        Binding::RateLimiter { .. } => "rate_limiter",
        Binding::SecretsStoreSecret { .. } => "secret",
        Binding::Service { .. } => "service",
        Binding::Vectorize { .. } => "vectorize",
        Binding::Outbound { .. } => "outbound",
        Binding::Workflow { .. } => "workflow",
        Binding::Assets => "assets",
    }
}

fn queue_name(consumer: &QueueConsumer) -> &str {
    match consumer {
        QueueConsumer::Name(name) => name,
        QueueConsumer::Settings(settings) => &settings.queue,
    }
}

fn binding_env_vars(binding: &Binding) -> Vec<&str> {
    match binding {
        Binding::R2Bucket {
            access_key_env,
            secret_key_env,
            token_env,
            ..
        }
        | Binding::AwsSigv4 {
            access_key_env,
            secret_key_env,
            token_env,
            ..
        } => credential_env_vars(
            access_key_env.as_ref(),
            secret_key_env.as_ref(),
            token_env.as_ref(),
        ),
        Binding::Vectorize { provider, .. } => match provider {
            peren_config::VectorProvider::Http { token_env, .. }
            | peren_config::VectorProvider::Qdrant {
                api_key_env: token_env,
                ..
            }
            | peren_config::VectorProvider::Weaviate {
                api_key_env: token_env,
                ..
            } => token_env.iter().map(String::as_str).collect(),
            peren_config::VectorProvider::Pinecone { api_key_env, .. } => vec![api_key_env],
            peren_config::VectorProvider::Local => Vec::new(),
        },
        Binding::MtlsCertificate {
            cert_pem_env,
            key_pem_env,
        } => vec![cert_pem_env, key_pem_env],
        Binding::Ai { provider, .. } => match provider {
            peren_config::AiProvider::OpenAi { api_key_env, .. }
            | peren_config::AiProvider::Anthropic { api_key_env, .. }
            | peren_config::AiProvider::Gemini { api_key_env, .. } => vec![api_key_env],
            peren_config::AiProvider::WorkersAi {
                account_id_env,
                api_token_env,
            } => vec![account_id_env, api_token_env],
            peren_config::AiProvider::Http | peren_config::AiProvider::Local { .. } => Vec::new(),
        },
        Binding::D1Database { backend, .. } => match backend {
            peren_config::D1Backend::Turso {
                url_env, token_env, ..
            } => vec![url_env, token_env],
            peren_config::D1Backend::External { url_env, .. } => vec![url_env],
            peren_config::D1Backend::NativeSqlite => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn credential_env_vars<'a>(
    access_key_env: Option<&'a String>,
    secret_key_env: Option<&'a String>,
    token_env: Option<&'a String>,
) -> Vec<&'a str> {
    [access_key_env, secret_key_env, token_env]
        .into_iter()
        .flatten()
        .map(String::as_str)
        .collect()
}

fn resolve_paths(config: &mut FleetConfig, base: &Path) {
    absolutize_option(&mut config.node.identity_key_path, base);
    absolutize_option(&mut config.bucket.path, base);
    absolutize(&mut config.mtls.ca_cert_path, base);
    absolutize(&mut config.mtls.leaf_cert_path, base);
    absolutize(&mut config.mtls.leaf_key_path, base);
    if let Some(console) = &mut config.console {
        absolutize(&mut console.data_dir, base);
    }
    if let Some(queues) = &mut config.queues {
        absolutize_string_option(&mut queues.file_path, base);
        absolutize_string_option(&mut queues.cell_path, base);
    }
    for namespace in &mut config.dispatch_namespaces {
        for script in &mut namespace.scripts {
            absolutize(&mut script.worker_bundle_path, base);
        }
    }
    for service in &mut config.services {
        resolve_service(service, base);
    }
}

fn resolve_service(service: &mut Service, base: &Path) {
    absolutize(&mut service.worker_bundle_path, base);
    for path in service.additional_modules.values_mut() {
        absolutize(path, base);
    }
    for path in service.source_maps.values_mut() {
        absolutize(path, base);
    }
    if let Some(assets) = &mut service.assets {
        absolutize(&mut assets.directory, base);
    }
    let _ = service;
}

fn absolutize_option(path: &mut Option<PathBuf>, base: &Path) {
    if let Some(path) = path {
        absolutize(path, base);
    }
}

fn absolutize_string_option(path: &mut Option<String>, base: &Path) {
    if let Some(path) = path
        && !path.contains("://")
    {
        let candidate = PathBuf::from(path.as_str());
        if candidate.is_relative() {
            *path = base.join(candidate).display().to_string();
        }
    }
}

fn absolutize(path: &mut PathBuf, base: &Path) {
    if path.is_relative() {
        *path = base.join(&path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn resolves_topology_paths_relative_to_config() {
        let base = PathBuf::from("/workspace/app");
        let mut config = FleetConfig::from_toml(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
identity_key_path = "keys/node.pem"
[bucket]
kind = "file"
path = "data"
[mtls]
ca_cert_path = "certs/ca.pem"
leaf_cert_path = "certs/leaf.pem"
leaf_key_path = "certs/leaf.key"
[[services]]
name = "api"
worker_bundle_path = "src/worker.js"
compatibility_date = "2026-01-01"
[services.bindings.CACHE]
type = "kv"
namespace = "cache"
unique_key = "cache"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
        )
        .unwrap();

        resolve_paths(&mut config, &base);

        assert_eq!(config.services.len(), 1);
        assert_eq!(
            config.node.identity_key_path.unwrap(),
            base.join("keys/node.pem")
        );
        assert_eq!(config.bucket.path.unwrap(), base.join("data"));
        assert_eq!(config.mtls.ca_cert_path, base.join("certs/ca.pem"));
        assert_eq!(
            config.services[0].worker_bundle_path,
            base.join("src/worker.js")
        );
    }

    #[test]
    fn wrangler_profile_adds_default_dev_socket_for_single_service() {
        let temp = temp_dir();
        let wrangler = temp.join("wrangler.toml");
        fs::write(
            &wrangler,
            r#"
name = "api"
main = "worker.js"
compatibility_date = "2026-01-01"
"#,
        )
        .unwrap();

        let imported = load_wrangler_config(&wrangler).unwrap();

        assert_eq!(imported.value.services.len(), 1);
        assert_eq!(imported.value.sockets.len(), 1);
        assert_eq!(imported.value.sockets[0].name, "public");
        assert_eq!(imported.value.sockets[0].service, "api");
    }

    #[test]
    fn wrangler_profile_rejects_overlay_configs() {
        let command = cli::Worker {
            config: PathBuf::from("wrangler.toml"),
            wrangler: vec![PathBuf::from("overlay.toml")],
            profile: Some(cli::DevProfile::Wrangler),
            environment: None,
            json: false,
        };

        let error = match Session::load(&command) {
            Ok(_) => panic!("expected --profile wrangler with overlays to fail"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("--profile wrangler does not accept --wrangler overlays"));
    }

    #[test]
    fn topology_reports_dev_session_context_and_diagnostics() {
        let temp = temp_dir();
        let fleet = temp.join("fleet.toml");
        fs::write(
            &fleet,
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "file"
path = "data"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "missing-worker.js"
compatibility_date = "2026-01-01"
secrets = { TOKEN = "API_TOKEN" }
[services.bindings.JOBS]
type = "queue"
queue_name = "jobs"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
        )
        .unwrap();
        fs::write(
            temp.join(".dev.vars"),
            format!("PEREN_DATA_DIR={}\n", temp.join("state").display()),
        )
        .unwrap();
        let command = cli::Worker {
            config: fleet,
            wrangler: Vec::new(),
            profile: Some(cli::DevProfile::Peren),
            environment: None,
            json: false,
        };

        let session = Session::load(&command).unwrap();
        let topology = session.topology();

        assert_eq!(topology.source_profile, "peren");
        assert_eq!(topology.env_profile, "peren");
        assert_eq!(topology.data_dir, temp.join("state").display().to_string());
        assert_eq!(topology.env_files.loaded.len(), 1);
        assert!(
            topology
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("worker bundle does not exist"))
        );
        assert!(
            topology
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("missing env var API_TOKEN"))
        );
        assert!(
            topology
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("no local consumer"))
        );
    }

    fn temp_dir() -> PathBuf {
        for attempt in 0..100 {
            let name = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("peren-develop-{name}-{attempt}"));
            if fs::create_dir(&path).is_ok() {
                return path;
            }
        }
        panic!("failed to create temporary develop directory");
    }
}
