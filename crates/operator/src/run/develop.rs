use std::path::{Path, PathBuf};

use peren_config::{Binding, FleetConfig, Service, Socket};
use peren_migrate::Source;
use serde::Serialize;

use crate::cli;

use super::devvars::{LocalDevEnvironment, LocalEnvProfile};

pub(super) struct Session {
    pub(super) raw: FleetConfig,
    pub(super) environment: LocalDevEnvironment,
    base: PathBuf,
}

#[derive(Serialize)]
pub(super) struct Topology {
    pub(super) services: Vec<ServiceView>,
    pub(super) sockets: Vec<SocketView>,
    pub(super) queues: Vec<QueueView>,
}

#[derive(Serialize)]
pub(super) struct ServiceView {
    pub(super) name: String,
    pub(super) worker: String,
    pub(super) bindings: Vec<String>,
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
        })
    }

    pub(super) fn topology(&self) -> Topology {
        Topology {
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
        for service in topology.services {
            println!("service {} worker={}", service.name, service.worker);
            if !service.bindings.is_empty() {
                println!("  bindings: {}", service.bindings.join(", "));
            }
        }
        for socket in topology.sockets {
            println!(
                "socket {} service={} listen={}",
                socket.name, socket.service, socket.listen
            );
        }
        for queue in topology.queues {
            println!("queue {} consumer={}", queue.queue, queue.service);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum SourceProfile {
    Peren,
    Wrangler,
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
        bindings,
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
