use std::path::{Path, PathBuf};

use peren_config::{Binding, FleetConfig, Service};
use serde::Serialize;

use super::devvars::DevEnvironment;

pub(super) struct Session {
    pub(super) raw: FleetConfig,
    pub(super) environment: DevEnvironment,
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
    pub(super) fn load(config: &Path, wrangler: &[PathBuf]) -> Result<Self, super::CliError> {
        let imported = peren_migrate::load_raw(config, wrangler)?;
        super::emit_notices(imported.notices);
        let base = config
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let mut raw = imported.value;
        resolve_paths(&mut raw, &base);
        let environment = DevEnvironment::load(config)?;
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
}
