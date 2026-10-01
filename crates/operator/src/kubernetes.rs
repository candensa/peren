use peren_config::{BucketKind, CredentialsSource, FleetConfig};
use serde::Serialize;

pub use crate::cli::KeyValue;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Plan {
    pub fleet: PerenFleet,
    pub values: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PerenFleet {
    pub name: String,
    pub replicas: u16,
    pub image: String,
    pub tag: String,
    pub admission: AdmissionMode,
    pub services: Vec<PerenService>,
    pub storage: PerenStorageReservation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PerenService {
    pub name: String,
    pub worker_bundle: String,
    pub sockets: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PerenNodeRetirement {
    pub node: String,
    pub admission: AdmissionMode,
    pub reason: Option<String>,
    pub disk_removal_safe: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PerenStorageReservation {
    pub size: String,
    pub durable_authority: DurableAuthority,
    pub disk_removal_safe: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableAuthority {
    ObjectStore,
    Local,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionMode {
    Serving,
    Draining,
    ControlOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    pub image: String,
    pub tag: String,
    pub replicas: u16,
    pub storage: String,
    pub annotations: Vec<KeyValue>,
    pub config: String,
}

pub trait Backend {
    fn plan(&self, config: &FleetConfig, options: Options) -> Plan;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HelmBackend;

impl Backend for HelmBackend {
    fn plan(&self, config: &FleetConfig, options: Options) -> Plan {
        Plan {
            fleet: PerenFleet::from_config(config, &options),
            values: Values::from_options(options).render(),
        }
    }
}

impl PerenFleet {
    fn from_config(config: &FleetConfig, options: &Options) -> Self {
        Self {
            name: "peren".into(),
            replicas: options.replicas,
            image: options.image.clone(),
            tag: options.tag.clone(),
            admission: AdmissionMode::Serving,
            services: config
                .services
                .iter()
                .map(|service| PerenService {
                    name: service.name.clone(),
                    worker_bundle: service.worker_bundle_path.display().to_string(),
                    sockets: config
                        .sockets
                        .iter()
                        .filter(|socket| socket.service == service.name)
                        .map(|socket| socket.name.clone())
                        .collect(),
                })
                .collect(),
            storage: PerenStorageReservation {
                size: options.storage.clone(),
                durable_authority: if matches!(config.bucket.kind, BucketKind::S3) {
                    DurableAuthority::ObjectStore
                } else {
                    DurableAuthority::Local
                },
                disk_removal_safe: false,
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CheckReport {
    pub replicas: u16,
    pub problems: Vec<Finding>,
    pub warnings: Vec<Finding>,
}

#[derive(Debug, Serialize)]
pub struct Finding {
    pub field: String,
    pub message: String,
}

impl Finding {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

impl CheckReport {
    #[must_use]
    pub fn new(config: &FleetConfig, replicas: u16) -> Self {
        let mut report = Self {
            replicas,
            problems: Vec::new(),
            warnings: Vec::new(),
        };
        report.check_storage(config);
        report.check_listeners(config);
        report.check_credentials(config);
        report
    }

    fn check_storage(&mut self, config: &FleetConfig) {
        if self.replicas > 1 && matches!(config.bucket.kind, BucketKind::Memory | BucketKind::File)
        {
            self.problems.push(Finding::new(
                "bucket.kind",
                "multi-replica Kubernetes fleets require S3-compatible durable authority; memory and file buckets are local to one pod",
            ));
        }
    }

    fn check_listeners(&mut self, config: &FleetConfig) {
        check_address(&mut self.problems, "node.listen", &config.node.listen);
        for (index, socket) in config.sockets.iter().enumerate() {
            check_address(
                &mut self.problems,
                format!("sockets[{index}].listen"),
                &socket.listen,
            );
        }
    }

    fn check_credentials(&mut self, config: &FleetConfig) {
        if matches!(config.bucket.kind, BucketKind::S3)
            && matches!(
                config.bucket.credentials_source,
                CredentialsSource::Configured | CredentialsSource::Environment
            )
        {
            self.warnings.push(Finding::new(
                "bucket.credentials_source",
                "prefer workload identity, IRSA, or pod identity for Kubernetes so provider credentials stay host-owned",
            ));
        }
    }
}

fn check_address(problems: &mut Vec<Finding>, field: impl Into<String>, value: &str) {
    let field = field.into();
    let Ok(address) = value.parse::<std::net::SocketAddr>() else {
        problems.push(Finding::new(field, "must be an IP socket address"));
        return;
    };
    if address.ip().is_loopback() {
        problems.push(Finding::new(
            field,
            "must not bind to loopback inside Kubernetes; use 0.0.0.0 or the pod address",
        ));
    }
}

pub struct Values {
    pub image: String,
    pub tag: String,
    pub replicas: u16,
    pub storage: String,
    pub annotations: Vec<KeyValue>,
    pub config: String,
}

impl Values {
    fn from_options(options: Options) -> Self {
        Self {
            image: options.image,
            tag: options.tag,
            replicas: options.replicas,
            storage: options.storage,
            annotations: options.annotations,
            config: options.config,
        }
    }

    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(
            "# Generated by `peren kubernetes render`. Review before applying with Helm.\n",
        );
        line(&mut out, 0, "replicaCount", self.replicas);
        out.push_str("\nimage:\n");
        line(&mut out, 2, "repository", quote(&self.image));
        line(&mut out, 2, "tag", quote(&self.tag));
        line(&mut out, 2, "pullPolicy", "IfNotPresent");
        out.push_str("\nserviceAccount:\n");
        line(&mut out, 2, "create", true);
        line(&mut out, 2, "automount", true);
        out.push_str("  annotations:");
        if self.annotations.is_empty() {
            out.push_str(" {}\n");
        } else {
            out.push('\n');
            for item in &self.annotations {
                line(&mut out, 4, &item.key, quote(&item.value));
            }
        }
        out.push_str("\nstorage:\n");
        line(&mut out, 2, "size", quote(&self.storage));
        out.push_str("\nconfig:\n");
        line(&mut out, 2, "existingSecret", quote(""));
        line(&mut out, 2, "fileName", quote("config.toml"));
        out.push_str("  inline: |\n");
        for line_text in self.config.lines() {
            out.push_str("    ");
            out.push_str(line_text);
            out.push('\n');
        }
        if self.config.ends_with('\n') {
            out.push('\n');
        }
        out
    }
}

fn line(out: &mut String, indent: usize, key: &str, value: impl std::fmt::Display) {
    out.push_str(&" ".repeat(indent));
    out.push_str(key);
    out.push_str(": ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).expect("yaml string quoting uses JSON-compatible scalar quoting")
}
