use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use peren_config::{Cache, DispatchNamespace, R2EventType, Service, ValidatedConfig};
use peren_primitives::NodeId;
use peren_provider_object_store::{R2Store, S3Credentials, S3Options};
use peren_runtime::{WorkerBundle, WorkerEnvironment};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    Environment, Node, Providers, Repository, Supervisor, TaskError,
    admission::Admission,
    asset::AssetConfig,
    bundle,
    host::{CacheStore, KvStore},
    metrics::{Metrics, Telemetry, now_ms},
    websocket::Registry as WebSocketRegistry,
};

mod aws;
mod drain;

use drain::{QueueConsumerPlan, queue_consumers, spawn_queue_consumers};

pub struct Process {
    supervisor: Supervisor,
    pub(super) readiness: Arc<AtomicBool>,
    listeners: BTreeMap<String, SocketAddr>,
    shutdown_deadline: Duration,
    _providers: Providers,
}

#[derive(Clone)]
pub(super) struct App {
    pub(super) node: NodeId,
    pub(super) incarnation: Uuid,
    pub(super) config: Arc<ValidatedConfig>,
    pub(super) data: PathBuf,
    pub(super) readiness: Arc<AtomicBool>,
    pub(super) retired: Arc<AtomicBool>,
    pub(super) metrics: Metrics,
    pub(super) admission: Admission,
    pub(super) socket: Option<SocketApp>,
}

#[derive(Clone)]
pub(super) struct SocketApp {
    pub(super) node: Node<Repository>,
    pub(super) bundle: WorkerBundle,
    pub(super) service: Arc<str>,
    pub(super) limits: Limits,
    tail: PathBuf,
    pub(super) environment: WorkerEnvironment,
    d1: BTreeMap<String, D1Route>,
    r2: BTreeMap<String, Arc<R2Store>>,
    r2_notifications: Vec<R2NotificationRule>,
    kv: BTreeMap<String, KvStore>,
    outbound_hosts: Arc<BTreeSet<String>>,
    aws: Arc<BTreeMap<String, AwsBinding>>,
    mtls: Arc<BTreeMap<String, crate::tls::Identity>>,
    queues: QueueBrokerState,
    cache: CacheStore,
    consumers: Vec<QueueConsumerPlan>,
    assets: Option<AssetConfig>,
    pub(super) services: Arc<BTreeMap<String, ServiceTarget>>,
    pub(super) objects: Arc<BTreeMap<String, String>>,
    pub(super) registry: ObjectRegistry,
    pub(super) websocket_sessions: Arc<WebSocketRegistry>,
    telemetry: Arc<Telemetry>,
    pub(super) admission: Admission,
}

#[derive(Clone)]
pub(super) struct ServiceTarget {
    pub(super) bundle: WorkerBundle,
    pub(super) environment: WorkerEnvironment,
    d1: BTreeMap<String, D1Route>,
    r2: BTreeMap<String, Arc<R2Store>>,
    r2_notifications: Vec<R2NotificationRule>,
    kv: BTreeMap<String, KvStore>,
    outbound_hosts: Arc<BTreeSet<String>>,
    aws: Arc<BTreeMap<String, AwsBinding>>,
    mtls: Arc<BTreeMap<String, crate::tls::Identity>>,
}

pub(super) type ObjectRegistry = Arc<StdMutex<BTreeMap<String, BTreeSet<String>>>>;

#[derive(Clone)]
struct D1Route {
    url: Arc<str>,
    token: Arc<str>,
    replica: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct R2NotificationRule {
    pub(super) bucket: String,
    pub(super) queue: String,
    pub(super) events: Vec<R2EventType>,
    pub(super) prefix: Option<String>,
    pub(super) suffix: Option<String>,
}

#[derive(Clone)]
pub(super) struct AwsBinding {
    pub(super) credential: AwsCredential,
    pub(super) region: String,
    pub(super) service: String,
    pub(super) hosts: BTreeSet<String>,
}

#[derive(Clone)]
pub(super) enum AwsCredential {
    Static {
        access: Arc<str>,
        secret: Arc<str>,
        token: Option<Arc<str>>,
    },
    DefaultChain,
}

struct SocketContext<'a> {
    node: NodeId,
    data: &'a Path,
    repository: Repository,
    limits: Limits,
    environments: &'a BTreeMap<String, WorkerEnvironment>,
    d1: &'a BTreeMap<String, BTreeMap<String, D1Route>>,
    r2: &'a BTreeMap<String, BTreeMap<String, Arc<R2Store>>>,
    r2_notifications: &'a BTreeMap<String, Vec<R2NotificationRule>>,
    kv: &'a BTreeMap<String, BTreeMap<String, KvStore>>,
    outbound_hosts: &'a BTreeMap<String, Arc<BTreeSet<String>>>,
    aws: &'a BTreeMap<String, Arc<BTreeMap<String, AwsBinding>>>,
    mtls: &'a BTreeMap<String, Arc<BTreeMap<String, crate::tls::Identity>>>,
    queues: QueueBrokerState,
    cache: CacheStore,
    consumers: &'a BTreeMap<String, Vec<QueueConsumerPlan>>,
    assets: &'a BTreeMap<String, AssetConfig>,
    pub(super) services: Arc<BTreeMap<String, ServiceTarget>>,
    pub(super) objects: Arc<BTreeMap<String, String>>,
    pub(super) registry: ObjectRegistry,
    telemetry: Arc<Telemetry>,
    admission: Admission,
}

mod queue;
pub(crate) use queue::QueueBrokerState;
pub(crate) use queue::queue_broker;
use queue::queue_provider_metadata;

struct ProcessContext {
    node: NodeId,
    data: PathBuf,
    repository: Repository,
    limits: Limits,
    telemetry: Arc<Telemetry>,
    admission: Admission,
    environments: BTreeMap<String, WorkerEnvironment>,
    d1: BTreeMap<String, BTreeMap<String, D1Route>>,
    r2: BTreeMap<String, BTreeMap<String, Arc<R2Store>>>,
    r2_notifications: BTreeMap<String, Vec<R2NotificationRule>>,
    kv: BTreeMap<String, BTreeMap<String, KvStore>>,
    outbound_hosts: BTreeMap<String, Arc<BTreeSet<String>>>,
    aws: BTreeMap<String, Arc<BTreeMap<String, AwsBinding>>>,
    mtls: BTreeMap<String, Arc<BTreeMap<String, crate::tls::Identity>>>,
    queues: QueueBrokerState,
    cache: CacheStore,
    consumers: BTreeMap<String, Vec<QueueConsumerPlan>>,
    assets: BTreeMap<String, AssetConfig>,
}

#[derive(Clone, Copy)]
pub(super) struct Limits {
    body: u64,
    body_limit: usize,
    pub(super) heap: usize,
    pub(super) execution: Duration,
    subrequests: u32,
}

fn reject_unknown_inherited(
    config: &ValidatedConfig,
    inherited: &BTreeMap<String, std::net::TcpListener>,
) -> Result<(), ProcessError> {
    let expected = config
        .sockets
        .keys()
        .cloned()
        .chain(std::iter::once("peer".to_string()))
        .chain(config.console_listen.map(|_| "console".to_string()))
        .collect::<BTreeSet<_>>();
    if let Some(name) = inherited.keys().find(|name| !expected.contains(*name)) {
        return Err(ProcessError::UnknownInheritedListener(name.clone()));
    }
    Ok(())
}

impl Process {
    pub async fn start<E: Environment>(
        config: ValidatedConfig,
        environment: &E,
    ) -> Result<Self, ProcessError> {
        Self::start_with_listeners(config, environment, BTreeMap::new()).await
    }

    pub async fn start_with_listeners<E: Environment>(
        config: ValidatedConfig,
        environment: &E,
        inherited: BTreeMap<String, std::net::TcpListener>,
    ) -> Result<Self, ProcessError> {
        let providers = Providers::build(&config, environment).await?;
        let data = environment
            .get("PEREN_DATA_DIR")
            .map_or_else(default_data, PathBuf::from);
        let bundles = bundles(&config.raw.services, &config.raw.dispatch_namespaces)?;
        Self::start_with_providers(config, providers, inherited, data, bundles).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "process startup wires listeners, providers and background tasks in one lifecycle boundary"
    )]
    async fn start_with_providers(
        config: ValidatedConfig,
        providers: Providers,
        mut inherited: BTreeMap<String, std::net::TcpListener>,
        data: PathBuf,
        bundles: BTreeMap<String, WorkerBundle>,
    ) -> Result<Self, ProcessError> {
        let shutdown_deadline = Duration::from_secs(config.raw.shutdown.evacuation_deadline_secs);
        reject_unknown_inherited(&config, &inherited)?;
        let control_config = Arc::new(config);

        let telemetry = Arc::new(Telemetry::default());
        let started_at_ms = now_ms();
        let admission = Admission::new(control_config.raw.limits.isolates);
        let incarnation = Uuid::new_v4();
        let context = process_context(
            &control_config,
            &providers,
            &data,
            Arc::clone(&telemetry),
            admission.clone(),
        )?;
        let services = Arc::new(service_targets(&bundles, &context));
        let objects = Arc::new(durable_services(&control_config.raw.services));
        let registry = Arc::new(StdMutex::new(BTreeMap::new()));

        let readiness = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let listeners_total =
            1 + control_config.sockets.len() + usize::from(control_config.console_listen.is_some());
        let services_total = control_config.raw.services.len();
        let consumers_total = context.consumers.values().map(Vec::len).sum();
        let mut bound = Vec::new();
        bound.push((
            "peer".to_string(),
            listener("peer", control_config.peer_listen, &mut inherited)
                .await
                .map_err(ProcessError::Listen)?,
            App {
                node: context.node,
                incarnation,
                config: Arc::clone(&control_config),
                data: data.clone(),
                readiness: readiness.clone(),
                retired: retired.clone(),
                metrics: Metrics {
                    listener: Arc::from("peer"),
                    listeners: listeners_total,
                    services: services_total,
                    consumers: consumers_total,
                    worker: false,
                    started_at_ms,
                    telemetry: Arc::clone(&telemetry),
                },
                admission: admission.clone(),
                socket: None,
            },
        ));
        let socket_services = socket_services(&control_config.raw.sockets);
        for (name, address) in control_config.sockets.clone() {
            let service = socket_services
                .get(&name)
                .ok_or_else(|| ProcessError::SocketService(name.clone()))?;
            let bundle = bundles
                .get(service)
                .ok_or_else(|| ProcessError::ServiceBundle(service.clone()))?
                .clone();
            let socket = listener(&name, address, &mut inherited)
                .await
                .map_err(ProcessError::Listen)?;
            let listener_name = Arc::from(name.as_str());
            bound.push((
                name,
                socket,
                App {
                    node: context.node,
                    incarnation,
                    config: Arc::clone(&control_config),
                    data: data.clone(),
                    readiness: readiness.clone(),
                    retired: retired.clone(),
                    metrics: Metrics {
                        listener: listener_name,
                        listeners: listeners_total,
                        services: services_total,
                        consumers: consumers_total,
                        worker: true,
                        started_at_ms,
                        telemetry: Arc::clone(&telemetry),
                    },
                    admission: admission.clone(),
                    socket: Some(socket_app(
                        SocketContext {
                            node: context.node,
                            data: &data,
                            repository: context.repository.clone(),
                            limits: context.limits,
                            environments: &context.environments,
                            d1: &context.d1,
                            r2: &context.r2,
                            r2_notifications: &context.r2_notifications,
                            kv: &context.kv,
                            outbound_hosts: &context.outbound_hosts,
                            aws: &context.aws,
                            mtls: &context.mtls,
                            queues: context.queues.clone(),
                            cache: context.cache.clone(),
                            consumers: &context.consumers,
                            assets: &context.assets,
                            services: Arc::clone(&services),
                            objects: Arc::clone(&objects),
                            registry: Arc::clone(&registry),
                            telemetry: Arc::clone(&telemetry),
                            admission: admission.clone(),
                        },
                        bundle,
                        service,
                    )),
                },
            ));
        }
        if let Some(address) = control_config.console_listen {
            bound.push((
                "console".to_string(),
                listener("console", address, &mut inherited)
                    .await
                    .map_err(ProcessError::Listen)?,
                App {
                    node: context.node,
                    incarnation,
                    config: Arc::clone(&control_config),
                    data: data.clone(),
                    readiness: readiness.clone(),
                    retired: retired.clone(),
                    metrics: Metrics {
                        listener: Arc::from("console"),
                        listeners: listeners_total,
                        services: services_total,
                        consumers: consumers_total,
                        worker: false,
                        started_at_ms,
                        telemetry: Arc::clone(&telemetry),
                    },
                    admission: admission.clone(),
                    socket: None,
                },
            ));
        }

        let mut supervisor = Supervisor::new();
        let mut listeners = BTreeMap::new();
        for (name, listener, app) in bound {
            let address = listener.local_addr().map_err(ProcessError::Listen)?;
            listeners.insert(name.clone(), address);
            let router = router(app);
            supervisor.spawn(name, true, |mut shutdown| async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move { shutdown.wait().await })
                    .await
                    .map_err(|error| TaskError::new(error.to_string()))
            });
        }
        spawn_queue_consumers(&mut supervisor, &context, &services, &objects, &registry);
        supervisor.ready()?;
        readiness.store(true, Ordering::Release);
        Ok(Self {
            supervisor,
            readiness,
            listeners,
            shutdown_deadline,
            _providers: providers,
        })
    }

    #[must_use]
    pub fn listeners(&self) -> &BTreeMap<String, SocketAddr> {
        &self.listeners
    }

    #[must_use]
    pub fn ready(&mut self) -> bool {
        self.readiness.load(Ordering::Acquire) && self.supervisor.accepting()
    }

    pub async fn shutdown(self) -> Result<(), ProcessError> {
        self.readiness.store(false, Ordering::Release);
        self.supervisor.shutdown(self.shutdown_deadline).await?;
        Ok(())
    }
}

pub(crate) fn default_data() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("data")
}

fn process_context(
    config: &ValidatedConfig,
    providers: &Providers,
    data: &Path,
    telemetry: Arc<Telemetry>,
    admission: Admission,
) -> Result<ProcessContext, ProcessError> {
    Ok(ProcessContext {
        node: NodeId::from_uuid(config.raw.node.id),
        data: data.to_path_buf(),
        repository: providers.repository.clone(),
        limits: Limits {
            body: config.raw.limits.request_body_bytes,
            body_limit: usize::try_from(config.raw.limits.request_body_bytes)
                .map_err(|_| ProcessError::Limit("max_request_body_bytes"))?,
            heap: usize::try_from(config.raw.limits.heap_bytes)
                .map_err(|_| ProcessError::Limit("max_heap_bytes"))?,
            execution: Duration::from_millis(config.raw.limits.execution_time_ms),
            subrequests: config.raw.limits.subrequests_per_invocation,
        },
        telemetry,
        admission,
        environments: environments(
            &config.raw.services,
            &config.raw.dispatch_namespaces,
            &config.raw.cache,
            providers,
            &queue_provider_metadata(config),
        )?,
        d1: d1_routes(&config.raw.services, providers, data)?,
        r2: r2_routes(&config.raw.services, providers)?,
        r2_notifications: r2_notifications(&config.raw.services),
        kv: kv_routes(&config.raw.services, providers)?,
        outbound_hosts: outbound_hosts(&config.raw.services),
        aws: aws_bindings(&config.raw.services, providers),
        mtls: mtls_bindings(&config.raw.services, providers),
        queues: queue_broker(config, data)?,
        cache: cache_store(config, providers, data)?,
        consumers: queue_consumers(
            &config.raw.services,
            config
                .raw
                .queues
                .as_ref()
                .map(|queues| &queues.consumer_defaults),
        ),
        assets: asset_configs(&config.raw.services),
    })
}

fn socket_app(context: SocketContext<'_>, bundle: WorkerBundle, service: &str) -> SocketApp {
    SocketApp {
        node: Node::new(context.node, context.data.join("cells"), context.repository),
        bundle,
        service: Arc::from(service),
        limits: context.limits,
        tail: context.data.join("tail"),
        environment: context
            .environments
            .get(service)
            .cloned()
            .unwrap_or_else(WorkerEnvironment::empty),
        d1: context.d1.get(service).cloned().unwrap_or_default(),
        r2: context.r2.get(service).cloned().unwrap_or_default(),
        r2_notifications: context
            .r2_notifications
            .get(service)
            .cloned()
            .unwrap_or_default(),
        kv: context.kv.get(service).cloned().unwrap_or_default(),
        outbound_hosts: context
            .outbound_hosts
            .get(service)
            .cloned()
            .unwrap_or_default(),
        aws: context.aws.get(service).cloned().unwrap_or_default(),
        mtls: context.mtls.get(service).cloned().unwrap_or_default(),
        queues: context.queues.clone(),
        cache: context.cache.clone(),
        consumers: context.consumers.get(service).cloned().unwrap_or_default(),
        assets: context.assets.get(service).cloned(),
        services: context.services,
        objects: context.objects,
        registry: context.registry,
        websocket_sessions: Arc::new(WebSocketRegistry::default()),
        telemetry: context.telemetry,
        admission: context.admission,
    }
}

fn durable_services(services: &[Service]) -> BTreeMap<String, String> {
    services
        .iter()
        .filter_map(|service| match &service.entrypoint {
            peren_config::Entrypoint::DurableObject { class_name, .. } => {
                Some((class_name.clone(), service.name.clone()))
            }
            peren_config::Entrypoint::Stateless => None,
        })
        .collect()
}

fn r2_notifications(services: &[Service]) -> BTreeMap<String, Vec<R2NotificationRule>> {
    services
        .iter()
        .map(|service| {
            let rules = service
                .bindings
                .values()
                .filter_map(|binding| match binding {
                    peren_config::Binding::R2Bucket {
                        bucket,
                        notifications,
                        ..
                    } => Some(
                        notifications
                            .iter()
                            .map(move |notification| R2NotificationRule {
                                bucket: bucket.clone(),
                                queue: notification.queue_name.clone(),
                                events: notification.event_types.clone(),
                                prefix: notification.prefix.clone(),
                                suffix: notification.suffix.clone(),
                            }),
                    ),
                    _ => None,
                })
                .flatten()
                .collect::<Vec<_>>();
            (service.name.clone(), rules)
        })
        .collect()
}

fn outbound_hosts(services: &[Service]) -> BTreeMap<String, Arc<BTreeSet<String>>> {
    services
        .iter()
        .map(|service| {
            let hosts = service
                .bindings
                .values()
                .flat_map(outbound_binding_hosts)
                .collect::<BTreeSet<_>>();
            (service.name.clone(), Arc::new(hosts))
        })
        .collect()
}

fn outbound_binding_hosts(binding: &peren_config::Binding) -> Vec<String> {
    match binding {
        peren_config::Binding::Outbound { allowed_hosts } => allowed_hosts.clone(),
        peren_config::Binding::Images {
            provider: peren_config::ImageProvider::Http { url, .. },
        } => url_host(url).into_iter().collect(),
        peren_config::Binding::Ai {
            endpoint, provider, ..
        } => ai_hosts(endpoint, provider),
        peren_config::Binding::Vectorize { provider, .. } => vector_hosts(provider),
        peren_config::Binding::Container { default_port, .. } => {
            vec![format!("127.0.0.1:{default_port}")]
        }
        _ => Vec::new(),
    }
}

fn mtls_bindings(
    services: &[Service],
    providers: &Providers,
) -> BTreeMap<String, Arc<BTreeMap<String, crate::tls::Identity>>> {
    services
        .iter()
        .map(|service| {
            let bindings = service
                .bindings
                .iter()
                .filter_map(|(name, binding)| {
                    mtls_binding(binding, providers).map(|identity| (name.clone(), identity))
                })
                .collect();
            (service.name.clone(), Arc::new(bindings))
        })
        .collect()
}

fn mtls_binding(
    binding: &peren_config::Binding,
    providers: &Providers,
) -> Option<crate::tls::Identity> {
    let peren_config::Binding::MtlsCertificate {
        cert_pem_env,
        key_pem_env,
    } = binding
    else {
        return None;
    };
    let cert = providers.resolved(cert_pem_env)?;
    let key = providers.resolved(key_pem_env)?;
    Some(crate::tls::Identity::new(cert.to_string(), key.to_string()))
}

fn aws_bindings(
    services: &[Service],
    providers: &Providers,
) -> BTreeMap<String, Arc<BTreeMap<String, AwsBinding>>> {
    services
        .iter()
        .map(|service| {
            let bindings = service
                .bindings
                .iter()
                .filter_map(|(name, binding)| {
                    aws_binding(binding, providers).map(|route| (name.clone(), route))
                })
                .collect();
            (service.name.clone(), Arc::new(bindings))
        })
        .collect()
}

fn aws_binding(binding: &peren_config::Binding, providers: &Providers) -> Option<AwsBinding> {
    let peren_config::Binding::AwsSigv4 {
        credential_source,
        region,
        service,
        allowed_hosts,
        access_key_env,
        secret_key_env,
        token_env,
    } = binding
    else {
        return None;
    };
    let credential = match credential_source {
        peren_config::CredentialsSource::Configured
        | peren_config::CredentialsSource::Environment => {
            let access = providers.resolved(access_key_env.as_deref()?)?;
            let secret = providers.resolved(secret_key_env.as_deref()?)?;
            let token = token_env
                .as_deref()
                .and_then(|name| providers.resolved(name))
                .map(Arc::from);
            AwsCredential::Static {
                access: Arc::from(access),
                secret: Arc::from(secret),
                token,
            }
        }
        peren_config::CredentialsSource::InstanceRole
        | peren_config::CredentialsSource::WorkloadIdentity
        | peren_config::CredentialsSource::EksPodIdentity => AwsCredential::DefaultChain,
    };
    Some(AwsBinding {
        credential,
        region: region.clone(),
        service: service.clone(),
        hosts: allowed_hosts.iter().cloned().collect(),
    })
}

fn ai_hosts(endpoint: &str, provider: &peren_config::AiProvider) -> Vec<String> {
    match provider {
        peren_config::AiProvider::Http => url_host(endpoint).into_iter().collect(),
        peren_config::AiProvider::OpenAi { base_url, .. }
        | peren_config::AiProvider::Anthropic { base_url, .. }
        | peren_config::AiProvider::Gemini { base_url, .. } => {
            url_host(base_url).into_iter().collect()
        }
        peren_config::AiProvider::WorkersAi { .. } => vec!["api.cloudflare.com".into()],
        peren_config::AiProvider::Local { .. } => Vec::new(),
    }
}

fn vector_hosts(provider: &peren_config::VectorProvider) -> Vec<String> {
    match provider {
        peren_config::VectorProvider::Http { url, .. }
        | peren_config::VectorProvider::Qdrant { url, .. }
        | peren_config::VectorProvider::Pinecone { url, .. }
        | peren_config::VectorProvider::Weaviate { url, .. } => url_host(url).into_iter().collect(),
        peren_config::VectorProvider::Local => Vec::new(),
    }
}

fn url_host(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url).ok()?;
    let host = url.host_str()?;
    Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn service_targets(
    bundles: &BTreeMap<String, WorkerBundle>,
    context: &ProcessContext,
) -> BTreeMap<String, ServiceTarget> {
    bundles
        .iter()
        .map(|(name, bundle)| {
            (
                name.clone(),
                ServiceTarget {
                    bundle: bundle.clone(),
                    environment: context
                        .environments
                        .get(name)
                        .cloned()
                        .unwrap_or_else(WorkerEnvironment::empty),
                    d1: context.d1.get(name).cloned().unwrap_or_default(),
                    r2: context.r2.get(name).cloned().unwrap_or_default(),
                    r2_notifications: context
                        .r2_notifications
                        .get(name)
                        .cloned()
                        .unwrap_or_default(),
                    kv: context.kv.get(name).cloned().unwrap_or_default(),
                    outbound_hosts: context
                        .outbound_hosts
                        .get(name)
                        .cloned()
                        .unwrap_or_default(),
                    aws: context.aws.get(name).cloned().unwrap_or_default(),
                    mtls: context.mtls.get(name).cloned().unwrap_or_default(),
                },
            )
        })
        .collect()
}

fn asset_configs(services: &[Service]) -> BTreeMap<String, AssetConfig> {
    services
        .iter()
        .filter_map(|service| {
            service.assets.as_ref().map(|assets| {
                (
                    service.name.clone(),
                    AssetConfig::new(assets.directory.clone(), assets.run_worker_first.clone()),
                )
            })
        })
        .collect()
}

fn cache_store(
    config: &ValidatedConfig,
    providers: &Providers,
    data: &Path,
) -> Result<CacheStore, ProcessError> {
    match &config.raw.cache {
        Cache::Memory => Ok(CacheStore::memory()),
        Cache::Kv { namespace } => Ok(CacheStore::native_kv(
            Arc::new(tokio::sync::Mutex::new(peren_storage::CellStorage::open(
                &data.join("cache.sqlite"),
            )?)),
            namespace.clone(),
        )),
        Cache::Redis { url_env } => {
            let url = providers
                .resolved(url_env)
                .ok_or_else(|| ProcessError::SecretVariable(url_env.clone()))?;
            Ok(CacheStore::redis(url.to_string(), "peren"))
        }
        Cache::Bucket {
            endpoint,
            bucket,
            prefix,
            access_key_id_env,
            secret_access_key_env,
            allow_http,
        } => {
            if endpoint.starts_with("memory://") {
                return Ok(CacheStore::bucket(
                    Arc::new(R2Store::new(
                        Arc::new(object_store::memory::InMemory::new()),
                    )),
                    prefix.clone(),
                ));
            }
            let access_key = providers
                .resolved(access_key_id_env)
                .ok_or_else(|| ProcessError::SecretVariable(access_key_id_env.clone()))?;
            let secret_key = providers
                .resolved(secret_access_key_env)
                .ok_or_else(|| ProcessError::SecretVariable(secret_access_key_env.clone()))?;
            let store = R2Store::s3(
                S3Options {
                    bucket: bucket.clone(),
                    region: "us-east-1".into(),
                    endpoint: Some(endpoint.clone()),
                    allow_http: *allow_http,
                    virtual_hosted: false,
                },
                S3Credentials::new(access_key.to_string(), secret_key.to_string(), None),
            )?;
            Ok(CacheStore::bucket(Arc::new(store), prefix.clone()))
        }
    }
}

mod environment;
use environment::{d1_routes, environments, has_binding, kv_routes, r2_routes};

fn socket_services(sockets: &[peren_config::Socket]) -> BTreeMap<String, String> {
    sockets
        .iter()
        .map(|socket| (socket.name.clone(), socket.service.clone()))
        .collect()
}

fn bundles(
    services: &[Service],
    dispatch_namespaces: &[DispatchNamespace],
) -> Result<BTreeMap<String, WorkerBundle>, ProcessError> {
    let mut bundles = services
        .iter()
        .map(|service| bundle::load(service).map(|bundle| (service.name.clone(), bundle)))
        .collect::<Result<BTreeMap<_, _>, ProcessError>>()?;
    for namespace in dispatch_namespaces {
        for script in &namespace.scripts {
            let name = format!("{}/{}", namespace.name, script.name);
            bundles.insert(name, bundle::load_path(&script.worker_bundle_path)?);
        }
    }
    Ok(bundles)
}

async fn turso_routes(
    routes: &BTreeMap<String, D1Route>,
) -> Result<BTreeMap<String, Arc<Mutex<peren_provider_turso::TursoStore>>>, ProcessError> {
    let mut stores = BTreeMap::new();
    for (name, route) in routes {
        if let Some(parent) = route.replica.as_ref().and_then(|path| path.parent()) {
            std::fs::create_dir_all(parent).map_err(ProcessError::Data)?;
        }
        let store = match &route.replica {
            Some(path) => {
                peren_provider_turso::TursoStore::replica(
                    path,
                    route.url.to_string(),
                    route.token.to_string(),
                )
                .await
            }
            None => {
                peren_provider_turso::TursoStore::remote(
                    route.url.to_string(),
                    route.token.to_string(),
                )
                .await
            }
        }
        .map_err(ProcessError::Turso)?;
        stores.insert(name.clone(), Arc::new(Mutex::new(store)));
    }
    Ok(stores)
}

mod dispatch;
#[cfg(test)]
pub(crate) use dispatch::dispatch_worker;
use dispatch::{listener, router};

mod host;
#[cfg(test)]
pub(crate) use host::object_cell;
use host::{ProcessHost, cell};

mod error;
pub use error::ProcessError;
