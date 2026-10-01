use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::Instant,
};

use axum::http::Uri;
use peren_config::R2EventType;
use peren_primitives::CellId;
use peren_queues::Send;
use peren_runtime::{
    AwsSigv4Fetch, DurableObjectFetch, DurableObjectHost, DurableStorageHost, HostError,
    HttpRequest, HttpResponse, InvocationLimits, IsolateLimits, KvGet, KvHost, KvList, KvPut,
    ListOptions, ListPage, OutboundFetchHost, QueueProducerHost, QueueSend, R2BucketHost, R2Delete,
    R2Get, R2List, R2ListPage, R2Object, R2Put, ServiceBindingHost, ServiceFetch, SqlQuery,
    SqlResult,
};
use sha2::{Digest, Sha256};

use crate::{
    Node, Repository,
    host::{CacheStore, RoutedHost},
    metrics::Telemetry,
    process::{
        AwsBinding, Limits, ObjectRegistry, QueueBrokerState, R2NotificationRule, ServiceTarget,
        turso_routes,
    },
};

#[derive(Clone)]
pub(super) struct ProcessHost {
    pub(super) inner: RoutedHost,
    pub(super) node: Node<Repository>,
    pub(super) services: Arc<BTreeMap<String, ServiceTarget>>,
    pub(super) objects: Arc<BTreeMap<String, String>>,
    pub(super) registry: ObjectRegistry,
    pub(super) outbound_hosts: Arc<BTreeSet<String>>,
    pub(super) aws: Arc<BTreeMap<String, AwsBinding>>,
    pub(super) mtls: Arc<BTreeMap<String, crate::tls::Identity>>,
    pub(super) queues: QueueBrokerState,
    pub(super) r2_notifications: Vec<R2NotificationRule>,
    pub(super) cache: CacheStore,
    pub(super) limits: Limits,
    pub(super) telemetry: Arc<Telemetry>,
}

impl ProcessHost {
    fn record_cache<T>(&self, result: &Result<T, HostError>) {
        match result {
            Ok(_) => Telemetry::inc(&self.telemetry.cache_operations),
            Err(_) => Telemetry::inc(&self.telemetry.cache_errors),
        }
    }

    fn record_r2<T>(&self, result: &Result<T, HostError>) {
        match result {
            Ok(_) => Telemetry::inc(&self.telemetry.r2_operations),
            Err(_) => Telemetry::inc(&self.telemetry.r2_errors),
        }
    }

    fn record_kv<T>(&self, result: &Result<T, HostError>) {
        match result {
            Ok(_) => Telemetry::inc(&self.telemetry.kv_operations),
            Err(_) => Telemetry::inc(&self.telemetry.kv_errors),
        }
    }

    fn record_d1<T>(&self, result: &Result<T, HostError>) {
        match result {
            Ok(_) => Telemetry::inc(&self.telemetry.d1_queries),
            Err(_) => Telemetry::inc(&self.telemetry.d1_errors),
        }
    }

    async fn notify_r2(
        &self,
        bucket: &str,
        key: &str,
        event: R2EventType,
    ) -> Result<(), HostError> {
        for rule in self
            .r2_notifications
            .iter()
            .filter(|rule| rule.matches(bucket, key, event))
        {
            let body = serde_json::to_vec(&serde_json::json!({
                "source": "peren.r2",
                "type": r2_event_name(event),
                "bucket": bucket,
                "key": key,
                "eventTime": chrono::Utc::now().to_rfc3339(),
            }))
            .map_err(|_| HostError)?;
            self.queues
                .send(
                    Send {
                        queue: rule.queue.clone(),
                        body,
                        content_type: Some("application/json".into()),
                        partition: Some(format!("{bucket}/{key}")),
                        delay: chrono::Duration::zero(),
                        dedup_id: Some(format!("{}:{bucket}:{key}", r2_event_name(event))),
                    },
                    chrono::Utc::now(),
                )
                .await
                .map_err(|_| HostError)?;
        }
        Ok(())
    }
}

impl R2NotificationRule {
    fn matches(&self, bucket: &str, key: &str, event: R2EventType) -> bool {
        self.bucket == bucket
            && self.events.contains(&event)
            && self
                .prefix
                .as_ref()
                .is_none_or(|prefix| key.starts_with(prefix))
            && self
                .suffix
                .as_ref()
                .is_none_or(|suffix| key.ends_with(suffix))
    }
}

fn r2_event_name(event: R2EventType) -> &'static str {
    match event {
        R2EventType::ObjectCreate => "object_create",
        R2EventType::ObjectDelete => "object_delete",
    }
}

#[async_trait::async_trait]
impl peren_runtime::AiHost for ProcessHost {
    async fn run(&self, request: peren_runtime::AiRun) -> Result<serde_json::Value, HostError> {
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || run_ai_command(&request))
            .await
            .map_err(|_| HostError)?;
        Telemetry::observe(
            &self.telemetry.ai_duration_ms,
            &self.telemetry.ai_duration,
            started,
        );
        match result {
            Ok(value) => {
                Telemetry::inc(&self.telemetry.ai_runs);
                Ok(value)
            }
            Err(error) => {
                Telemetry::inc(&self.telemetry.ai_errors);
                Err(error)
            }
        }
    }
}

fn run_ai_command(request: &peren_runtime::AiRun) -> Result<serde_json::Value, HostError> {
    if request.command.trim().is_empty() {
        return Err(HostError);
    }
    let mut child = ProcessCommand::new(&request.command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| HostError)?;
    let payload = serde_json::json!({
        "model": request.model,
        "input": request.input,
        "options": request.options,
    });
    let input = serde_json::to_vec(&payload).map_err(|_| HostError)?;
    child
        .stdin
        .as_mut()
        .ok_or(HostError)?
        .write_all(&input)
        .map_err(|_| HostError)?;
    drop(child.stdin.take());
    let output = child.wait_with_output().map_err(|_| HostError)?;
    if !output.status.success() {
        return Err(HostError);
    }
    match serde_json::from_slice(&output.stdout) {
        Ok(value) => Ok(value),
        Err(_) => Ok(serde_json::Value::String(
            String::from_utf8(output.stdout).map_err(|_| HostError)?,
        )),
    }
}

#[async_trait::async_trait]
impl peren_runtime::CacheHost for ProcessHost {
    async fn match_entry(
        &self,
        request: peren_runtime::CacheGet,
    ) -> Result<Option<peren_runtime::CacheEntry>, HostError> {
        let started = Instant::now();
        let result = self.inner.match_entry(request).await;
        Telemetry::observe(
            &self.telemetry.cache_duration_ms,
            &self.telemetry.cache_duration,
            started,
        );
        self.record_cache(&result);
        result
    }

    async fn put_entry(&self, entry: peren_runtime::CachePut) -> Result<(), HostError> {
        let started = Instant::now();
        let result = self.inner.put_entry(entry).await;
        Telemetry::observe(
            &self.telemetry.cache_duration_ms,
            &self.telemetry.cache_duration,
            started,
        );
        self.record_cache(&result);
        result
    }

    async fn delete_entry(&self, request: peren_runtime::CacheGet) -> Result<bool, HostError> {
        let started = Instant::now();
        let result = self.inner.delete_entry(request).await;
        Telemetry::observe(
            &self.telemetry.cache_duration_ms,
            &self.telemetry.cache_duration,
            started,
        );
        self.record_cache(&result);
        result
    }
}

#[async_trait::async_trait]
impl KvHost for ProcessHost {
    async fn get(&self, request: KvGet) -> Result<Option<Vec<u8>>, HostError> {
        let started = Instant::now();
        let result = KvHost::get(&self.inner, request).await;
        Telemetry::observe(
            &self.telemetry.kv_duration_ms,
            &self.telemetry.kv_duration,
            started,
        );
        self.record_kv(&result);
        result
    }

    async fn put(&self, request: KvPut) -> Result<(), HostError> {
        let started = Instant::now();
        let result = KvHost::put(&self.inner, request).await;
        Telemetry::observe(
            &self.telemetry.kv_duration_ms,
            &self.telemetry.kv_duration,
            started,
        );
        self.record_kv(&result);
        result
    }

    async fn delete(&self, request: KvGet) -> Result<bool, HostError> {
        let started = Instant::now();
        let result = KvHost::delete(&self.inner, request).await;
        Telemetry::observe(
            &self.telemetry.kv_duration_ms,
            &self.telemetry.kv_duration,
            started,
        );
        self.record_kv(&result);
        result
    }

    async fn list(&self, request: KvList) -> Result<ListPage, HostError> {
        let started = Instant::now();
        let result = KvHost::list(&self.inner, request).await;
        Telemetry::observe(
            &self.telemetry.kv_duration_ms,
            &self.telemetry.kv_duration,
            started,
        );
        self.record_kv(&result);
        result
    }
}

#[async_trait::async_trait]
impl OutboundFetchHost for ProcessHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        let started = Instant::now();
        let result = self.fetch_outbound(request).await;
        Telemetry::observe(
            &self.telemetry.outbound_duration_ms,
            &self.telemetry.outbound_duration,
            started,
        );
        match &result {
            Ok(_) => Telemetry::inc(&self.telemetry.outbound_fetches),
            Err(_) => Telemetry::inc(&self.telemetry.outbound_errors),
        }
        result
    }

    async fn fetch_aws(&self, request: AwsSigv4Fetch) -> Result<HttpResponse, HostError> {
        let started = Instant::now();
        let result = self.fetch_aws_bound(request).await;
        Telemetry::observe(
            &self.telemetry.outbound_duration_ms,
            &self.telemetry.outbound_duration,
            started,
        );
        match &result {
            Ok(_) => Telemetry::inc(&self.telemetry.outbound_fetches),
            Err(_) => Telemetry::inc(&self.telemetry.outbound_errors),
        }
        result
    }
}

impl ProcessHost {
    async fn fetch_aws_bound(&self, request: AwsSigv4Fetch) -> Result<HttpResponse, HostError> {
        let binding = self.aws.get(&request.binding).ok_or(HostError)?;
        let client = crate::tls::client().map_err(|_| HostError)?;
        super::aws::fetch(&client, binding, request).await
    }

    async fn fetch_outbound(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        let url = reqwest::Url::parse(&request.url).map_err(|_| HostError)?;
        let host = request_host(&url).ok_or(HostError)?;
        if !self.outbound_hosts.contains(&host) {
            return Err(HostError);
        }
        let method =
            reqwest::Method::from_bytes(request.method.as_bytes()).map_err(|_| HostError)?;
        let client = match request.mtls.as_deref() {
            Some(name) => {
                let identity = self.mtls.get(name).ok_or(HostError)?.clone();
                crate::tls::client_with_identity(Some(identity)).map_err(|_| HostError)?
            }
            None => crate::tls::client().map_err(|_| HostError)?,
        };
        let mut builder = client.request(method, url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(error) => {
                return Ok(HttpResponse {
                    status: 502,
                    headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
                    body: error.to_string().into_bytes(),
                    upgrade: false,
                    websocket_id: None,
                });
            }
        };
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect();
        let body = response.bytes().await.map_err(|_| HostError)?.to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
            upgrade: false,
            websocket_id: None,
        })
    }
}

fn request_host(url: &reqwest::Url) -> Option<String> {
    let host = url.host_str()?;
    Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

#[async_trait::async_trait]
impl ServiceBindingHost for ProcessHost {
    async fn fetch(&self, fetch: ServiceFetch) -> Result<HttpResponse, HostError> {
        let started = Instant::now();
        let host = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| HostError)?;
            runtime.block_on(dispatch_service(host, fetch))
        })
        .await
        .map_err(|_| HostError)?;
        Telemetry::observe(
            &self.telemetry.service_duration_ms,
            &self.telemetry.service_duration,
            started,
        );
        match result {
            Ok(response) => {
                Telemetry::inc(&self.telemetry.service_fetches);
                Ok(response)
            }
            Err(error) => {
                Telemetry::inc(&self.telemetry.service_errors);
                Err(error)
            }
        }
    }
}

async fn dispatch_service(
    host: ProcessHost,
    fetch: ServiceFetch,
) -> Result<HttpResponse, HostError> {
    let target = host.services.get(&fetch.service).ok_or(HostError)?.clone();
    let uri = fetch.request.url.parse::<Uri>().map_err(|_| HostError)?;
    let path = uri.path_and_query().map_or("/", |value| value.as_str());
    let isolate = IsolateLimits::new(host.limits.heap, host.limits.execution);
    let invocation = InvocationLimits::new(host.limits.body, host.limits.subrequests);
    let d1 = turso_routes(&target.d1).await.map_err(|_| HostError)?;
    let r2 = target.r2.clone();
    let kv = target.kv.clone();
    let queues = host.queues.clone();
    let cache = host.cache.clone();
    let outbound_hosts = Arc::clone(&target.outbound_hosts);
    let services = Arc::clone(&host.services);
    let objects = Arc::clone(&host.objects);
    let node = host.node.clone();
    let registry = Arc::clone(&host.registry);
    let limits = host.limits;
    host.node
        .restore_and_dispatch(
            cell(&fetch.service, path),
            |path, lease, store| async move {
                let mut resident = peren_cell::WorkerCell::activate_with_capabilities(
                    &path,
                    lease,
                    store,
                    target.bundle,
                    isolate,
                    target.environment,
                    move |storage| {
                        Arc::new(ProcessHost {
                            inner: RoutedHost::new(
                                storage,
                                d1,
                                r2,
                                queues.clone(),
                                cache.clone(),
                                kv.clone(),
                            ),
                            node,
                            services,
                            objects,
                            registry: Arc::clone(&registry),
                            outbound_hosts: Arc::clone(&outbound_hosts),
                            aws: Arc::clone(&target.aws),
                            mtls: Arc::clone(&target.mtls),
                            queues,
                            r2_notifications: target.r2_notifications,
                            cache,
                            limits,
                            telemetry: Arc::clone(&host.telemetry),
                        })
                    },
                )
                .await?;
                let response = resident.dispatch_http(fetch.request, invocation).await?;
                resident.release().await?;
                Ok(response)
            },
        )
        .await
        .map_err(|_| HostError)
}

#[async_trait::async_trait]
impl DurableObjectHost for ProcessHost {
    async fn fetch(&self, fetch: DurableObjectFetch) -> Result<HttpResponse, HostError> {
        let started = Instant::now();
        let host = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| HostError)?;
            runtime.block_on(dispatch_object(host, fetch))
        })
        .await
        .map_err(|_| HostError)?;
        Telemetry::observe(
            &self.telemetry.object_duration_ms,
            &self.telemetry.object_duration,
            started,
        );
        match result {
            Ok(response) => {
                Telemetry::inc(&self.telemetry.object_fetches);
                Ok(response)
            }
            Err(error) => {
                Telemetry::inc(&self.telemetry.object_errors);
                Err(error)
            }
        }
    }
}

async fn dispatch_object(
    host: ProcessHost,
    fetch: DurableObjectFetch,
) -> Result<HttpResponse, HostError> {
    let service = host
        .objects
        .get(&fetch.class_name)
        .ok_or(HostError)?
        .clone();
    let target = host.services.get(&service).ok_or(HostError)?.clone();
    let _uri = fetch.request.url.parse::<Uri>().map_err(|_| HostError)?;
    let isolate = IsolateLimits::new(host.limits.heap, host.limits.execution);
    let invocation = InvocationLimits::new(host.limits.body, host.limits.subrequests);
    let d1 = turso_routes(&target.d1).await.map_err(|_| HostError)?;
    let r2 = target.r2.clone();
    let kv = target.kv.clone();
    let queues = host.queues.clone();
    let cache = host.cache.clone();
    let outbound_hosts = Arc::clone(&target.outbound_hosts);
    let services = Arc::clone(&host.services);
    let objects = Arc::clone(&host.objects);
    let node = host.node.clone();
    let registry = Arc::clone(&host.registry);
    let limits = host.limits;
    host.node
        .restore_and_dispatch(
            object_cell(&fetch.namespace, &fetch.id),
            |path, lease, store| async move {
                let mut resident = peren_cell::WorkerCell::activate_with_capabilities(
                    &path,
                    lease,
                    store,
                    target.bundle,
                    isolate,
                    target.environment,
                    move |storage| {
                        Arc::new(ProcessHost {
                            inner: RoutedHost::new(
                                storage,
                                d1,
                                r2,
                                queues.clone(),
                                cache.clone(),
                                kv.clone(),
                            ),
                            node,
                            services,
                            objects,
                            registry: Arc::clone(&registry),
                            outbound_hosts: Arc::clone(&outbound_hosts),
                            aws: Arc::clone(&target.aws),
                            mtls: Arc::clone(&target.mtls),
                            queues,
                            r2_notifications: target.r2_notifications,
                            cache,
                            limits,
                            telemetry: Arc::clone(&host.telemetry),
                        })
                    },
                )
                .await?;
                resident.bind_durable_class(&fetch.class_name)?;
                let response = resident.dispatch_http(fetch.request, invocation).await?;
                resident.release().await?;
                Ok(response)
            },
        )
        .await
        .map_err(|_| HostError)
}

#[async_trait::async_trait]
impl QueueProducerHost for ProcessHost {
    async fn send(&self, message: QueueSend) -> Result<(), HostError> {
        let started = Instant::now();
        let delay = chrono::Duration::seconds(i64::from(message.delay_seconds.unwrap_or(0)));
        let result = self
            .queues
            .send(
                Send {
                    queue: message.queue,
                    body: message.body,
                    content_type: message.content_type,
                    partition: message.partition,
                    delay,
                    dedup_id: message.dedup_id,
                },
                chrono::Utc::now(),
            )
            .await
            .map(|_| ())
            .map_err(|_| HostError);
        Telemetry::observe(
            &self.telemetry.queue_send_duration_ms,
            &self.telemetry.queue_send_duration,
            started,
        );
        match &result {
            Ok(()) => Telemetry::inc(&self.telemetry.queue_sends),
            Err(_) => Telemetry::inc(&self.telemetry.queue_send_errors),
        }
        result
    }
}

#[async_trait::async_trait]
impl R2BucketHost for ProcessHost {
    async fn put(&self, object: R2Put) -> Result<(), HostError> {
        let started = Instant::now();
        let bucket = object.bucket.clone();
        let key = object.key.clone();
        let result = R2BucketHost::put(&self.inner, object).await;
        if result.is_ok() {
            self.notify_r2(&bucket, &key, R2EventType::ObjectCreate)
                .await?;
        }
        Telemetry::observe(
            &self.telemetry.r2_duration_ms,
            &self.telemetry.r2_duration,
            started,
        );
        self.record_r2(&result);
        result
    }

    async fn get(&self, object: R2Get) -> Result<Option<R2Object>, HostError> {
        let started = Instant::now();
        let result = R2BucketHost::get(&self.inner, object).await;
        Telemetry::observe(
            &self.telemetry.r2_duration_ms,
            &self.telemetry.r2_duration,
            started,
        );
        self.record_r2(&result);
        result
    }

    async fn delete(&self, object: R2Delete) -> Result<(), HostError> {
        let started = Instant::now();
        let bucket = object.bucket.clone();
        let key = object.key.clone();
        let result = R2BucketHost::delete(&self.inner, object).await;
        if result.is_ok() {
            self.notify_r2(&bucket, &key, R2EventType::ObjectDelete)
                .await?;
        }
        Telemetry::observe(
            &self.telemetry.r2_duration_ms,
            &self.telemetry.r2_duration,
            started,
        );
        self.record_r2(&result);
        result
    }

    async fn list(&self, request: R2List) -> Result<R2ListPage, HostError> {
        let started = Instant::now();
        let result = R2BucketHost::list(&self.inner, request).await;
        Telemetry::observe(
            &self.telemetry.r2_duration_ms,
            &self.telemetry.r2_duration,
            started,
        );
        self.record_r2(&result);
        result
    }
}

#[async_trait::async_trait]
impl DurableStorageHost for ProcessHost {
    async fn begin(&self) -> Result<(), HostError> {
        self.inner.begin().await
    }

    async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        self.inner.load(scope, key).await
    }

    async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError> {
        DurableStorageHost::put(&self.inner, scope, key, value).await
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
        DurableStorageHost::delete(&self.inner, scope, key).await
    }

    async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError> {
        DurableStorageHost::list(&self.inner, scope, options).await
    }

    async fn sql(&self, query: SqlQuery) -> Result<SqlResult, HostError> {
        let started = Instant::now();
        let result = self.inner.sql(query).await;
        Telemetry::observe(
            &self.telemetry.d1_duration_ms,
            &self.telemetry.d1_duration,
            started,
        );
        self.record_d1(&result);
        result
    }

    async fn mutation_outcome(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        self.inner.mutation_outcome(id).await
    }

    async fn record_mutation_outcome(&self, id: &str, outcome: &[u8]) -> Result<(), HostError> {
        self.inner.record_mutation_outcome(id, outcome).await
    }

    async fn attachment(&self, id: &str) -> Result<Option<Vec<u8>>, HostError> {
        self.inner.attachment(id).await
    }

    async fn set_attachment(&self, id: &str, bytes: &[u8]) -> Result<(), HostError> {
        self.inner.set_attachment(id, bytes).await
    }

    async fn delete_attachment(&self, id: &str) -> Result<bool, HostError> {
        self.inner.delete_attachment(id).await
    }

    async fn commit(&self) -> Result<peren_primitives::StorageRevision, HostError> {
        let started = Instant::now();
        let result = self.inner.commit().await;
        Telemetry::observe(
            &self.telemetry.storage_commit_duration_ms,
            &self.telemetry.storage_commit_duration,
            started,
        );
        if result.is_ok() {
            Telemetry::inc(&self.telemetry.storage_commits);
        }
        result
    }

    async fn rollback(&self) -> Result<(), HostError> {
        let result = self.inner.rollback().await;
        if result.is_ok() {
            Telemetry::inc(&self.telemetry.storage_rollbacks);
        }
        result
    }
}

pub(crate) fn object_cell(namespace: &str, id: &str) -> CellId {
    let mut digest = Sha256::new();
    digest.update(b"peren-do-v1\0");
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(id.as_bytes());
    CellId::from_bytes(digest.finalize().into())
}

pub(super) fn cell(service: &str, path: &str) -> CellId {
    let id = path
        .trim_start_matches('/')
        .split('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    let mut digest = Sha256::new();
    digest.update(b"peren-cell-v1\0");
    digest.update(service.as_bytes());
    digest.update([0]);
    digest.update(id.as_bytes());
    CellId::from_bytes(digest.finalize().into())
}
