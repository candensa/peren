use crate::{
    AiHost, CacheHost, DurableObjectHost, DurableStorageHost, InvocationStorage, IsolateLimits,
    KvHost, OutboundFetchHost, QueueProducerHost, R2BucketHost, ServiceBindingHost, WorkerBundle,
    loader::BundleLoader,
};
use deno_core::{
    ExtensionFileSource, FastString, JsRuntime, PollEventLoopOptions, RuntimeOptions, v8,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{collections::BTreeMap, rc::Rc};

mod dispatch;
mod error;
pub(crate) mod host;
mod script;
pub(crate) mod watchdog;

pub use error::EngineError;
pub(crate) use host::{InvocationHosts, web_extensions};
use script::SEAL_INTERNALS;
pub(crate) use watchdog::Watchdog;

deno_core::extension!(
    peren_web,
    deps = [deno_webidl, deno_web, deno_fetch],
    esm_entry_point = "ext:peren_web/bootstrap.js",
    customizer = |extension: &mut deno_core::Extension| {
        extension.esm_files.to_mut().push(ExtensionFileSource::new(
            "ext:peren_web/bootstrap.js",
            deno_core::ascii_str_include!("bootstrap.js"),
        ));
    },
);

deno_core::extension!(peren_storage,
    ops = [
        crate::ops::op_storage_begin,
        crate::ops::op_storage_get,
        crate::ops::op_storage_put,
        crate::ops::op_storage_delete,
        crate::ops::op_storage_list,
        crate::ops::op_storage_sql,
        crate::ops::op_storage_commit,
        crate::ops::op_storage_rollback,
        crate::ops::op_storage_mutation_outcome_get,
        crate::ops::op_storage_mutation_outcome_record,
        crate::ops::op_ws_attachment_get,
        crate::ops::op_ws_attachment_set,
        crate::ops::op_ws_attachment_delete,
        crate::ops::op_cache_match,
        crate::ops::op_cache_put,
        crate::ops::op_cache_delete,
        crate::ops::op_kv_get,
        crate::ops::op_kv_put,
        crate::ops::op_kv_delete,
        crate::ops::op_kv_list,
        crate::ops::op_ai_run,
        crate::ops::op_r2_put,
        crate::ops::op_r2_get,
        crate::ops::op_r2_delete,
        crate::ops::op_r2_list,
        crate::ops::op_queue_send,
        crate::ops::op_console_log,
        crate::ops::op_outbound_fetch,
        crate::ops::op_aws_sigv4_fetch,
        crate::ops::op_service_fetch,
        crate::ops::op_durable_object_fetch,
        crate::ops::op_crypto_random,
        crate::ops::op_crypto_digest,
        crate::ops::op_crypto_aes_gcm_encrypt,
        crate::ops::op_crypto_aes_gcm_decrypt,
        crate::ops::op_crypto_aes_cbc_encrypt,
        crate::ops::op_crypto_aes_cbc_decrypt,
        crate::ops::op_crypto_ed25519_generate,
        crate::ops::op_crypto_ed25519_sign,
        crate::ops::op_crypto_ed25519_verify,
        crate::ops::op_crypto_ecdsa_p256_generate,
        crate::ops::op_crypto_ecdsa_p256_sign,
        crate::ops::op_crypto_ecdsa_p256_verify,
        crate::ops::op_crypto_hmac,
        crate::ops::op_crypto_pbkdf2,
        crate::ops::op_crypto_hkdf,
    ],
    options = { hosts: InvocationHosts },
    state = |state, config| {
        state.put(config.hosts.storage);
        state.put(config.hosts.fetch);
        state.put(config.hosts.queue);
        state.put(config.hosts.r2);
        state.put(config.hosts.service);
        state.put(config.hosts.durable);
        state.put(config.hosts.cache);
        state.put(config.hosts.kv);
        state.put(config.hosts.ai);
        state.put(crate::ops::console::ConsoleEvents::default());
    },
);

pub struct WorkerRuntime {
    runtime: JsRuntime,
    limits: IsolateLimits,
    heap_exceeded: Arc<AtomicBool>,
    storage: Option<InvocationStorage>,
    exports: v8::Global<v8::Object>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkerEnvironment {
    values: BTreeMap<String, String>,
}

impl WorkerEnvironment {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn new(values: BTreeMap<String, String>) -> Self {
        Self { values }
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    #[must_use]
    pub const fn values(&self) -> &BTreeMap<String, String> {
        &self.values
    }
}

#[derive(Default)]
struct Hosts {
    storage: Option<Arc<dyn DurableStorageHost>>,
    fetch: Option<Arc<dyn OutboundFetchHost>>,
    queue: Option<Arc<dyn QueueProducerHost>>,
    r2: Option<Arc<dyn R2BucketHost>>,
    service: Option<Arc<dyn ServiceBindingHost>>,
    durable: Option<Arc<dyn DurableObjectHost>>,
    cache: Option<Arc<dyn CacheHost>>,
    kv: Option<Arc<dyn KvHost>>,
    ai: Option<Arc<dyn AiHost>>,
}

pub struct Capabilities {
    pub storage: Arc<dyn DurableStorageHost>,
    pub fetch: Option<Arc<dyn OutboundFetchHost>>,
    pub queue: Option<Arc<dyn QueueProducerHost>>,
    pub r2: Option<Arc<dyn R2BucketHost>>,
    pub service: Option<Arc<dyn ServiceBindingHost>>,
    pub durable: Option<Arc<dyn DurableObjectHost>>,
    pub cache: Option<Arc<dyn CacheHost>>,
    pub kv: Option<Arc<dyn KvHost>>,
    pub ai: Option<Arc<dyn AiHost>>,
}

impl WorkerRuntime {
    pub async fn load(bundle: WorkerBundle, limits: IsolateLimits) -> Result<Self, EngineError> {
        Self::load_inner(bundle, limits, WorkerEnvironment::empty(), Hosts::default()).await
    }

    pub async fn load_with_host(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        host: Arc<dyn DurableStorageHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            WorkerEnvironment::empty(),
            Hosts {
                storage: Some(host),
                ..Hosts::default()
            },
        )
        .await
    }

    pub async fn load_with_hosts(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        storage: Arc<dyn DurableStorageHost>,
        fetch: Arc<dyn OutboundFetchHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            WorkerEnvironment::empty(),
            Hosts {
                storage: Some(storage),
                fetch: Some(fetch),
                ..Hosts::default()
            },
        )
        .await
    }

    pub async fn load_with_service(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: Arc<dyn DurableStorageHost>,
        service: Arc<dyn ServiceBindingHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            Hosts {
                storage: Some(storage),
                service: Some(service),
                ..Hosts::default()
            },
        )
        .await
    }

    pub async fn load_with_queue(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: Arc<dyn DurableStorageHost>,
        queue: Arc<dyn QueueProducerHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            Hosts {
                storage: Some(storage),
                queue: Some(queue),
                ..Hosts::default()
            },
        )
        .await
    }

    pub async fn load_with_r2(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: Arc<dyn DurableStorageHost>,
        r2: Arc<dyn R2BucketHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            Hosts {
                storage: Some(storage),
                r2: Some(r2),
                ..Hosts::default()
            },
        )
        .await
    }

    pub async fn load_with_capabilities(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        capabilities: Capabilities,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            Hosts {
                storage: Some(capabilities.storage),
                fetch: capabilities.fetch,
                queue: capabilities.queue,
                r2: capabilities.r2,
                service: capabilities.service,
                durable: capabilities.durable,
                cache: capabilities.cache,
                kv: capabilities.kv,
                ai: capabilities.ai,
            },
        )
        .await
    }

    pub async fn load_with_environment(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        host: Arc<dyn DurableStorageHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            Hosts {
                storage: Some(host),
                ..Hosts::default()
            },
        )
        .await
    }

    async fn load_inner(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        hosts: Hosts,
    ) -> Result<Self, EngineError> {
        let specifier = BundleLoader::specifier(bundle.entry())?;
        let loader = Rc::new(BundleLoader::new(bundle));
        let create_params =
            v8::CreateParams::default().set_max_old_generation_size_in_bytes(limits.heap_bytes());
        let hosts = InvocationHosts::from_hosts(hosts);
        let storage = Some(hosts.storage.clone());
        let extensions = web_extensions(hosts);
        let mut runtime = JsRuntime::new(RuntimeOptions {
            module_loader: Some(loader),
            extensions,
            create_params: Some(create_params),
            ..Default::default()
        });
        let heap_exceeded = Arc::new(AtomicBool::new(false));
        let heap_flag = Arc::clone(&heap_exceeded);
        let heap_handle = runtime.v8_isolate().thread_safe_handle();
        runtime.add_near_heap_limit_callback(move |current, _initial| {
            heap_flag.store(true, Ordering::Release);
            heap_handle.terminate_execution();
            current.saturating_mul(2)
        });
        runtime
            .execute_script("peren:seal", FastString::from_static(SEAL_INTERNALS))
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        let environment = serde_json::to_string(&environment.values)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let entry_specifier = serde_json::to_string(specifier.as_str())
            .map_err(|error| EngineError::Request(error.to_string()))?;
        runtime
            .execute_script(
                "peren:env",
                FastString::from(format!(
                    "globalThis.__perenEntryModuleSpecifier = {entry_specifier}; globalThis.__perenEnv = globalThis.__perenHydrateEnv({environment})"
                )),
            )
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        let module = runtime
            .load_main_es_module(&specifier)
            .await
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        let watchdog = Watchdog::start(&mut runtime, limits.execution_time());
        let evaluation = runtime.mod_evaluate(module);
        let result = async {
            runtime
                .run_event_loop(PollEventLoopOptions::default())
                .await
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
            evaluation
                .await
                .map_err(|error| EngineError::JavaScript(error.to_string()))
        }
        .await;
        watchdog.finish(&mut runtime, &heap_exceeded)?;
        result?;

        let namespace = runtime
            .get_module_namespace(module)
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        let exports = namespace.clone();
        {
            deno_core::scope!(scope, runtime);
            let namespace = v8::Local::new(scope, namespace);
            let key = v8::String::new(scope, "default").ok_or(EngineError::Allocation)?;
            let entry = namespace
                .get(scope, key.into())
                .ok_or(EngineError::MissingEntrypoint)?;
            let global = scope.get_current_context().global(scope);
            let key = v8::String::new(scope, "__perenEntry").ok_or(EngineError::Allocation)?;
            if global.set(scope, key.into(), entry).is_none() {
                return Err(EngineError::Allocation);
            }
        }
        runtime
            .execute_script(
                "peren:entry",
                FastString::from_static(
                    "if ((globalThis.__perenEntry == null || typeof globalThis.__perenEntry.fetch !== 'function') && globalThis.__perenDispatchListener == null) throw new TypeError('default export must provide fetch');",
                ),
            )
            .map_err(|_| EngineError::MissingFetch)?;
        Ok(Self {
            runtime,
            limits,
            heap_exceeded,
            storage,
            exports,
        })
    }

    pub fn bind_durable_class(&mut self, class_name: &str) -> Result<(), EngineError> {
        deno_core::scope!(scope, self.runtime);
        let namespace = v8::Local::new(scope, &self.exports);
        let key = v8::String::new(scope, class_name).ok_or(EngineError::Allocation)?;
        let Some(class) = namespace.get(scope, key.into()) else {
            return Ok(());
        };
        if !class.is_function() {
            return Ok(());
        }
        let global = scope.get_current_context().global(scope);
        let slot = v8::String::new(scope, "__perenDurableClass").ok_or(EngineError::Allocation)?;
        if global.set(scope, slot.into(), class).is_none() {
            return Err(EngineError::Allocation);
        }
        Ok(())
    }

    pub fn committed_revision(&self) -> peren_primitives::StorageRevision {
        self.storage.as_ref().map_or_else(
            peren_primitives::StorageRevision::default,
            InvocationStorage::revision,
        )
    }

    pub(super) fn reset_storage(&self) {
        if let Some(storage) = &self.storage {
            storage.reset();
        }
    }

    pub(super) fn reset_console(&mut self) {
        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<crate::ops::console::ConsoleEvents>()
            .clear();
    }

    pub fn take_console_events(&mut self) -> Vec<crate::wire::WorkerLogEvent> {
        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<crate::ops::console::ConsoleEvents>()
            .take()
    }
}
