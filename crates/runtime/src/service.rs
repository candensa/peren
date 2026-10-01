use crate::{
    Capabilities, EngineError, HttpRequest, HttpResponse, InvocationLimits, IsolateLimits,
    PyodideRuntime, PythonRuntime, QueueDispatch, QueueEvent, ScheduledEvent, TailEvent,
    WebSocketCloseEvent, WebSocketDispatch, WebSocketMessageEvent, WorkerBundle, WorkerEnvironment,
    WorkerLogEvent, WorkerRuntime, WorkflowActivityEvent, WorkflowEvent,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RuntimeKind {
    #[default]
    JavaScript,
    Python(PythonEngine),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PythonEngine {
    Compat,
    #[default]
    Pyodide,
}

#[derive(Clone, Debug)]
pub struct RuntimePackage {
    kind: RuntimeKind,
    bundle: WorkerBundle,
}

impl RuntimePackage {
    #[must_use]
    pub const fn new(kind: RuntimeKind, bundle: WorkerBundle) -> Self {
        Self { kind, bundle }
    }

    #[must_use]
    pub const fn kind(&self) -> RuntimeKind {
        self.kind
    }

    #[must_use]
    pub const fn bundle(&self) -> &WorkerBundle {
        &self.bundle
    }

    #[must_use]
    pub fn into_bundle(self) -> WorkerBundle {
        self.bundle
    }
}

pub enum ServiceRuntime {
    JavaScript(WorkerRuntime),
    Python(PythonRuntime),
    Pyodide(PyodideRuntime),
}

impl ServiceRuntime {
    pub async fn load_with_environment(
        package: RuntimePackage,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        host: std::sync::Arc<dyn crate::DurableStorageHost>,
    ) -> Result<Self, EngineError> {
        match package.kind {
            RuntimeKind::JavaScript => Ok(Self::JavaScript(
                WorkerRuntime::load_with_environment(package.bundle, limits, environment, host)
                    .await?,
            )),
            RuntimeKind::Python(PythonEngine::Compat) => Ok(Self::Python(
                PythonRuntime::load_with_storage(package.bundle, limits, environment, host)?,
            )),
            RuntimeKind::Python(PythonEngine::Pyodide) => Ok(Self::Pyodide(
                PyodideRuntime::load_with_storage(package.bundle, limits, environment, host)
                    .await?,
            )),
        }
    }

    pub async fn load_with_r2(
        package: RuntimePackage,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: std::sync::Arc<dyn crate::DurableStorageHost>,
        r2: std::sync::Arc<dyn crate::R2BucketHost>,
    ) -> Result<Self, EngineError> {
        match package.kind {
            RuntimeKind::JavaScript => Ok(Self::JavaScript(
                WorkerRuntime::load_with_r2(package.bundle, limits, environment, storage, r2)
                    .await?,
            )),
            RuntimeKind::Python(PythonEngine::Compat) => Ok(Self::Python(
                PythonRuntime::load_with_r2(package.bundle, limits, environment, storage, r2)?,
            )),
            RuntimeKind::Python(PythonEngine::Pyodide) => Ok(Self::Pyodide(
                PyodideRuntime::load_with_r2(package.bundle, limits, environment, storage, r2)
                    .await?,
            )),
        }
    }

    pub async fn load_with_capabilities(
        package: RuntimePackage,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        capabilities: Capabilities,
    ) -> Result<Self, EngineError> {
        match package.kind {
            RuntimeKind::JavaScript => Ok(Self::JavaScript(
                WorkerRuntime::load_with_capabilities(
                    package.bundle,
                    limits,
                    environment,
                    capabilities,
                )
                .await?,
            )),
            RuntimeKind::Python(PythonEngine::Compat) => {
                Ok(Self::Python(PythonRuntime::load_with_capabilities(
                    package.bundle,
                    limits,
                    environment,
                    capabilities,
                )?))
            }
            RuntimeKind::Python(PythonEngine::Pyodide) => Ok(Self::Pyodide(
                PyodideRuntime::load_with_capabilities(
                    package.bundle,
                    limits,
                    environment,
                    capabilities,
                )
                .await?,
            )),
        }
    }

    pub fn bind_durable_class(&mut self, class_name: &str) -> Result<(), EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.bind_durable_class(class_name),
            Self::Python(runtime) => runtime.bind_durable_class(class_name),
            Self::Pyodide(runtime) => runtime.bind_durable_class(class_name),
        }
    }

    pub async fn dispatch_http(
        &mut self,
        request: HttpRequest,
        limits: InvocationLimits,
    ) -> Result<HttpResponse, EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_http(request, limits).await,
            Self::Python(runtime) => runtime.dispatch_http(request, limits),
            Self::Pyodide(runtime) => runtime.dispatch_http(request, limits).await,
        }
    }

    pub async fn dispatch_alarm(&mut self) -> Result<(), EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_alarm().await,
            Self::Python(runtime) => runtime.dispatch_alarm(),
            Self::Pyodide(runtime) => runtime.dispatch_alarm().await,
        }
    }

    pub async fn dispatch_scheduled(&mut self, event: ScheduledEvent) -> Result<(), EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_scheduled(event).await,
            Self::Python(runtime) => runtime.dispatch_scheduled(event),
            Self::Pyodide(runtime) => runtime.dispatch_scheduled(event).await,
        }
    }

    pub async fn dispatch_queue(
        &mut self,
        event: QueueEvent,
    ) -> Result<QueueDispatch, EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_queue(event).await,
            Self::Python(runtime) => runtime.dispatch_queue(event),
            Self::Pyodide(runtime) => runtime.dispatch_queue(event).await,
        }
    }

    pub async fn dispatch_tail(&mut self, event: TailEvent) -> Result<(), EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_tail(event).await,
            Self::Python(runtime) => runtime.dispatch_tail(event),
            Self::Pyodide(runtime) => runtime.dispatch_tail(event).await,
        }
    }

    pub async fn dispatch_websocket_message(
        &mut self,
        event: WebSocketMessageEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_websocket_message(event).await,
            Self::Python(runtime) => runtime.dispatch_websocket_message(event),
            Self::Pyodide(runtime) => runtime.dispatch_websocket_message(event).await,
        }
    }

    pub async fn dispatch_websocket_close(
        &mut self,
        event: WebSocketCloseEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_websocket_close(event).await,
            Self::Python(runtime) => runtime.dispatch_websocket_close(event),
            Self::Pyodide(runtime) => runtime.dispatch_websocket_close(event).await,
        }
    }

    pub async fn dispatch_workflow(&mut self, event: WorkflowEvent) -> Result<(), EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_workflow(event).await,
            Self::Python(runtime) => runtime.dispatch_workflow(event),
            Self::Pyodide(runtime) => runtime.dispatch_workflow(event).await,
        }
    }

    pub async fn dispatch_workflow_activity(
        &mut self,
        event: WorkflowActivityEvent,
    ) -> Result<serde_json::Value, EngineError> {
        match self {
            Self::JavaScript(runtime) => runtime.dispatch_workflow_activity(event).await,
            Self::Python(runtime) => runtime.dispatch_workflow_activity(event),
            Self::Pyodide(runtime) => runtime.dispatch_workflow_activity(event).await,
        }
    }

    #[must_use]
    pub fn committed_revision(&self) -> peren_primitives::StorageRevision {
        match self {
            Self::JavaScript(runtime) => runtime.committed_revision(),
            Self::Python(runtime) => runtime.committed_revision(),
            Self::Pyodide(runtime) => runtime.committed_revision(),
        }
    }

    pub fn take_console_events(&mut self) -> Vec<WorkerLogEvent> {
        match self {
            Self::JavaScript(runtime) => runtime.take_console_events(),
            Self::Python(runtime) => runtime.take_console_events(),
            Self::Pyodide(runtime) => runtime.take_console_events(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{Module, ModuleKind, ModuleName, WorkerBundle};

    use super::*;

    #[test]
    fn runtime_package_preserves_kind_and_bundle_digest() {
        let entry = ModuleName::parse("worker.js").unwrap();
        let bundle = WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                Module::new(
                    ModuleKind::JavaScript,
                    b"export default { fetch() { return new Response('ok'); } };".as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap();
        let digest = bundle.digest();

        let package = RuntimePackage::new(RuntimeKind::Python(PythonEngine::Compat), bundle);

        assert_eq!(package.kind(), RuntimeKind::Python(PythonEngine::Compat));
        assert_eq!(package.bundle().digest(), digest);
    }

    #[tokio::test]
    async fn pyodide_engine_refuses_without_wasm_executor() {
        let entry = ModuleName::parse("worker.py").unwrap();
        let bundle = WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                Module::new(
                    ModuleKind::Python,
                    b"def fetch(request, env, ctx): pass".as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap();
        let package = RuntimePackage::new(RuntimeKind::Python(PythonEngine::Pyodide), bundle);

        let result = ServiceRuntime::load_with_environment(
            package,
            crate::IsolateLimits::new(128 * 1024 * 1024, std::time::Duration::from_secs(30)),
            crate::WorkerEnvironment::empty(),
            std::sync::Arc::new(crate::empty::NoStorage),
        )
        .await;
        let Err(error) = result else {
            panic!("pyodide should refuse before compatibility fallback");
        };

        assert_eq!(
            error.to_string(),
            "python:pyodide runtime is not supported by this build"
        );
    }
}
