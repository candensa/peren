use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use deno_core::{
    FastString, FsModuleLoader, JsRuntime, OpState, PollEventLoopOptions, RuntimeOptions, op2, v8,
};

use crate::{
    Capabilities, EngineError, HttpRequest, HttpResponse, InvocationLimits, IsolateLimits,
    ModuleKind, PYTHON_PACKAGE_LOCK_MODULE, QueueDispatch, QueueEvent, ScheduledEvent, TailEvent,
    WebSocketCloseEvent, WebSocketDispatch, WebSocketMessageEvent, WorkerBundle, WorkerEnvironment,
    WorkflowActivityEvent, WorkflowEvent,
    empty::{NoAi, NoCache, NoDurableObject, NoFetch, NoKv, NoQueue, NoR2, NoService},
    host::{DurableStorageHost, R2BucketHost},
    isolate::{InvocationHosts, Watchdog, web_extensions},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PyodideArtifacts {
    root: PathBuf,
}

impl PyodideArtifacts {
    #[must_use]
    pub fn from_env() -> Option<Self> {
        std::env::var_os("PEREN_PYODIDE_ROOT").map(|root| Self {
            root: PathBuf::from(root),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub fn validate(&self) -> Result<(), EngineError> {
        for artifact in [
            "pyodide.mjs",
            "pyodide.asm.js",
            "pyodide.asm.wasm",
            "python_stdlib.zip",
            "pyodide-lock.json",
        ] {
            let path = self.root.join(artifact);
            if !path.is_file() {
                return Err(EngineError::Python(format!(
                    "Pyodide executor requires {}; set PEREN_PYODIDE_ROOT to a Pyodide distribution",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}

pub struct PyodideRuntime {
    runtime: JsRuntime,
    limits: IsolateLimits,
    heap_exceeded: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PythonPackageLock {
    pub version: u8,
    #[serde(default)]
    pub packages: Vec<String>,
}

impl PythonPackageLock {
    pub fn parse(source: &[u8]) -> Result<Self, EngineError> {
        let lock = serde_json::from_slice::<Self>(source).map_err(|error| {
            EngineError::Python(format!("invalid Python package lock: {error}"))
        })?;
        lock.validate()?;
        Ok(lock)
    }

    fn validate(&self) -> Result<(), EngineError> {
        if self.version != 1 {
            return Err(EngineError::Python(format!(
                "unsupported Python package lock version {}; expected 1",
                self.version
            )));
        }
        let mut previous: Option<&str> = None;
        for package in &self.packages {
            if !is_pyodide_package_name(package) {
                return Err(EngineError::Python(format!(
                    "Python package {package:?} must be a Pyodide package name, not a URL, path, wheel, or version specifier"
                )));
            }
            if previous.is_some_and(|value| value >= package.as_str()) {
                return Err(EngineError::Python(
                    "Python package lock packages must be sorted and unique".into(),
                ));
            }
            previous = Some(package);
        }
        Ok(())
    }
}

fn is_pyodide_package_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        && !value.starts_with('.')
        && !std::path::Path::new(value)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("whl"))
        && !value.contains("..")
}

impl PyodideRuntime {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "runtime adapters consume owned packages at the shared service-runtime boundary"
    )]
    pub async fn load(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
    ) -> Result<Self, EngineError> {
        Self::load_inner(bundle, limits, environment, InvocationHosts::empty()).await
    }

    pub async fn load_with_storage(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: Arc<dyn DurableStorageHost>,
    ) -> Result<Self, EngineError> {
        Self::load_with_capabilities(
            bundle,
            limits,
            environment,
            Capabilities {
                storage,
                fetch: Some(Arc::new(NoFetch)),
                queue: Some(Arc::new(NoQueue)),
                r2: Some(Arc::new(NoR2)),
                service: Some(Arc::new(NoService)),
                durable: Some(Arc::new(NoDurableObject)),
                cache: Some(Arc::new(NoCache)),
                kv: Some(Arc::new(NoKv)),
                ai: Some(Arc::new(NoAi)),
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
        Self::load_with_capabilities(
            bundle,
            limits,
            environment,
            Capabilities {
                storage,
                fetch: Some(Arc::new(NoFetch)),
                queue: Some(Arc::new(NoQueue)),
                r2: Some(r2),
                service: Some(Arc::new(NoService)),
                durable: Some(Arc::new(NoDurableObject)),
                cache: Some(Arc::new(NoCache)),
                kv: Some(Arc::new(NoKv)),
                ai: Some(Arc::new(NoAi)),
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
            InvocationHosts::from_capabilities(capabilities),
        )
        .await
    }

    async fn load_inner(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        hosts: InvocationHosts,
    ) -> Result<Self, EngineError> {
        let Some(artifacts) = PyodideArtifacts::from_env() else {
            return Err(EngineError::UnsupportedRuntime("python:pyodide"));
        };
        artifacts.validate()?;
        let root = artifacts.root().to_path_buf();
        let create_params =
            v8::CreateParams::default().set_max_old_generation_size_in_bytes(limits.heap_bytes());
        let mut extensions = web_extensions(hosts);
        extensions.push(pyodide_files::init(PyodideFileRoot { root: root.clone() }));
        let mut runtime = JsRuntime::new(RuntimeOptions {
            module_loader: Some(Rc::new(FsModuleLoader)),
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

        install_shell_file_api(&mut runtime, &root)?;
        bootstrap_pyodide(&mut runtime, &root, bundle, environment).await?;
        Ok(Self {
            runtime,
            limits,
            heap_exceeded,
        })
    }

    #[allow(
        clippy::unused_self,
        reason = "dispatch methods mirror the shared runtime interface before the executor stores state"
    )]
    pub fn bind_durable_class(&mut self, _class_name: &str) -> Result<(), EngineError> {
        Self::unsupported("durable class binding")
    }

    #[allow(
        clippy::unused_self,
        reason = "dispatch methods mirror the shared runtime interface before the executor stores state"
    )]
    pub async fn dispatch_http(
        &mut self,
        request: HttpRequest,
        limits: InvocationLimits,
    ) -> Result<HttpResponse, EngineError> {
        let body_bytes = u64::try_from(request.body.len()).unwrap_or(u64::MAX);
        limits.validate_request(body_bytes)?;
        let started = Instant::now();
        let request = serde_json::to_string(&request)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let promise = self
            .runtime
            .execute_script(
                "peren:pyodide:http",
                FastString::from(format!("globalThis.__perenPyodideDispatchHttp({request})")),
            )
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            #[allow(
                deprecated,
                reason = "deno_core has no replacement for resolving an arbitrary value"
            )]
            self.runtime
                .resolve_value(promise)
                .await
                .map_err(|error| EngineError::JavaScript(error.to_string()))
        }
        .await;
        watchdog.finish(&mut self.runtime, &self.heap_exceeded)?;
        limits.validate_cpu_time(started.elapsed())?;
        let value = result?;
        deno_core::scope!(scope, self.runtime);
        let value = v8::Local::new(scope, value);
        let response = deno_core::serde_v8::from_v8::<HttpResponse>(scope, value)
            .map_err(|error| EngineError::Response(error.to_string()))?;
        let body_bytes = u64::try_from(response.body.len()).unwrap_or(u64::MAX);
        limits.validate_response(body_bytes)?;
        Ok(response)
    }

    pub async fn dispatch_alarm(&mut self) -> Result<(), EngineError> {
        self.dispatch_event("alarm", serde_json::Value::Null)
            .await?;
        Ok(())
    }

    pub async fn dispatch_scheduled(&mut self, event: ScheduledEvent) -> Result<(), EngineError> {
        self.dispatch_event("scheduled", to_json(event)?).await?;
        Ok(())
    }

    pub async fn dispatch_queue(
        &mut self,
        event: QueueEvent,
    ) -> Result<QueueDispatch, EngineError> {
        let value = self.dispatch_event("queue", to_json(event)?).await?;
        Ok(QueueDispatch {
            dispositions: serde_json::from_value(
                value
                    .get("dispositions")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
            )
            .map_err(|error| EngineError::Response(error.to_string()))?,
        })
    }

    pub async fn dispatch_tail(&mut self, event: TailEvent) -> Result<(), EngineError> {
        self.dispatch_event("tail", to_json(event)?).await?;
        Ok(())
    }

    pub async fn dispatch_websocket_message(
        &mut self,
        event: WebSocketMessageEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        let value = self
            .dispatch_event("websocketMessage", to_json(event)?)
            .await?;
        serde_json::from_value(value).map_err(|error| EngineError::Response(error.to_string()))
    }

    pub async fn dispatch_websocket_close(
        &mut self,
        event: WebSocketCloseEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        let value = self
            .dispatch_event("websocketClose", to_json(event)?)
            .await?;
        serde_json::from_value(value).map_err(|error| EngineError::Response(error.to_string()))
    }

    pub async fn dispatch_workflow(&mut self, event: WorkflowEvent) -> Result<(), EngineError> {
        self.dispatch_event("workflow", to_json(event)?).await?;
        Ok(())
    }

    pub async fn dispatch_workflow_activity(
        &mut self,
        event: WorkflowActivityEvent,
    ) -> Result<serde_json::Value, EngineError> {
        self.dispatch_event("activity", to_json(event)?).await
    }

    #[allow(
        clippy::unused_self,
        reason = "dispatch methods mirror the shared runtime interface before the executor stores state"
    )]
    #[must_use]
    pub const fn committed_revision(&self) -> peren_primitives::StorageRevision {
        peren_primitives::StorageRevision::new(0)
    }

    #[allow(
        clippy::unused_self,
        reason = "dispatch methods mirror the shared runtime interface before the executor stores state"
    )]
    pub fn take_console_events(&mut self) -> Vec<crate::WorkerLogEvent> {
        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<crate::ops::console::ConsoleEvents>()
            .take()
    }

    async fn dispatch_event(
        &mut self,
        kind: &str,
        event: serde_json::Value,
    ) -> Result<serde_json::Value, EngineError> {
        let kind =
            serde_json::to_string(kind).map_err(|error| EngineError::Request(error.to_string()))?;
        let event = serde_json::to_string(&event)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let promise = self
            .runtime
            .execute_script(
                "peren:pyodide:event",
                FastString::from(format!(
                    "globalThis.__perenPyodideDispatch({kind}, {event})"
                )),
            )
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            #[allow(
                deprecated,
                reason = "deno_core has no replacement for resolving an arbitrary value"
            )]
            self.runtime
                .resolve_value(promise)
                .await
                .map_err(|error| EngineError::JavaScript(error.to_string()))
        }
        .await;
        watchdog.finish(&mut self.runtime, &self.heap_exceeded)?;
        let value = result?;
        deno_core::scope!(scope, self.runtime);
        let value = v8::Local::new(scope, value);
        deno_core::serde_v8::from_v8(scope, value)
            .map_err(|error| EngineError::Response(error.to_string()))
    }

    fn unsupported<T>(feature: &'static str) -> Result<T, EngineError> {
        Err(EngineError::UnsupportedRuntimeFeature {
            runtime: "python:pyodide",
            feature,
        })
    }
}

fn to_json<T: serde::Serialize>(value: T) -> Result<serde_json::Value, EngineError> {
    serde_json::to_value(value).map_err(|error| EngineError::Request(error.to_string()))
}

fn install_shell_file_api(runtime: &mut JsRuntime, root: &Path) -> Result<(), EngineError> {
    let location = root.join("pyodide.mjs");
    let location = deno_core::url::Url::from_file_path(location)
        .map_err(|()| EngineError::Python("Pyodide artifact path is not a file URL".into()))?;
    let source = format!(
        r#"
globalThis.location = {location:?};
globalThis.read = (path) => Deno.core.ops.op_pyodide_read_text(String(path));
globalThis.readbuffer = (path) => {{
  const bytes = Deno.core.ops.op_pyodide_read_binary(String(path));
  return new Uint8Array(bytes).buffer;
}};
globalThis.load = (path) => {{
  globalThis.eval(`${{Deno.core.ops.op_pyodide_read_text(String(path))}}\n//# sourceURL=${{String(path)}}`);
}};
globalThis.os = Object.freeze({{
  getRandomValues: (view) => globalThis.crypto.getRandomValues(view),
  system: (command, args) => {{
    const shell = Array.isArray(args) ? args.join(" ") : "";
    const match = shell.match(/head -c(\d+) \/dev\/urandom \| base64 --wrap=0/);
    if (command !== "sh" || match === null) throw new Error("unsupported Pyodide shell command");
    const bytes = Deno.core.ops.op_crypto_random(Number(match[1]));
    let binary = "";
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return btoa(binary);
  }},
}});
"#,
        location = location.as_str()
    );
    runtime
        .execute_script("peren:pyodide:shell", FastString::from(source))
        .map_err(|error| EngineError::JavaScript(error.to_string()))?;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "Pyodide bootstrap keeps the JS host bridge and Python runner wiring in one reviewable boundary"
)]
async fn bootstrap_pyodide(
    runtime: &mut JsRuntime,
    root: &Path,
    bundle: WorkerBundle,
    environment: WorkerEnvironment,
) -> Result<(), EngineError> {
    let root_url = deno_core::url::Url::from_directory_path(root)
        .map_err(|()| EngineError::Python("Pyodide artifact root is not a directory URL".into()))?;
    let pyodide_url = deno_core::url::Url::from_file_path(root.join("pyodide.mjs"))
        .map_err(|()| EngineError::Python("Pyodide artifact path is not a file URL".into()))?;
    let entry = bundle.entry().to_string();
    let packages = bundle
        .module(&crate::ModuleName::parse(PYTHON_PACKAGE_LOCK_MODULE)?)
        .map(|module| PythonPackageLock::parse(module.source()).map(|lock| lock.packages))
        .transpose()?
        .unwrap_or_default();
    let packages = serde_json::to_string(&packages)
        .map_err(|error| EngineError::Request(error.to_string()))?;
    let modules = bundle
        .modules()
        .filter(|(_, module)| module.kind() == ModuleKind::Python)
        .map(|(name, module)| {
            let source = std::str::from_utf8(module.source())
                .map_err(|error| EngineError::Python(error.to_string()))?;
            Ok((name.to_string(), source.to_string()))
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    let modules =
        serde_json::to_string(&modules).map_err(|error| EngineError::Request(error.to_string()))?;
    let env = serde_json::to_string(environment.values())
        .map_err(|error| EngineError::Request(error.to_string()))?;
    let source = format!(
        r#"
const {{ loadPyodide }} = await import({pyodide_url});
globalThis.__perenPyodideLogs = [];
globalThis.__perenPyodide = await loadPyodide({{
  indexURL: {root_url},
  fullStdLib: true,
  stdout: (line) => globalThis.__perenPyodideLogs.push({{ level: "Info", message: String(line) }}),
  stderr: (line) => globalThis.__perenPyodideLogs.push({{ level: "Error", message: String(line) }}),
}});
globalThis.__perenPyodideModules = {modules};
globalThis.__perenPyodideEnv = {env};
globalThis.__perenPyodideEntry = {entry:?};
globalThis.__perenPyodidePackages = {packages};
if (globalThis.__perenPyodidePackages.length > 0) {{
  await globalThis.__perenPyodide.loadPackage(globalThis.__perenPyodidePackages);
}}
for (const [name, source] of globalThis.__perenPyodideModules) {{
  const path = "/" + name;
  const parent = path.slice(0, path.lastIndexOf("/"));
  if (parent) globalThis.__perenPyodide.FS.mkdirTree(parent);
  globalThis.__perenPyodide.FS.writeFile(path, source);
}}
globalThis.__perenPyodide.globals.set("__perenPyodideEntry", globalThis.__perenPyodideEntry);
globalThis.__perenPyodide.globals.set("__perenPyodideEnvJson", JSON.stringify(globalThis.__perenPyodideEnv));
globalThis.__perenPyodideHostCall = async (op, payloadJson) => {{
  const payload = JSON.parse(payloadJson || "{{}}");
  let result;
  switch (op) {{
    case "storage_begin": result = await Deno.core.ops.op_storage_begin(); break;
    case "storage_get": result = await Deno.core.ops.op_storage_get(payload.scope, payload.key); break;
    case "storage_put": result = await Deno.core.ops.op_storage_put(payload.scope, payload.key, payload.value); break;
    case "storage_delete": result = await Deno.core.ops.op_storage_delete(payload.scope, payload.key); break;
    case "storage_list": result = await Deno.core.ops.op_storage_list(payload.scope, payload.options); break;
    case "storage_sql": result = await Deno.core.ops.op_storage_sql(payload); break;
    case "storage_commit": result = await Deno.core.ops.op_storage_commit(); break;
    case "storage_rollback": result = await Deno.core.ops.op_storage_rollback(); break;
    case "kv_get":
      result = payload.provider?.kind === "native"
        ? await Deno.core.ops.op_storage_get(payload.namespace, Array.from(new TextEncoder().encode(payload.key)))
        : await Deno.core.ops.op_kv_get({{ namespace: payload.namespace, key: payload.key }});
      break;
    case "kv_put":
      if (payload.provider?.kind === "native") {{
        await Deno.core.ops.op_storage_begin();
        try {{
          await Deno.core.ops.op_storage_put(payload.namespace, Array.from(new TextEncoder().encode(payload.key)), payload.value);
          await Deno.core.ops.op_storage_commit();
        }} catch (error) {{
          await Deno.core.ops.op_storage_rollback();
          throw error;
        }}
        result = null;
      }} else {{
        result = await Deno.core.ops.op_kv_put({{ namespace: payload.namespace, key: payload.key, value: payload.value }});
      }}
      break;
    case "kv_delete":
      if (payload.provider?.kind === "native") {{
        await Deno.core.ops.op_storage_begin();
        try {{
          await Deno.core.ops.op_storage_delete(payload.namespace, Array.from(new TextEncoder().encode(payload.key)));
          await Deno.core.ops.op_storage_commit();
        }} catch (error) {{
          await Deno.core.ops.op_storage_rollback();
          throw error;
        }}
        result = true;
      }} else {{
        result = await Deno.core.ops.op_kv_delete({{ namespace: payload.namespace, key: payload.key }});
      }}
      break;
    case "kv_list":
      result = payload.provider?.kind === "native"
        ? await Deno.core.ops.op_storage_list(payload.namespace, {{ prefix: payload.prefix == null ? null : Array.from(new TextEncoder().encode(payload.prefix)), cursor: payload.cursor == null ? null : Array.from(new TextEncoder().encode(payload.cursor)), limit: payload.limit ?? null }})
        : await Deno.core.ops.op_kv_list({{ namespace: payload.namespace, prefix: payload.prefix ?? null, cursor: payload.cursor ?? null, limit: payload.limit ?? null }});
      break;
    case "r2_put": result = await Deno.core.ops.op_r2_put(payload); break;
    case "r2_get": result = await Deno.core.ops.op_r2_get(payload); break;
    case "r2_delete": result = await Deno.core.ops.op_r2_delete(payload); break;
    case "r2_list": result = await Deno.core.ops.op_r2_list(payload); break;
    case "queue_send": result = await Deno.core.ops.op_queue_send(payload); break;
    case "outbound_fetch": result = await Deno.core.ops.op_outbound_fetch(payload); break;
    case "service_fetch": result = await Deno.core.ops.op_service_fetch(payload); break;
    case "durable_object_fetch": result = await Deno.core.ops.op_durable_object_fetch(payload); break;
    case "cache_match": result = await Deno.core.ops.op_cache_match(payload); break;
    case "cache_put": result = await Deno.core.ops.op_cache_put(payload); break;
    case "cache_delete": result = await Deno.core.ops.op_cache_delete(payload); break;
    case "ai_run": result = await Deno.core.ops.op_ai_run(payload); break;
    case "d1_query": result = await Deno.core.ops.op_storage_sql({{ database: payload.database, sql: payload.sql, parameters: payload.parameters ?? [] }}); break;
    case "d1_exec": result = await Deno.core.ops.op_storage_sql({{ database: payload.database, sql: payload.sql, parameters: [] }}); break;
    case "workflow_id": result = `workflow-${{Date.now()}}-${{Math.random().toString(16).slice(2)}}`; break;
    case "workflow_status": {{
      const key = `${{payload.binding}}/${{payload.id}}.json`;
      const bytes = await Deno.core.ops.op_storage_get("workflows", Array.from(new TextEncoder().encode(key)));
      result = bytes == null ? {{ id: payload.id, status: "unknown" }} : JSON.parse(new TextDecoder().decode(new Uint8Array(bytes)));
      break;
    }}
    case "workflow_write": {{
      const state = {{ id: payload.id, status: payload.status, reason: payload.reason ?? null, updatedAt: Date.now() }};
      const key = `${{payload.binding}}/${{payload.id}}.json`;
      await Deno.core.ops.op_storage_begin();
      try {{
        await Deno.core.ops.op_storage_put("workflows", Array.from(new TextEncoder().encode(key)), Array.from(new TextEncoder().encode(JSON.stringify(state))));
        await Deno.core.ops.op_storage_commit();
      }} catch (error) {{
        await Deno.core.ops.op_storage_rollback();
        throw error;
      }}
      result = state;
      break;
    }}
    default: throw new Error(`unsupported Pyodide host operation: ${{op}}`);
  }}
  return JSON.stringify(result ?? null);
}};
globalThis.__perenPyodide.globals.set("__peren_host_call_json", globalThis.__perenPyodideHostCall);
await globalThis.__perenPyodide.runPythonAsync({runner:?});
globalThis.__perenPyodideLogs.length = 0;
globalThis.__perenPyodideDispatch = async (kind, event) => {{
  const resultJson = await globalThis.__perenPyodide.globals.get("__peren_dispatch")(kind, JSON.stringify(event));
  const logs = globalThis.__perenPyodideLogs.splice(0);
  for (const log of logs) Deno.core.ops.op_console_log(log.level, log.message);
  return JSON.parse(resultJson);
}};
globalThis.__perenPyodideDispatchHttp = async (request) => {{
  return await globalThis.__perenPyodideDispatch("fetch", request);
}};
"#,
        pyodide_url = serde_json::to_string(pyodide_url.as_str())
            .map_err(|error| EngineError::Request(error.to_string()))?,
        root_url = serde_json::to_string(root_url.as_str())
            .map_err(|error| EngineError::Request(error.to_string()))?,
        modules = modules,
        env = env,
        packages = packages,
        entry = entry,
        runner = PYODIDE_RUNNER,
    );
    let module = deno_core::resolve_url("peren:pyodide:bootstrap")
        .map_err(|error| EngineError::JavaScript(error.to_string()))?;
    let id = runtime
        .load_side_es_module_from_code(&module, FastString::from(source))
        .await
        .map_err(|error| EngineError::JavaScript(error.to_string()))?;
    let evaluation = runtime.mod_evaluate(id);
    runtime
        .run_event_loop(PollEventLoopOptions::default())
        .await
        .map_err(|error| EngineError::JavaScript(error.to_string()))?;
    evaluation
        .await
        .map_err(|error| EngineError::JavaScript(error.to_string()))
}

const PYODIDE_RUNNER: &str = r#"
import asyncio, importlib, importlib.abc, json, sys, types
sys.path.insert(0, "/")

BLOCKED_MODULES = {
    "curses",
    "dbm",
    "ensurepip",
    "fcntl",
    "grp",
    "idlelib",
    "lib2to3",
    "multiprocessing",
    "msvcrt",
    "pty",
    "pwd",
    "resource",
    "socket",
    "subprocess",
    "syslog",
    "termios",
    "threading",
    "tkinter",
    "turtle",
    "turtledemo",
    "tty",
    "venv",
    "webbrowser",
    "winreg",
    "winsound",
}

class BlockedModuleImporter(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        root = fullname.split(".", 1)[0]
        if root in BLOCKED_MODULES:
            raise ModuleNotFoundError(f"No module named '{fullname}'")
        return None

sys.meta_path.insert(0, BlockedModuleImporter())
for _name in list(sys.modules):
    if _name.split(".", 1)[0] in BLOCKED_MODULES:
        del sys.modules[_name]

async def host_call(op, payload=None):
    return json.loads(await __peren_host_call_json(op, json.dumps(payload or {})))

def bytes_body(value):
    if value is None: return []
    if isinstance(value, bytes): return list(value)
    if isinstance(value, bytearray): return list(bytes(value))
    if isinstance(value, str): return list(value.encode("utf-8"))
    return list(json.dumps(value).encode("utf-8"))

class Headers:
    def __init__(self, values=None):
        self._values = {}
        for name, value in values or []:
            self._values[str(name).lower()] = str(value)
    def get(self, name, default=None): return self._values.get(str(name).lower(), default)
    def items(self): return list(self._values.items())

class Request:
    def __init__(self, value):
        self.method = value.get("method", "GET")
        self.url = value.get("url", "")
        self.headers = Headers(value.get("headers", []))
        self.body = bytes(value.get("body", []))
    async def text(self): return self.body.decode("utf-8")
    async def json(self): return json.loads(await self.text())

class ResponseJson:
    def __get__(self, instance, owner):
        if instance is None:
            def create(value, status=200, headers=None):
                merged = {"content-type": "application/json"}
                if headers: merged.update(dict(headers))
                return owner(json.dumps(value), status=status, headers=merged)
            return create
        async def parse(): return json.loads(await instance.text())
        return parse

class Response:
    def __init__(self, body=b"", status=200, headers=None):
        if isinstance(body, str): body = body.encode("utf-8")
        self.body = bytes(body or b"")
        self.status = int(status)
        values = headers.items() if isinstance(headers, dict) else (headers or [])
        self.headers = Headers(values)
    json = ResponseJson()
    async def text(self): return self.body.decode("utf-8")

def response_from_host(value):
    return Response(bytes(value.get("body", [])), status=value.get("status", 200), headers=value.get("headers", []))

def encode_request(request):
    return {"method": request.method, "url": request.url, "headers": list(request.headers.items()), "body": list(request.body), "mtls": None}

class Context:
    def __init__(self): self._background = []
    def waitUntil(self, value): self._background.append(value)
    async def drain(self): await asyncio.gather(*[item for item in self._background if hasattr(item, "__await__")])

class WorkerEntrypoint:
    def __init__(self, env=None, ctx=None): self.env, self.ctx = env, ctx

class QueueMessage:
    def __init__(self, value):
        self.id, self.timestamp, self.attempts = value.get("id"), value.get("timestamp"), value.get("attempts", 1)
        raw = value.get("body", b"")
        self.body = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
        self._dispositions = []
    async def text(self): return self.body.decode("utf-8")
    async def json(self): return json.loads(await self.text())
    def ack(self): self._dispositions.append({"id": self.id, "outcome": "ack"})
    def retry(self, options=None):
        options = options or {}
        self._dispositions.append({"id": self.id, "outcome": "retry", "delaySeconds": options.get("delaySeconds"), "dedupId": options.get("dedupId")})
    def dispositions(self): return list(self._dispositions)

class QueueBatch:
    def __init__(self, value):
        self.queue = value.get("queue")
        self.messages = [QueueMessage(message) for message in value.get("messages", [])]
    def ackAll(self):
        for message in self.messages: message.ack()
    def retryAll(self, options=None):
        for message in self.messages: message.retry(options)
    def dispositions(self): return [d for message in self.messages for d in message.dispositions()]

class Scheduled:
    def __init__(self, value): self.cron, self.scheduledTime = value.get("cron"), value.get("scheduledTime")
class WorkflowEvent:
    def __init__(self, value): self.instance, self.payload = value.get("instance"), value.get("payload")
class WorkflowActivityEvent:
    def __init__(self, value): self.instance, self.name, self.task, self.payload = value.get("instance"), value.get("name"), value.get("task"), value.get("payload")

class WebSocket:
    CONNECTING, OPEN, CLOSING, CLOSED = 0, 1, 2, 3
    def __init__(self, id): self.id, self.readyState, self._outbound = str(id), WebSocket.OPEN, []
    def accept(self): self.readyState = WebSocket.OPEN
    def send(self, message):
        if self.readyState != WebSocket.OPEN: raise RuntimeError("WebSocket is not open")
        self._outbound.append(str(message))
    def close(self, code=1000, reason=""): self.readyState = WebSocket.CLOSED
    def drain(self):
        outbound, self._outbound = self._outbound, []
        return outbound

class WebSocketClose:
    def __init__(self, value): self.code, self.reason, self.wasClean = int(value.get("code", 1000)), value.get("reason", ""), bool(value.get("wasClean", False))

class KvNamespace:
    def __init__(self, scope, provider=None): self.scope, self.provider = str(scope), provider or {"kind": "native"}
    async def get(self, key, options=None):
        value = await host_call("kv_get", {"namespace": self.scope, "key": str(key), "provider": self.provider})
        if value is None: return None
        raw, kind = bytes(value), (options or {}).get("type", "text")
        if kind == "json": return json.loads(raw.decode("utf-8"))
        if kind in ("bytes", "arrayBuffer"): return raw
        return raw.decode("utf-8")
    async def put(self, key, value, options=None): await host_call("kv_put", {"namespace": self.scope, "key": str(key), "value": bytes_body(value), "provider": self.provider})
    async def delete(self, key): return await host_call("kv_delete", {"namespace": self.scope, "key": str(key), "provider": self.provider})
    async def list(self, options=None):
        options = options or {}
        return await host_call("kv_list", {"namespace": self.scope, "prefix": options.get("prefix"), "cursor": options.get("cursor"), "limit": options.get("limit"), "provider": self.provider})

class R2Object:
    def __init__(self, value):
        self.key, self.size = value.get("key"), value.get("size", len(value.get("body", [])))
        self.httpMetadata, self.customMetadata = types.SimpleNamespace(contentType=value.get("contentType")), value.get("customMetadata", {})
        self._body = bytes(value.get("body", []))
    async def text(self): return self._body.decode("utf-8")
    async def json(self): return json.loads(await self.text())
    async def arrayBuffer(self): return self._body

class R2Bucket:
    def __init__(self, bucket, prefix="", provider=None): self.bucket, self.prefix, self.provider = str(bucket), str(prefix or ""), provider or {"kind": "memory"}
    def _key(self, key): return self.prefix + str(key)
    async def put(self, key, body, options=None):
        options = options or {}
        await host_call("r2_put", {"bucket": self.bucket, "key": self._key(key), "body": bytes_body(body), "contentType": (options.get("httpMetadata") or {}).get("contentType"), "customMetadata": options.get("customMetadata") or {}})
    async def get(self, key):
        value = await host_call("r2_get", {"bucket": self.bucket, "key": self._key(key)})
        return None if value is None else R2Object(value)
    async def delete(self, key):
        for item in (key if isinstance(key, list) else [key]): await host_call("r2_delete", {"bucket": self.bucket, "key": self._key(item)})
    async def list(self, options=None):
        options = options or {}
        return await host_call("r2_list", {"bucket": self.bucket, "prefix": self._key(options.get("prefix") or ""), "cursor": options.get("cursor"), "limit": options.get("limit")})

class Queue:
    def __init__(self, queue, provider=None): self.queue, self.provider = str(queue), provider or {"kind": "memory"}
    async def send(self, body, options=None):
        options = options or {}
        await host_call("queue_send", {"queue": self.queue, "body": bytes_body(body), "contentType": options.get("contentType"), "delaySeconds": options.get("delaySeconds"), "dedupId": options.get("dedupId"), "partition": options.get("partition")})
    async def sendBatch(self, messages):
        for message in messages or []: await self.send(message.get("body"), message)

class D1PreparedStatement:
    def __init__(self, database, sql, parameters=None): self.database, self.sql, self.parameters = database, str(sql), parameters or []
    def bind(self, *parameters): return D1PreparedStatement(self.database, self.sql, list(parameters))
    async def all(self):
        result = await host_call("d1_query", {"database": self.database, "sql": self.sql, "parameters": self.parameters})
        columns = result.get("columns", [])
        rows = [dict(zip(columns, [_sql_value(value) for value in row])) for row in result.get("rows", [])]
        return {"success": True, "results": rows, "raw": result.get("rows", []), "meta": {"changes": result.get("changes"), "last_row_id": result.get("lastInsertRowid")}}
    async def run(self): return await self.all()
    async def first(self, column=None):
        rows = (await self.all()).get("results", [])
        return None if not rows else (rows[0] if column is None else rows[0].get(column))
    async def raw(self): return (await self.all()).get("raw", [])

class D1Database:
    def __init__(self, database, provider=None): self.database, self.provider = str(database), provider or {"kind": "native_sqlite"}
    def prepare(self, sql): return D1PreparedStatement(self.database, sql)
    async def exec(self, sql): return await host_call("d1_exec", {"database": self.database, "sql": str(sql)})

def _sql_value(value):
    if not isinstance(value, dict) or "type" not in value: return value
    kind = value.get("type")
    if kind == "null": return None
    if kind in ("integer", "real", "text", "blob"): return value.get("value")
    return value

class ServiceBinding:
    def __init__(self, service): self.service = str(service)
    async def fetch(self, input, init=None):
        request = input if isinstance(input, Request) else Request({"url": str(input), "method": (init or {}).get("method", "GET"), "headers": list(((init or {}).get("headers") or {}).items()) if isinstance((init or {}).get("headers"), dict) else ((init or {}).get("headers") or []), "body": bytes_body((init or {}).get("body"))})
        return response_from_host(await host_call("service_fetch", {"service": self.service, "request": encode_request(request)}))

class DurableObjectId:
    def __init__(self, namespace, value, name=None): self.namespace, self.value, self.name = namespace, value, name
    def __str__(self): return f"{self.namespace}:{self.value}"
class DurableObjectStub:
    def __init__(self, id, class_name): self.id, self.className, self.name = id, class_name, id.name
    async def fetch(self, input, init=None):
        request = input if isinstance(input, Request) else Request({"url": str(input), "method": (init or {}).get("method", "GET"), "headers": list(((init or {}).get("headers") or {}).items()) if isinstance((init or {}).get("headers"), dict) else ((init or {}).get("headers") or []), "body": bytes_body((init or {}).get("body"))})
        return response_from_host(await host_call("durable_object_fetch", {"namespace": self.id.namespace, "id": self.id.value, "name": self.id.name, "className": self.className, "request": encode_request(request)}))
class DurableObjectNamespace:
    def __init__(self, binding, class_name): self.binding, self.className = binding, class_name
    def idFromName(self, name): return DurableObjectId(self.binding, "name:" + str(name), str(name))
    def idFromString(self, id):
        text, prefix = str(id), self.binding + ":"
        if not text.startswith(prefix): raise TypeError("Durable Object id belongs to a different namespace")
        raw = text[len(prefix):]
        return DurableObjectId(self.binding, raw, raw[5:] if raw.startswith("name:") else None)
    def get(self, id): return DurableObjectStub(id, self.className)

class Cache:
    def __init__(self, name="default"): self.name = name
    async def match(self, request):
        value = await host_call("cache_match", {"cache": self.name, "key": request.url if isinstance(request, Request) else str(request)})
        return None if value is None else response_from_host(value)
    async def put(self, request, response): await host_call("cache_put", {"cache": self.name, "key": request.url if isinstance(request, Request) else str(request), "status": response.status, "headers": list(response.headers.items()), "body": list(response.body)})
    async def delete(self, request): return await host_call("cache_delete", {"cache": self.name, "key": request.url if isinstance(request, Request) else str(request)})
class CacheStorage:
    def __init__(self): self.default = Cache("default")
    async def open(self, name): return Cache(str(name))
class Ai:
    async def run(self, model, input, options=None): return await host_call("ai_run", {"command": "run", "model": str(model), "input": input, "options": options or {}})
class Workflow:
    def __init__(self, binding, id): self.binding, self.id = binding, id
    async def status(self): return await host_call("workflow_status", {"binding": self.binding, "id": self.id})
    async def terminate(self, reason=None): return await host_call("workflow_write", {"binding": self.binding, "id": self.id, "status": "terminated", "reason": reason})
    async def restart(self): return await host_call("workflow_write", {"binding": self.binding, "id": self.id, "status": "running"})
class WorkflowBinding:
    def __init__(self, binding): self.binding = str(binding)
    async def create(self, options=None):
        options = options or {}
        id = options.get("id") or str(await host_call("workflow_id", {}))
        await host_call("workflow_write", {"binding": self.binding, "id": id, "status": "running"})
        return Workflow(self.binding, id)
    def get(self, id): return Workflow(self.binding, str(id))

async def fetch(input, init=None):
    request = input if isinstance(input, Request) else Request({"url": str(input), "method": (init or {}).get("method", "GET"), "headers": list(((init or {}).get("headers") or {}).items()) if isinstance((init or {}).get("headers"), dict) else ((init or {}).get("headers") or []), "body": bytes_body((init or {}).get("body"))})
    return response_from_host(await host_call("outbound_fetch", encode_request(request)))

class Env:
    def __init__(self, values):
        raw = dict(values)
        bindings = json.loads(raw.pop("__perenBindings", "{}"))
        raw.pop("__perenCache", None)
        self._values = raw
        for name, binding in bindings.items():
            kind = binding.get("type")
            if kind == "kv": self._values[name] = KvNamespace(binding.get("scope"), binding.get("provider"))
            elif kind == "d1": self._values[name] = D1Database(name, binding.get("provider"))
            elif kind == "r2": self._values[name] = R2Bucket(binding.get("bucket"), binding.get("prefix", ""), binding.get("provider"))
            elif kind == "queue": self._values[name] = Queue(binding.get("queue"), binding.get("provider"))
            elif kind == "service": self._values[name] = ServiceBinding(binding.get("service"))
            elif kind == "durable_object_namespace": self._values[name] = DurableObjectNamespace(name, binding.get("className"))
            elif kind == "workflow": self._values[name] = WorkflowBinding(name)
            elif kind == "ai": self._values[name] = Ai()
            else: self._values[name] = types.SimpleNamespace(type=kind, metadata=binding)
    def __getattr__(self, name):
        try: return self._values[name]
        except KeyError as exc: raise AttributeError(name) from exc

workers = types.ModuleType("workers")
for name, value in {"WorkerEntrypoint": WorkerEntrypoint, "Request": Request, "Response": Response, "Headers": Headers, "KvNamespace": KvNamespace, "R2Bucket": R2Bucket, "Queue": Queue, "D1Database": D1Database, "DurableObjectNamespace": DurableObjectNamespace, "WebSocket": WebSocket, "NonRetryableError": RuntimeError}.items(): setattr(workers, name, value)
sys.modules["workers"] = workers
js = types.ModuleType("js")
for name, value in {"Request": Request, "Response": Response, "Headers": Headers, "WebSocket": WebSocket, "fetch": fetch, "caches": CacheStorage()}.items(): setattr(js, name, value)
sys.modules["js"] = js
pyodide_ffi = types.ModuleType("pyodide.ffi")
pyodide_ffi.to_js = lambda value, *args, **kwargs: value
pyodide_ffi.to_py = lambda value, *args, **kwargs: value
pyodide_package = types.ModuleType("pyodide")
pyodide_package.ffi = pyodide_ffi
sys.modules["pyodide"] = pyodide_package
sys.modules["pyodide.ffi"] = pyodide_ffi

__perenPyodideEnv = json.loads(__perenPyodideEnvJson)
entry_name = __perenPyodideEntry[:-3].replace("/", ".") if __perenPyodideEntry.endswith(".py") else __perenPyodideEntry.replace("/", ".")
if entry_name.endswith(".__init__"): entry_name = entry_name[:-9]
module = importlib.import_module(entry_name)

def _response(value):
    if not isinstance(value, Response): raise TypeError("Python fetch must return a Response")
    return {"status": value.status, "headers": value.headers.items(), "body": list(value.body), "upgrade": False, "websocketId": None}
async def maybe_await(value): return await value if hasattr(value, "__await__") else value
def _entrypoint(env, ctx): return module.Default(env, ctx) if hasattr(module, "Default") else None
def _handler(instance, name):
    if instance is not None and hasattr(instance, name): return getattr(instance, name), True
    selected = getattr(module, name, None)
    return (selected, False) if selected is not None else (None, False)
async def _call(selected, bound, *args, env, ctx):
    return await maybe_await(selected(*args) if bound else selected(*args, env, ctx))

async def _dispatch(kind, event):
    ctx, env = Context(), Env(__perenPyodideEnv)
    instance = _entrypoint(env, ctx)
    if kind == "fetch":
        selected, bound = _handler(instance, "fetch")
        if selected is None: raise TypeError("Python entrypoint must define Default.fetch or fetch")
        result = await _call(selected, bound, Request(event), env=env, ctx=ctx); await ctx.drain(); return _response(result)
    if kind == "scheduled":
        selected, bound = _handler(instance, "scheduled")
        if selected is not None: await _call(selected, bound, Scheduled(event), env=env, ctx=ctx)
        await ctx.drain(); return None
    if kind == "queue":
        selected, bound = _handler(instance, "queue"); batch = QueueBatch(event)
        if selected is not None: await _call(selected, bound, batch, env=env, ctx=ctx)
        await ctx.drain(); return {"dispositions": batch.dispositions()}
    if kind == "alarm":
        selected, bound = _handler(instance, "alarm")
        if selected is not None: await maybe_await(selected() if bound else selected(env, ctx))
        await ctx.drain(); return None
    if kind == "tail":
        selected, bound = _handler(instance, "tail")
        if selected is not None: await _call(selected, bound, event, env=env, ctx=ctx)
        await ctx.drain(); return None
    if kind == "workflow":
        selected, bound = _handler(instance, "workflow")
        if selected is not None: await _call(selected, bound, WorkflowEvent(event), env=env, ctx=ctx)
        await ctx.drain(); return None
    if kind == "activity":
        selected, bound = _handler(instance, "activity")
        if selected is None: raise TypeError("Python entrypoint must define Default.activity")
        result = await _call(selected, bound, WorkflowActivityEvent(event), env=env, ctx=ctx); await ctx.drain(); return result
    if kind == "websocketMessage":
        selected, bound = _handler(instance, "webSocketMessage"); socket = WebSocket(event["id"])
        if selected is not None: await _call(selected, bound, socket, event.get("message", ""), env=env, ctx=ctx)
        await ctx.drain(); return {"outbound": socket.drain()}
    if kind == "websocketClose":
        selected, bound = _handler(instance, "webSocketClose"); socket = WebSocket(event["id"])
        if selected is not None: await _call(selected, bound, socket, WebSocketClose(event), env=env, ctx=ctx)
        socket.close(event.get("code", 1000), event.get("reason", "")); await ctx.drain(); return {"outbound": socket.drain()}
    raise TypeError(f"unsupported Pyodide event: {kind}")

async def _dispatch_json(kind, event_json): return json.dumps(await _dispatch(kind, json.loads(event_json)))
def __peren_dispatch(kind, event_json): return asyncio.ensure_future(_dispatch_json(kind, event_json))
def __peren_dispatch_http(request_json): return __peren_dispatch("fetch", request_json)
"#;

#[derive(Clone, Debug)]
struct PyodideFileRoot {
    root: PathBuf,
}

deno_core::extension!(
    pyodide_files,
    ops = [op_pyodide_read_text, op_pyodide_read_binary],
    options = { root: PyodideFileRoot },
    state = |state, config| {
        state.put(config.root);
    },
);

#[op2]
#[string]
#[allow(
    clippy::needless_pass_by_value,
    reason = "deno_core op2 owns OpState handles and string arguments at the JS boundary"
)]
fn op_pyodide_read_text(
    state: Rc<RefCell<OpState>>,
    #[string] path: String,
) -> Result<String, deno_error::JsErrorBox> {
    let path = resolve_artifact_path(&state, &path)?;
    std::fs::read_to_string(path)
        .map_err(|error| deno_error::JsErrorBox::generic(error.to_string()))
}

#[op2]
#[serde]
#[allow(
    clippy::needless_pass_by_value,
    reason = "deno_core op2 owns OpState handles and string arguments at the JS boundary"
)]
fn op_pyodide_read_binary(
    state: Rc<RefCell<OpState>>,
    #[string] path: String,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let path = resolve_artifact_path(&state, &path)?;
    std::fs::read(path).map_err(|error| deno_error::JsErrorBox::generic(error.to_string()))
}

fn resolve_artifact_path(
    state: &Rc<RefCell<OpState>>,
    path: &str,
) -> Result<PathBuf, deno_error::JsErrorBox> {
    let root = state.borrow().borrow::<PyodideFileRoot>().root.clone();
    let raw = path.strip_prefix("file://").unwrap_or(path);
    let path = PathBuf::from(raw);
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    let root = root
        .canonicalize()
        .map_err(|error| deno_error::JsErrorBox::generic(error.to_string()))?;
    let path = path
        .canonicalize()
        .map_err(|error| deno_error::JsErrorBox::generic(error.to_string()))?;
    if !path.starts_with(&root) {
        return Err(deno_error::JsErrorBox::generic(
            "Pyodide artifact read escaped PEREN_PYODIDE_ROOT",
        ));
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, sync::Mutex};

    use async_trait::async_trait;

    use crate::{
        HttpRequest, HttpResponse,
        host::{
            AiHost, CacheHost, DurableObjectHost, HostError, OutboundFetchHost, QueueProducerHost,
            ServiceBindingHost,
        },
        wire::{
            AiRun, CacheEntry, CacheGet, CachePut, DurableObjectFetch, ListEntry, ListOptions,
            ListPage, QueueSend, R2Delete, R2Get, R2List, R2ListPage, R2Object, R2ObjectEntry,
            R2Put, ServiceFetch, SqlQuery, SqlResult, SqlValue,
        },
    };

    #[test]
    fn pyodide_artifacts_validate_required_files() {
        let root = tempfile::tempdir().unwrap();
        let artifacts = PyodideArtifacts {
            root: root.path().to_path_buf(),
        };

        let error = artifacts.validate().unwrap_err();
        assert!(error.to_string().contains("pyodide.mjs"), "{error}");

        for artifact in [
            "pyodide.mjs",
            "pyodide.asm.js",
            "pyodide.asm.wasm",
            "python_stdlib.zip",
            "pyodide-lock.json",
        ] {
            std::fs::write(root.path().join(artifact), "").unwrap();
        }
        artifacts.validate().unwrap();
    }

    #[test]
    fn python_package_lock_accepts_only_sorted_pyodide_package_names() {
        let lock =
            PythonPackageLock::parse(br#"{"version":1,"packages":["micropip","packaging"]}"#)
                .unwrap();
        assert_eq!(lock.packages, vec!["micropip", "packaging"]);

        let error =
            PythonPackageLock::parse(br#"{"version":1,"packages":["https://example/pkg.whl"]}"#)
                .unwrap_err();
        assert!(
            error.to_string().contains("must be a Pyodide package name"),
            "{error}"
        );

        let error =
            PythonPackageLock::parse(br#"{"version":1,"packages":["packaging","micropip"]}"#)
                .unwrap_err();
        assert!(error.to_string().contains("sorted and unique"), "{error}");
    }

    #[tokio::test]
    #[ignore = "requires PEREN_PYODIDE_ROOT with a real Pyodide distribution"]
    async fn pyodide_executor_dispatches_http_with_real_artifacts() {
        if PyodideArtifacts::from_env().is_none() {
            eprintln!("set PEREN_PYODIDE_ROOT to run this qualification test");
            return;
        }
        let entry = crate::ModuleName::parse("worker.py").unwrap();
        let bundle = WorkerBundle::new(
            entry.clone(),
            std::collections::BTreeMap::from([(
                entry,
                crate::Module::new(
                    ModuleKind::Python,
                    br#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        name = await request.text()
        print("pyodide handled " + name)
        return Response("pyodide:" + self.env.GREETING + ":" + name, status=202, headers={"x-runtime": "pyodide"})
"#
                    .as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap();
        let mut runtime = PyodideRuntime::load(
            bundle,
            IsolateLimits::new(256 * 1024 * 1024, std::time::Duration::from_secs(30)),
            WorkerEnvironment::new(std::collections::BTreeMap::from([(
                "GREETING".to_string(),
                "hello".to_string(),
            )])),
        )
        .await
        .unwrap();

        let response = runtime
            .dispatch_http(
                crate::HttpRequest {
                    method: "POST".into(),
                    url: "https://example.test/".into(),
                    headers: Vec::new(),
                    body: b"peren".to_vec(),
                    mtls: None,
                },
                crate::InvocationLimits::new(4096, 10),
            )
            .await
            .unwrap();

        assert_eq!(response.status, 202);
        assert_eq!(
            response.headers,
            vec![("x-runtime".into(), "pyodide".into())]
        );
        assert_eq!(response.body, b"pyodide:hello:peren");
        let logs = runtime.take_console_events();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].message, "pyodide handled peren");
    }

    #[tokio::test]
    #[ignore = "requires PEREN_PYODIDE_ROOT with a real Pyodide distribution"]
    #[allow(
        clippy::too_many_lines,
        reason = "qualification test exercises every non-HTTP lifecycle through one real Pyodide runtime"
    )]
    async fn pyodide_executor_dispatches_lifecycle_events_with_real_artifacts() {
        if PyodideArtifacts::from_env().is_none() {
            eprintln!("set PEREN_PYODIDE_ROOT to run this qualification test");
            return;
        }
        let entry = crate::ModuleName::parse("worker.py").unwrap();
        let bundle = WorkerBundle::new(
            entry.clone(),
            std::collections::BTreeMap::from([(
                entry,
                crate::Module::new(
                    ModuleKind::Python,
                    br#"
from workers import WorkerEntrypoint

class Default(WorkerEntrypoint):
    async def scheduled(self, event):
        print("scheduled:" + event.cron)

    async def queue(self, batch):
        print("queue:" + batch.queue)
        batch.messages[0].ack()
        batch.messages[1].retry({"delaySeconds": 3})

    async def alarm(self):
        print("alarm")

    async def tail(self, events):
        print("tail:" + str(len(events["events"])))

    async def activity(self, event):
        return {"name": event.name, "payload": event.payload}

    async def webSocketMessage(self, socket, message):
        socket.send("echo:" + message)

    async def webSocketClose(self, socket, event):
        socket.send("closed:" + event.reason)
"#
                    .as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap();
        let mut runtime = PyodideRuntime::load(
            bundle,
            IsolateLimits::new(256 * 1024 * 1024, std::time::Duration::from_secs(30)),
            WorkerEnvironment::empty(),
        )
        .await
        .unwrap();

        runtime
            .dispatch_scheduled(ScheduledEvent {
                scheduled_time_ms: 123,
                cron: "* * * * *".into(),
            })
            .await
            .unwrap();
        runtime.dispatch_alarm().await.unwrap();
        runtime
            .dispatch_tail(TailEvent {
                events: vec![crate::TailRecord {
                    outcome: "ok".into(),
                    script: "api".into(),
                    wall_time_ms: 4,
                }],
            })
            .await
            .unwrap();

        let queue = runtime
            .dispatch_queue(QueueEvent {
                queue: "jobs".into(),
                messages: vec![
                    crate::QueueMessage {
                        id: "a".into(),
                        body: b"one".to_vec(),
                        attempts: 1,
                        timestamp: 1,
                    },
                    crate::QueueMessage {
                        id: "b".into(),
                        body: b"two".to_vec(),
                        attempts: 1,
                        timestamp: 2,
                    },
                ],
                metrics: crate::QueueMetrics::default(),
            })
            .await
            .unwrap();
        assert_eq!(queue.dispositions.len(), 2);

        let activity = runtime
            .dispatch_workflow_activity(WorkflowActivityEvent {
                instance: "wf".into(),
                name: "step".into(),
                task: "task".into(),
                payload: serde_json::json!({"ok": true}),
            })
            .await
            .unwrap();
        assert_eq!(activity["name"], "step");

        let ws = runtime
            .dispatch_websocket_message(WebSocketMessageEvent {
                id: "socket".into(),
                message: "hi".into(),
            })
            .await
            .unwrap();
        assert_eq!(ws.outbound, vec!["echo:hi"]);
        let ws = runtime
            .dispatch_websocket_close(crate::WebSocketCloseEvent {
                id: "socket".into(),
                code: 1000,
                reason: "bye".into(),
                was_clean: true,
            })
            .await
            .unwrap();
        assert_eq!(ws.outbound, vec!["closed:bye"]);

        let logs = runtime.take_console_events();
        assert!(
            logs.iter()
                .any(|event| event.message == "scheduled:* * * * *")
        );
        assert!(logs.iter().any(|event| event.message == "queue:jobs"));
        assert!(logs.iter().any(|event| event.message == "alarm"));
        assert!(logs.iter().any(|event| event.message == "tail:1"));
    }

    #[tokio::test]
    #[ignore = "requires PEREN_PYODIDE_ROOT with a real Pyodide distribution"]
    #[allow(
        clippy::too_many_lines,
        reason = "qualification test exercises the Pyodide host-call binding bridge end to end"
    )]
    async fn pyodide_executor_dispatches_bindings_with_real_artifacts() {
        if PyodideArtifacts::from_env().is_none() {
            eprintln!("set PEREN_PYODIDE_ROOT to run this qualification test");
            return;
        }
        let entry = crate::ModuleName::parse("worker.py").unwrap();
        let bundle = WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                crate::Module::new(
                    ModuleKind::Python,
                    br#"
from workers import WorkerEntrypoint, Response
from js import caches, fetch

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        await self.env.CACHE.put("alpha", "one")
        kv = await self.env.CACHE.get("alpha")

        await self.env.BUCKET.put("hello.txt", "r2")
        obj = await self.env.BUCKET.get("hello.txt")
        r2 = await obj.text()

        await self.env.JOBS.send("work", {"contentType": "text/plain", "delaySeconds": 2})

        rows = await self.env.DB.prepare("select message").all()
        d1 = rows["results"][0]["message"]

        service = await self.env.AUTH.fetch("https://auth.internal/session", {"method": "POST", "body": "svc"})
        service_text = await service.text()

        stub = self.env.OBJECTS.get(self.env.OBJECTS.idFromName("alpha"))
        durable = await stub.fetch("https://object.internal/state", {"method": "POST", "body": "do"})
        durable_text = await durable.text()

        outbound = await fetch("https://upstream.invalid/data", {"method": "POST", "body": "out"})
        outbound_text = await outbound.text()

        cache = await caches.open("pages")
        await cache.put("https://cache.invalid/item", Response("cached"))
        cached = await cache.match("https://cache.invalid/item")

        ai = await self.env.AI.run("@model/test", {"prompt": "hello"})

        workflow = await self.env.WORK.create({"id": "wf-1"})
        status = await workflow.status()
        await workflow.terminate("done")

        body = "|".join([
            kv,
            r2,
            d1,
            service_text,
            durable_text,
            outbound_text,
            await cached.text(),
            ai["model"],
            status["status"],
        ])
        return Response(body, status=207)
"#
                    .as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap();
        let storage = Arc::new(MemoryStorage::default());
        let queue = Arc::new(RecordingQueue::default());
        let r2 = Arc::new(MemoryR2::default());
        let fetch = Arc::new(RecordingFetch::default());
        let service = Arc::new(RecordingService::default());
        let durable = Arc::new(RecordingDurable::default());
        let cache = Arc::new(MemoryCache::default());
        let ai = Arc::new(RecordingAi::default());
        let mut runtime = PyodideRuntime::load_with_capabilities(
            bundle,
            IsolateLimits::new(256 * 1024 * 1024, std::time::Duration::from_secs(30)),
            WorkerEnvironment::new(BTreeMap::from([(
                "__perenBindings".to_string(),
                r#"{
                    "CACHE":{"type":"kv","scope":"cache","provider":{"kind":"native"}},
                    "BUCKET":{"type":"r2","bucket":"files","prefix":"app/","provider":{"kind":"memory"}},
                    "JOBS":{"type":"queue","queue":"jobs","provider":{"kind":"memory"}},
                    "DB":{"type":"d1","provider":{"kind":"native_sqlite"}},
                    "AUTH":{"type":"service","service":"auth"},
                    "OBJECTS":{"type":"durable_object_namespace","className":"Counter"},
                    "AI":{"type":"ai"},
                    "WORK":{"type":"workflow"}
                }"#
                .to_string(),
            )])),
            Capabilities {
                storage,
                fetch: Some(fetch.clone()),
                queue: Some(queue.clone()),
                r2: Some(r2),
                service: Some(service.clone()),
                durable: Some(durable.clone()),
                cache: Some(cache),
                kv: None,
                ai: Some(ai.clone()),
            },
        )
        .await
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "https://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                crate::InvocationLimits::new(16 * 1024, 50),
            )
            .await
            .unwrap();

        assert_eq!(response.status, 207);
        assert_eq!(
            String::from_utf8(response.body).unwrap(),
            "one|r2|d1-value|service:auth:svc|durable:Counter:name:alpha:do|fetch:out|cached|@model/test|running"
        );
        let messages = queue.messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].queue, "jobs");
        assert_eq!(messages[0].body, b"work");
        assert_eq!(messages[0].delay_seconds, Some(2));
        assert_eq!(service.calls.lock().unwrap()[0].service, "auth");
        assert_eq!(durable.calls.lock().unwrap()[0].class_name, "Counter");
        assert_eq!(
            fetch.calls.lock().unwrap()[0].url,
            "https://upstream.invalid/data"
        );
        assert_eq!(ai.calls.lock().unwrap()[0].model, "@model/test");
    }

    #[tokio::test]
    #[ignore = "requires PEREN_PYODIDE_ROOT with a real Pyodide distribution"]
    async fn pyodide_executor_enforces_stdlib_and_filesystem_policy_with_real_artifacts() {
        if PyodideArtifacts::from_env().is_none() {
            eprintln!("set PEREN_PYODIDE_ROOT to run this qualification test");
            return;
        }
        let entry = crate::ModuleName::parse("worker.py").unwrap();
        let source = br#"
import json, math, pathlib
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        blocked = False
        try:
            import socket
        except ModuleNotFoundError:
            blocked = True
        path = pathlib.Path("/tmp-peren.txt")
        existed = path.exists()
        path.write_text(json.dumps({"sqrt": math.sqrt(81)}))
        return Response.json({"blocked": blocked, "existed": existed, "body": path.read_text()})
"#;
        let bundle = WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                crate::Module::new(ModuleKind::Python, source.as_slice()).unwrap(),
            )]),
        )
        .unwrap();
        let mut first_runtime = PyodideRuntime::load(
            bundle.clone(),
            IsolateLimits::new(256 * 1024 * 1024, std::time::Duration::from_secs(30)),
            WorkerEnvironment::empty(),
        )
        .await
        .unwrap();
        let first = first_runtime
            .dispatch_http(empty_request(), crate::InvocationLimits::new(4096, 10))
            .await
            .unwrap();
        drop(first_runtime);

        let mut second_runtime = PyodideRuntime::load(
            bundle,
            IsolateLimits::new(256 * 1024 * 1024, std::time::Duration::from_secs(30)),
            WorkerEnvironment::empty(),
        )
        .await
        .unwrap();
        let second = second_runtime
            .dispatch_http(empty_request(), crate::InvocationLimits::new(4096, 10))
            .await
            .unwrap();

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&first.body).unwrap(),
            serde_json::json!({"blocked": true, "existed": false, "body": "{\"sqrt\": 9.0}"})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&second.body).unwrap()["existed"],
            false
        );
    }

    #[tokio::test]
    #[ignore = "requires PEREN_PYODIDE_ROOT with a real Pyodide distribution"]
    async fn pyodide_executor_reports_missing_handlers_with_real_artifacts() {
        if PyodideArtifacts::from_env().is_none() {
            eprintln!("set PEREN_PYODIDE_ROOT to run this qualification test");
            return;
        }
        let entry = crate::ModuleName::parse("worker.py").unwrap();
        let bundle = WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                crate::Module::new(
                    ModuleKind::Python,
                    br"
from workers import WorkerEntrypoint

class Default(WorkerEntrypoint):
    pass
"
                    .as_slice(),
                )
                .unwrap(),
            )]),
        )
        .unwrap();
        let mut runtime = PyodideRuntime::load(
            bundle,
            IsolateLimits::new(256 * 1024 * 1024, std::time::Duration::from_secs(30)),
            WorkerEnvironment::empty(),
        )
        .await
        .unwrap();

        runtime
            .dispatch_scheduled(ScheduledEvent {
                scheduled_time_ms: 1,
                cron: "* * * * *".into(),
            })
            .await
            .unwrap();
        let error = runtime
            .dispatch_http(empty_request(), crate::InvocationLimits::new(4096, 10))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Python entrypoint must define Default.fetch or fetch"),
            "{error}"
        );
    }

    fn empty_request() -> HttpRequest {
        HttpRequest {
            method: "GET".into(),
            url: "https://worker.invalid/".into(),
            headers: Vec::new(),
            body: Vec::new(),
            mtls: None,
        }
    }

    #[derive(Default)]
    struct RecordingQueue {
        messages: Mutex<Vec<QueueSend>>,
    }

    #[async_trait]
    impl QueueProducerHost for RecordingQueue {
        async fn send(&self, message: QueueSend) -> Result<(), HostError> {
            self.messages.lock().unwrap().push(message);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingFetch {
        calls: Mutex<Vec<HttpRequest>>,
    }

    #[async_trait]
    impl OutboundFetchHost for RecordingFetch {
        async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(HttpResponse {
                status: 208,
                headers: Vec::new(),
                body: format!("fetch:{}", String::from_utf8_lossy(&request.body)).into_bytes(),
                upgrade: false,
                websocket_id: None,
            })
        }
    }

    #[derive(Default)]
    struct RecordingService {
        calls: Mutex<Vec<ServiceFetch>>,
    }

    #[async_trait]
    impl ServiceBindingHost for RecordingService {
        async fn fetch(&self, request: ServiceFetch) -> Result<HttpResponse, HostError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(HttpResponse {
                status: 209,
                headers: Vec::new(),
                body: format!(
                    "service:{}:{}",
                    request.service,
                    String::from_utf8_lossy(&request.request.body)
                )
                .into_bytes(),
                upgrade: false,
                websocket_id: None,
            })
        }
    }

    #[derive(Default)]
    struct RecordingDurable {
        calls: Mutex<Vec<DurableObjectFetch>>,
    }

    #[async_trait]
    impl DurableObjectHost for RecordingDurable {
        async fn fetch(&self, request: DurableObjectFetch) -> Result<HttpResponse, HostError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(HttpResponse {
                status: 210,
                headers: Vec::new(),
                body: format!(
                    "durable:{}:{}:{}",
                    request.class_name,
                    request.id,
                    String::from_utf8_lossy(&request.request.body)
                )
                .into_bytes(),
                upgrade: false,
                websocket_id: None,
            })
        }
    }

    #[derive(Default)]
    struct RecordingAi {
        calls: Mutex<Vec<AiRun>>,
    }

    #[async_trait]
    impl AiHost for RecordingAi {
        async fn run(&self, request: AiRun) -> Result<serde_json::Value, HostError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(serde_json::json!({
                "model": request.model,
                "input": request.input,
            }))
        }
    }

    #[derive(Default)]
    struct MemoryCache {
        entries: Mutex<BTreeMap<(String, String), CacheEntry>>,
    }

    #[async_trait]
    impl CacheHost for MemoryCache {
        async fn match_entry(&self, request: CacheGet) -> Result<Option<CacheEntry>, HostError> {
            Ok(self
                .entries
                .lock()
                .unwrap()
                .get(&(request.cache, request.key))
                .cloned())
        }

        async fn put_entry(&self, entry: CachePut) -> Result<(), HostError> {
            self.entries.lock().unwrap().insert(
                (entry.cache, entry.key),
                CacheEntry {
                    status: entry.status,
                    headers: entry.headers,
                    body: entry.body,
                },
            );
            Ok(())
        }

        async fn delete_entry(&self, request: CacheGet) -> Result<bool, HostError> {
            Ok(self
                .entries
                .lock()
                .unwrap()
                .remove(&(request.cache, request.key))
                .is_some())
        }
    }

    #[derive(Default)]
    struct MemoryR2 {
        objects: Mutex<BTreeMap<(String, String), R2Object>>,
    }

    #[async_trait]
    impl R2BucketHost for MemoryR2 {
        async fn put(&self, object: R2Put) -> Result<(), HostError> {
            let size = object.body.len();
            self.objects.lock().unwrap().insert(
                (object.bucket.clone(), object.key.clone()),
                R2Object {
                    key: object.key,
                    body: object.body,
                    size,
                    content_type: object.content_type,
                    custom_metadata: object.custom_metadata,
                },
            );
            Ok(())
        }

        async fn get(&self, object: R2Get) -> Result<Option<R2Object>, HostError> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(&(object.bucket, object.key))
                .cloned())
        }

        async fn delete(&self, object: R2Delete) -> Result<(), HostError> {
            self.objects
                .lock()
                .unwrap()
                .remove(&(object.bucket, object.key));
            Ok(())
        }

        async fn list(&self, request: R2List) -> Result<R2ListPage, HostError> {
            let prefix = request.prefix.unwrap_or_default();
            let objects = self
                .objects
                .lock()
                .unwrap()
                .iter()
                .filter(|((bucket, key), _)| bucket == &request.bucket && key.starts_with(&prefix))
                .map(|(_, object)| R2ObjectEntry {
                    key: object.key.clone(),
                    size: object.size,
                    custom_metadata: object.custom_metadata.clone(),
                })
                .collect();
            Ok(R2ListPage {
                objects,
                cursor: None,
                list_complete: true,
            })
        }
    }

    type StorageMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

    #[derive(Default)]
    struct MemoryStorage {
        values: Mutex<StorageMap>,
        revision: Mutex<u64>,
    }

    #[async_trait]
    impl DurableStorageHost for MemoryStorage {
        async fn begin(&self) -> Result<(), HostError> {
            Ok(())
        }

        async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .get(&(scope.to_string(), key.to_vec()))
                .cloned())
        }

        async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError> {
            self.values
                .lock()
                .unwrap()
                .insert((scope.to_string(), key.to_vec()), value.to_vec());
            Ok(())
        }

        async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .remove(&(scope.to_string(), key.to_vec()))
                .is_some())
        }

        async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError> {
            let prefix = options.prefix.unwrap_or_default();
            let keys = self
                .values
                .lock()
                .unwrap()
                .keys()
                .filter(|(candidate_scope, key)| {
                    candidate_scope == scope && key.starts_with(&prefix)
                })
                .map(|(_, key)| ListEntry {
                    name: String::from_utf8_lossy(key).into_owned(),
                })
                .collect();
            Ok(ListPage {
                keys,
                cursor: None,
                list_complete: true,
            })
        }

        async fn sql(&self, _query: SqlQuery) -> Result<SqlResult, HostError> {
            Ok(SqlResult {
                columns: vec!["message".into()],
                rows: vec![vec![SqlValue::Text("d1-value".into())]],
                changes: 0,
                last_insert_rowid: 0,
            })
        }

        async fn commit(&self) -> Result<peren_primitives::StorageRevision, HostError> {
            let mut revision = self.revision.lock().unwrap();
            *revision += 1;
            Ok(peren_primitives::StorageRevision::new(*revision))
        }

        async fn rollback(&self) -> Result<(), HostError> {
            Ok(())
        }
    }
}
