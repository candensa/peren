use super::{
    EngineError, Watchdog, WorkerRuntime,
    script::{
        DISPATCH_ACTIVITY, DISPATCH_ALARM, DISPATCH_HTTP, DISPATCH_QUEUE, DISPATCH_SCHEDULED,
        DISPATCH_TAIL, DISPATCH_WEBSOCKET_CLOSE, DISPATCH_WEBSOCKET_MESSAGE, DISPATCH_WORKFLOW,
    },
};
use crate::{
    HttpRequest, HttpResponse, InvocationLimits,
    wire::{
        QueueDispatch, QueueDisposition, QueueEvent, ScheduledEvent, TailEvent,
        WebSocketCloseEvent, WebSocketDispatch, WebSocketMessageEvent, WorkflowActivityEvent,
        WorkflowEvent,
    },
};
use deno_core::{FastString, v8};
use serde::Serialize;
use std::time::Instant;

impl WorkerRuntime {
    pub async fn dispatch(
        &mut self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, EngineError> {
        self.reset_storage();
        self.reset_console();
        let request = serde_json::to_string(&request)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            let source = format!("Promise.resolve(globalThis.__perenEntry.fetch({request}))");
            let promise = self
                .runtime
                .execute_script("peren:dispatch", FastString::from(source))
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
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

    pub async fn dispatch_http(
        &mut self,
        request: HttpRequest,
        limits: InvocationLimits,
    ) -> Result<HttpResponse, EngineError> {
        let body_bytes = u64::try_from(request.body.len()).unwrap_or(u64::MAX);
        limits.validate_request(body_bytes)?;
        self.reset_storage();
        self.reset_console();
        let started = Instant::now();
        let request = serde_json::to_string(&request)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        self.runtime
            .execute_script(
                "peren:request",
                FastString::from(format!("globalThis.__perenRequest = {request}")),
            )
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            let promise = self
                .runtime
                .execute_script("peren:http", FastString::from_static(DISPATCH_HTTP))
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
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
        self.reset_storage();
        self.reset_console();
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            let promise = self
                .runtime
                .execute_script("peren:alarm", FastString::from_static(DISPATCH_ALARM))
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
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
        result?;
        Ok(())
    }

    pub async fn dispatch_scheduled(&mut self, event: ScheduledEvent) -> Result<(), EngineError> {
        self.reset_storage();
        self.reset_console();
        let event = serde_json::to_string(&event)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        self.runtime
            .execute_script(
                "peren:scheduled:event",
                FastString::from(format!("globalThis.__perenScheduled = {event}")),
            )
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            let promise = self
                .runtime
                .execute_script(
                    "peren:scheduled",
                    FastString::from_static(DISPATCH_SCHEDULED),
                )
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
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
        result?;
        Ok(())
    }

    pub async fn dispatch_queue(
        &mut self,
        event: QueueEvent,
    ) -> Result<QueueDispatch, EngineError> {
        self.dispatch_event("queue", "__perenQueue", &event, DISPATCH_QUEUE)
            .await?;
        let value = self
            .runtime
            .execute_script(
                "peren:queue:dispositions",
                FastString::from_static("globalThis.__perenQueueDispositions ?? []"),
            )
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        deno_core::scope!(scope, self.runtime);
        let local = v8::Local::new(scope, value);
        let dispositions = deno_core::serde_v8::from_v8::<Vec<QueueDisposition>>(scope, local)
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        Ok(QueueDispatch { dispositions })
    }

    pub async fn dispatch_tail(&mut self, event: TailEvent) -> Result<(), EngineError> {
        self.dispatch_event("tail", "__perenTail", &event, DISPATCH_TAIL)
            .await
    }

    pub async fn dispatch_websocket_message(
        &mut self,
        event: WebSocketMessageEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        self.dispatch_event(
            "websocket:message",
            "__perenWebSocketMessage",
            &event,
            DISPATCH_WEBSOCKET_MESSAGE,
        )
        .await?;
        self.websocket_dispatch()
    }

    pub async fn dispatch_websocket_close(
        &mut self,
        event: WebSocketCloseEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        self.dispatch_event(
            "websocket:close",
            "__perenWebSocketClose",
            &event,
            DISPATCH_WEBSOCKET_CLOSE,
        )
        .await?;
        self.websocket_dispatch()
    }

    fn websocket_dispatch(&mut self) -> Result<WebSocketDispatch, EngineError> {
        let value = self
            .runtime
            .execute_script(
                "peren:websocket:dispatch",
                FastString::from_static("globalThis.__perenWebSocketDispatch ?? { outbound: [] }"),
            )
            .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        deno_core::scope!(scope, self.runtime);
        let local = v8::Local::new(scope, value);
        deno_core::serde_v8::from_v8::<WebSocketDispatch>(scope, local)
            .map_err(|error| EngineError::JavaScript(error.to_string()))
    }

    pub async fn dispatch_workflow_activity(
        &mut self,
        event: WorkflowActivityEvent,
    ) -> Result<serde_json::Value, EngineError> {
        self.reset_storage();
        self.reset_console();
        let event = serde_json::to_string(&event)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        self.runtime
            .execute_script(
                "peren:activity:event",
                FastString::from(format!("globalThis.__perenActivity = {event}")),
            )
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            let promise = self
                .runtime
                .execute_script("peren:activity", FastString::from_static(DISPATCH_ACTIVITY))
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
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

    pub async fn dispatch_workflow(&mut self, event: WorkflowEvent) -> Result<(), EngineError> {
        self.dispatch_event("workflow", "__perenWorkflow", &event, DISPATCH_WORKFLOW)
            .await
    }

    async fn dispatch_event<T: Serialize>(
        &mut self,
        name: &str,
        global: &str,
        event: &T,
        source: &'static str,
    ) -> Result<(), EngineError> {
        self.reset_storage();
        self.reset_console();
        let event = serde_json::to_string(event)
            .map_err(|error| EngineError::Request(error.to_string()))?;
        self.runtime
            .execute_script(
                FastString::from(format!("peren:{name}:event")),
                FastString::from(format!("globalThis.{global} = {event}")),
            )
            .map_err(|error| EngineError::Request(error.to_string()))?;
        let watchdog = Watchdog::start(&mut self.runtime, self.limits.execution_time());
        let result = async {
            let promise = self
                .runtime
                .execute_script(
                    FastString::from(format!("peren:{name}")),
                    FastString::from_static(source),
                )
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
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
        if name.starts_with("websocket:") {
            let dispatch = {
                deno_core::scope!(scope, self.runtime);
                let value = v8::Local::new(scope, value);
                deno_core::serde_v8::from_v8::<WebSocketDispatch>(scope, value)
                    .map_err(|error| EngineError::JavaScript(error.to_string()))?
            };
            self.runtime
                .execute_script(
                    "peren:websocket:dispatch:set",
                    FastString::from(format!(
                        "globalThis.__perenWebSocketDispatch = {}",
                        serde_json::to_string(&dispatch)
                            .map_err(|error| EngineError::Response(error.to_string()))?
                    )),
                )
                .map_err(|error| EngineError::JavaScript(error.to_string()))?;
        }
        Ok(())
    }
}
