pub(super) const SEAL_INTERNALS: &str = "delete globalThis.Deno; delete globalThis.__bootstrap;";
pub(super) const DISPATCH_HTTP: &str = r#"
(async () => {
  const input = globalThis.__perenRequest;
  const request = new Request(input.url, {
    method: input.method,
    headers: input.headers,
    body: input.body.length === 0 || input.method === "GET" || input.method === "HEAD"
      ? undefined
      : new Uint8Array(input.body),
  });
  globalThis.__perenResetWaitUntil();
  await globalThis.__perenHydrateDurableWebSockets?.();
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  const rpcMethod = request.headers.get("x-peren-rpc-method");
  const durableClass = globalThis.__perenDurableClass;
  const response = rpcMethod !== null
    ? await (async () => {
      const target = globalThis.__perenEntry?.[rpcMethod];
      if (typeof target !== "function") {
        return Response.json({ ok: false, error: { message: `RPC method ${rpcMethod} is not exported` } }, { status: 404 });
      }
      try {
        const payload = request.body === null ? {} : await request.json();
        const value = await Promise.resolve(target.apply(globalThis.__perenEntry, Array.isArray(payload?.args) ? payload.args : []));
        return Response.json({ ok: true, value }, { headers: { "content-type": "application/vnd.peren.rpc+json" } });
      } catch (error) {
        return Response.json({ ok: false, error: { message: error?.message ?? String(error) } }, { status: 500 });
      }
    })()
    : typeof durableClass === "function"
      ? await (async () => {
          const holder = { object: null };
          const durableContext = globalThis.__perenDurableObjectContext ?? {};
          const durableId = durableContext.id == null
            ? null
            : globalThis.__perenCreateDurableObjectId(
                durableContext.id.namespace,
                durableContext.id.value,
                durableContext.id.name ?? null,
              );
          const state = new DurableObjectState(globalThis.Peren.storage, globalThis.__perenEnv, {
            id: durableId,
            props: durableContext.props ?? {},
            exports: () => holder.object,
          });
          const object = new durableClass(state, globalThis.__perenEnv);
          holder.object = object;
          if (typeof object.fetch !== "function") {
            throw new TypeError("durable object class must provide fetch");
          }
          return await Promise.resolve(object.fetch(request));
        })()
      : typeof globalThis.__perenEntry?.fetch === "function"
      ? await Promise.resolve(globalThis.__perenEntry.fetch(request, globalThis.__perenEnv, ctx))
      : await (async () => {
        const event = globalThis.__perenCreateFetchEvent(request);
        const result = await globalThis.__perenDispatchListener("fetch", event);
        return await event.__perenResponse() ?? result;
      })();
  if (!(response instanceof Response)) {
    throw new TypeError("fetch must return a Response");
  }
  await globalThis.__perenDrainWaitUntil();
  const upgrade = response.webSocket instanceof WebSocket;
  return {
    status: response.status,
    headers: Array.from(response.headers.entries()),
    body: upgrade ? [] : Array.from(new Uint8Array(await response.arrayBuffer())),
    upgrade,
    websocketId: upgrade ? globalThis.__perenWebSocketAttachmentId(response.webSocket) : null,
  };
})()
"#;
pub(super) const DISPATCH_ALARM: &str = r#"
(() => {
  if (typeof globalThis.__perenEntry?.alarm !== "function") {
    return Promise.resolve();
  }
  return Promise.resolve(globalThis.__perenEntry.alarm(globalThis.__perenEnv, {}));
})()
"#;
pub(super) const DISPATCH_SCHEDULED: &str = r#"
(() => {
  globalThis.__perenResetWaitUntil();
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  if (typeof globalThis.__perenEntry?.scheduled === "function") {
    return Promise.resolve(globalThis.__perenEntry.scheduled(globalThis.__perenScheduled, globalThis.__perenEnv, ctx))
      .then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
  }
  return Promise.resolve(globalThis.__perenDispatchListener(
    "scheduled",
    globalThis.__perenCreateExtendableEvent("scheduled", globalThis.__perenScheduled),
  )).then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
})()
"#;
pub(super) const DISPATCH_QUEUE: &str = r#"
(() => {
  globalThis.__perenResetWaitUntil();
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  const event = globalThis.__perenPrepareQueue(globalThis.__perenQueue);
  if (typeof globalThis.__perenEntry?.queue === "function") {
    return Promise.resolve(globalThis.__perenEntry.queue(event, globalThis.__perenEnv, ctx))
      .then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
  }
  return Promise.resolve(globalThis.__perenDispatchListener(
    "queue",
    globalThis.__perenCreateExtendableEvent("queue", event),
  )).then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
})()
"#;
pub(super) const DISPATCH_TAIL: &str = r#"
(() => {
  globalThis.__perenResetWaitUntil();
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  if (typeof globalThis.__perenEntry?.tail === "function") {
    return Promise.resolve(globalThis.__perenEntry.tail(globalThis.__perenTail, globalThis.__perenEnv, ctx))
      .then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
  }
  return Promise.resolve(globalThis.__perenDispatchListener(
    "tail",
    globalThis.__perenCreateExtendableEvent("tail", globalThis.__perenTail),
  )).then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
})()
"#;
pub(super) const DISPATCH_WORKFLOW: &str = r#"
(() => {
  globalThis.__perenResetWaitUntil();
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  const event = globalThis.__perenWorkflow;
  const step = globalThis.__perenCreateWorkflowStep(event);
  if (typeof globalThis.__perenEntry?.workflow !== "function") {
    return Promise.resolve();
  }
  return Promise.resolve(globalThis.__perenEntry.workflow(event, step, globalThis.__perenEnv, ctx))
    .then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
})()
"#;
pub(super) const DISPATCH_ACTIVITY: &str = r#"
(() => {
  globalThis.__perenResetWaitUntil();
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  const event = globalThis.__perenActivity;
  if (typeof globalThis.__perenEntry?.activity !== "function") {
    throw new TypeError("default export must provide activity for workflow activity dispatch");
  }
  return Promise.resolve(globalThis.__perenEntry.activity(event, globalThis.__perenEnv, ctx))
    .then(async (value) => { await globalThis.__perenDrainWaitUntil(); return value; });
})()
"#;

pub(super) const DISPATCH_WEBSOCKET_MESSAGE: &str = r#"
(async () => {
  globalThis.__perenResetWaitUntil();
  await globalThis.__perenHydrateDurableWebSockets?.();
  const event = globalThis.__perenWebSocketMessage;
  const socket = globalThis.__perenHostWebSocket(event.id);
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  if (globalThis.__perenMaybeAutoRespondWebSocket?.(socket, event.message)) {
    await globalThis.__perenDrainWaitUntil();
    return { outbound: globalThis.__perenDrainWebSocketOutbound(socket) };
  }
  const run = typeof globalThis.__perenEntry?.webSocketMessage === "function"
    ? Promise.resolve(globalThis.__perenEntry.webSocketMessage(socket, event.message, globalThis.__perenEnv, ctx))
    : Promise.resolve(globalThis.__perenDispatchListener("webSocketMessage", globalThis.__perenCreateExtendableEvent("webSocketMessage", { webSocket: socket, message: event.message })));
  return run.then(async () => {
    await globalThis.__perenDrainWaitUntil();
    return { outbound: globalThis.__perenDrainWebSocketOutbound(socket) };
  });
})()
"#;

pub(super) const DISPATCH_WEBSOCKET_CLOSE: &str = r#"
(async () => {
  globalThis.__perenResetWaitUntil();
  await globalThis.__perenHydrateDurableWebSockets?.();
  const event = globalThis.__perenWebSocketClose;
  const socket = globalThis.__perenHostWebSocket(event.id);
  const ctx = { waitUntil: globalThis.__perenRegisterWaitUntil };
  const run = typeof globalThis.__perenEntry?.webSocketClose === "function"
    ? Promise.resolve(globalThis.__perenEntry.webSocketClose(socket, event.code, event.reason, event.wasClean, globalThis.__perenEnv, ctx))
    : Promise.resolve(globalThis.__perenDispatchListener("webSocketClose", globalThis.__perenCreateExtendableEvent("webSocketClose", { webSocket: socket, code: event.code, reason: event.reason, wasClean: event.wasClean })));
  return run.then(async () => {
    await globalThis.__perenDrainWaitUntil();
    const outbound = globalThis.__perenDrainWebSocketOutbound(socket);
    globalThis.__perenReleaseWebSocket?.(event.id, event.code, event.reason);
    await globalThis.__perenDeleteDurableWebSocket?.(event.id);
    return { outbound };
  });
})()
"#;
