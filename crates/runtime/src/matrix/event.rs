use super::{CapabilityStatus, RuntimeCapability, event};

pub(super) const ALARM: RuntimeCapability = event(
    "alarm",
    CapabilityStatus::Supported,
    "dispatch_alarm isolate/cell/node tests",
);

pub(super) const FETCH: RuntimeCapability = event(
    "fetch",
    CapabilityStatus::Supported,
    "dispatch_http isolate/cell/node tests",
);

pub(super) const QUEUE: RuntimeCapability = event(
    "queue",
    CapabilityStatus::Supported,
    "worker producer binding, local broker lease/retry/DLQ, config consumer planning, node delivery, and process-level consumer drain are tested",
);

pub(super) const RPC: RuntimeCapability = event(
    "rpc",
    CapabilityStatus::Supported,
    "service binding and Durable Object stub method calls are routed over the internal fetch boundary and process-tested through target workers and object cells; DurableObjectState facets support scoped storage, fetch, RPC-style methods, abort, and delete in isolate tests; Loader bindings resolve bundled relative modules against the Worker entry module and reject unsupported remote/package specifiers",
);

pub(super) const SCHEDULED: RuntimeCapability = event(
    "scheduled",
    CapabilityStatus::Supported,
    "dispatch_scheduled isolate/cell/node tests",
);

pub(super) const TAIL: RuntimeCapability = event(
    "tail",
    CapabilityStatus::Supported,
    "first tail dispatch isolate/cell/node tests",
);

pub(super) const WAITUNTIL: RuntimeCapability = event(
    "waitUntil",
    CapabilityStatus::Supported,
    "fetch and queue ctx.waitUntil background drains are isolate-tested; scheduled and tail use the same dispatch drain path",
);

pub(super) const WEBSOCKETCLOSE: RuntimeCapability = event(
    "webSocketClose",
    CapabilityStatus::Partial,
    "WebSocketPair close events, runtime host re-entry, node upgraded-socket close bridging, DurableObjectState hibernation APIs, persisted accepted-socket metadata, persisted auto-response metadata, auto-response dispatch short-circuiting, dispatch hydration, and close cleanup are runtime-tested for the local Peren host model; full Cloudflare edge WebSocket hibernation behavior outside that local host model remains unsupported",
);

pub(super) const WEBSOCKETMESSAGE: RuntimeCapability = event(
    "webSocketMessage",
    CapabilityStatus::Partial,
    "WebSocketPair message events, runtime host re-entry, node upgraded-socket frame bridging, selected subprotocol preservation, session cleanup, DurableObjectState hibernation APIs, persisted accepted-socket metadata, persisted auto-response metadata, auto-response dispatch short-circuiting, and dispatch hydration are runtime-tested for the local Peren host model; full Cloudflare edge WebSocket hibernation behavior outside that local host model remains unsupported",
);

pub(super) const WORKFLOW: RuntimeCapability = event(
    "workflow",
    CapabilityStatus::Supported,
    "workflow event dispatch, step.do replay, durable step.sleep replay and activity dispatch are runtime-tested through runtime and node paths",
);
