#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CapabilityKind {
    Binding,
    Event,
    Global,
    Module,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CapabilityStatus {
    Supported,
    Partial,
    Unsupported,
    Planned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeCapability {
    pub kind: CapabilityKind,
    pub name: &'static str,
    pub status: CapabilityStatus,
    pub evidence: &'static str,
}

#[must_use]
pub fn release_blockers() -> Vec<RuntimeCapability> {
    CAPABILITIES
        .iter()
        .copied()
        .filter(|capability| capability.status == CapabilityStatus::Planned)
        .collect()
}

mod binding;
mod event;
mod global;
mod module;

pub const CAPABILITIES: &[RuntimeCapability] = &[
    global::ABORTCONTROLLER,
    global::ABORTSIGNAL,
    global::AI,
    global::ANALYTICSENGINEDATASET,
    global::BROADCASTCHANNEL,
    global::BYTELENGTHQUEUINGSTRATEGY,
    global::BLOB,
    global::COMPRESSIONSTREAM,
    global::COUNTQUEUINGSTRATEGY,
    global::CRYPTOKEY,
    global::D1DATABASE,
    global::D1PREPAREDSTATEMENT,
    global::DECOMPRESSIONSTREAM,
    global::DOMEXCEPTION,
    global::DISPATCHNAMESPACE,
    global::DURABLEOBJECTNAMESPACE,
    global::EVENT,
    global::EVENTSOURCE,
    global::EVENTTARGET,
    global::FILE,
    global::FILEREADER,
    global::FORMDATA,
    global::HEADERS,
    global::HYPERDRIVE,
    global::KVNAMESPACE,
    global::LOADER,
    global::MESSAGECHANNEL,
    global::MESSAGEEVENT,
    global::MESSAGEPORT,
    global::NONRETRYABLEERROR,
    global::QUEUE,
    global::R2BUCKET,
    global::PERFORMANCE,
    global::PROGRESSEVENT,
    global::RATELIMITER,
    global::READABLESTREAM,
    global::REQUEST,
    global::RESPONSE,
    global::TEXTDECODER,
    global::TEXTENCODER,
    global::TEXTDECODERSTREAM,
    global::TEXTENCODERSTREAM,
    global::TRANSFORMSTREAM,
    global::URL,
    global::URLPATTERN,
    global::URLSEARCHPARAMS,
    global::VECTORIZEINDEX,
    global::WEBSOCKET,
    global::WEBSOCKETPAIR,
    global::WEBSOCKETREQUESTRESPONSEPAIR,
    global::WORKFLOW,
    global::WRITABLESTREAM,
    global::ADDEVENTLISTENER,
    global::ATOB,
    global::BTOA,
    global::CACHE,
    global::CACHESTORAGE,
    global::CACHES,
    global::CLEARIMMEDIATE,
    global::CLEARINTERVAL,
    global::CLEARTIMEOUT,
    global::CONSOLE,
    global::CRYPTO,
    global::FETCH,
    global::NAVIGATOR,
    global::PERFORMANCE_2,
    global::SETIMMEDIATE,
    global::SETINTERVAL,
    global::SETTIMEOUT,
    global::SELF,
    global::STRUCTUREDCLONE,
    event::ALARM,
    event::FETCH,
    event::QUEUE,
    event::RPC,
    event::SCHEDULED,
    event::TAIL,
    event::WAITUNTIL,
    event::WEBSOCKETCLOSE,
    event::WEBSOCKETMESSAGE,
    event::WORKFLOW,
    module::NODE_ASSERT,
    module::NODE_ASYNC_HOOKS,
    module::NODE_BUFFER,
    module::NODE_CONSOLE,
    module::NODE_CRYPTO,
    module::NODE_DIAGNOSTICS_CHANNEL,
    module::NODE_DNS,
    module::NODE_DNS_PROMISES,
    module::NODE_EVENTS,
    module::NODE_FS,
    module::NODE_FS_PROMISES,
    module::NODE_HTTP,
    module::NODE_HTTPS,
    module::NODE_MODULE,
    module::NODE_NET,
    module::NODE_OS,
    module::NODE_PATH,
    module::NODE_PERF_HOOKS,
    module::NODE_PROCESS,
    module::NODE_PUNYCODE,
    module::NODE_QUERYSTRING,
    module::NODE_STREAM,
    module::NODE_STREAM_CONSUMERS,
    module::NODE_STREAM_PROMISES,
    module::NODE_STREAM_WEB,
    module::NODE_STRING_DECODER,
    module::NODE_TEST,
    module::NODE_TIMERS,
    module::NODE_TIMERS_PROMISES,
    module::NODE_TLS,
    module::NODE_URL,
    module::NODE_UTIL,
    module::NODE_ZLIB,
    binding::AI,
    binding::ANALYTICS_ENGINE,
    binding::ASSETS,
    binding::CONTAINER,
    binding::D1_DATABASE,
    binding::DISPATCHER,
    binding::DURABLE_OBJECT_NAMESPACE,
    binding::HYPERDRIVE,
    binding::IMAGES,
    binding::KV,
    binding::LOADER,
    binding::MTLS_CERTIFICATE,
    binding::OUTBOUND,
    binding::QUEUE,
    binding::R2_BUCKET,
    binding::RATE_LIMITER,
    binding::SECRETS_STORE_SECRET,
    binding::SERVICE,
    binding::VECTORIZE,
    binding::WORKFLOW,
];

const fn binding(
    name: &'static str,
    status: CapabilityStatus,
    evidence: &'static str,
) -> RuntimeCapability {
    RuntimeCapability {
        kind: CapabilityKind::Binding,
        name,
        status,
        evidence,
    }
}

const fn event(
    name: &'static str,
    status: CapabilityStatus,
    evidence: &'static str,
) -> RuntimeCapability {
    RuntimeCapability {
        kind: CapabilityKind::Event,
        name,
        status,
        evidence,
    }
}

const fn global(
    name: &'static str,
    status: CapabilityStatus,
    evidence: &'static str,
) -> RuntimeCapability {
    RuntimeCapability {
        kind: CapabilityKind::Global,
        name,
        status,
        evidence,
    }
}

const fn module(
    name: &'static str,
    status: CapabilityStatus,
    evidence: &'static str,
) -> RuntimeCapability {
    RuntimeCapability {
        kind: CapabilityKind::Module,
        name,
        status,
        evidence,
    }
}
