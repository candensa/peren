use super::{CapabilityStatus, RuntimeCapability, module};

pub(super) const NODE_ASSERT: RuntimeCapability = module(
    "node:assert",
    CapabilityStatus::Supported,
    "assertion functions, AssertionError, sync throws, async rejects, and deep equality are runtime-tested",
);

pub(super) const NODE_ASYNC_HOOKS: RuntimeCapability = module(
    "node:async_hooks",
    CapabilityStatus::Partial,
    "AsyncLocalStorage, AsyncResource, createHook enable/disable, and async id helpers are runtime-tested; remaining gap is full Node async lifecycle hook semantics",
);

pub(super) const NODE_BUFFER: RuntimeCapability = module(
    "node:buffer",
    CapabilityStatus::Supported,
    "Buffer construction, encoding, concat, compare, fill, write, and JSON behavior are runtime-tested",
);

pub(super) const NODE_CONSOLE: RuntimeCapability = module(
    "node:console",
    CapabilityStatus::Supported,
    "console method re-exports and Console shape are runtime-tested",
);

pub(super) const NODE_CRYPTO: RuntimeCapability = module(
    "node:crypto",
    CapabilityStatus::Partial,
    "createHash, createHmac, createSecretKey, secret KeyObject export, getHashes, randomBytes, randomUUID, timingSafeEqual, and webcrypto are runtime-tested; stream transforms and asymmetric key-object APIs remain outside the current subset",
);

pub(super) const NODE_DIAGNOSTICS_CHANNEL: RuntimeCapability = module(
    "node:diagnostics_channel",
    CapabilityStatus::Supported,
    "channel subscribe/publish/unsubscribe, hasSubscribers, tracingChannel, and global subscribeAll hooks are runtime-tested for instrumentation package compatibility",
);

pub(super) const NODE_DNS: RuntimeCapability = module(
    "node:dns",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_DNS_PROMISES: RuntimeCapability = module(
    "node:dns/promises",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_EVENTS: RuntimeCapability = module(
    "node:events",
    CapabilityStatus::Supported,
    "EventEmitter listener, once, removal, error, and static helpers are runtime-tested",
);

pub(super) const NODE_FS: RuntimeCapability = module(
    "node:fs",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_FS_PROMISES: RuntimeCapability = module(
    "node:fs/promises",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_HTTP: RuntimeCapability = module(
    "node:http",
    CapabilityStatus::Partial,
    "request/get client helpers route through host-gated fetch and are runtime-tested; remaining gap is server, Agent pooling, and classic socket semantics",
);

pub(super) const NODE_HTTPS: RuntimeCapability = module(
    "node:https",
    CapabilityStatus::Partial,
    "request/get client helpers route through host-gated fetch and are runtime-tested; remaining gap is server, Agent pooling, and classic TLS/socket semantics",
);

pub(super) const NODE_MODULE: RuntimeCapability = module(
    "node:module",
    CapabilityStatus::Supported,
    "builtinModules, isBuiltin, syncBuiltinESMExports, and createRequire for supported builtins are runtime-tested; filesystem-relative require is refused",
);

pub(super) const NODE_NET: RuntimeCapability = module(
    "node:net",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_OS: RuntimeCapability = module(
    "node:os",
    CapabilityStatus::Supported,
    "stable Worker-safe OS constants and sandbox-shaped host metadata are runtime-tested",
);

pub(super) const NODE_PATH: RuntimeCapability = module(
    "node:path",
    CapabilityStatus::Supported,
    "POSIX path normalize/join/resolve/relative/parse helpers are runtime-tested; win32 is refused",
);

pub(super) const NODE_PERF_HOOKS: RuntimeCapability = module(
    "node:perf_hooks",
    CapabilityStatus::Supported,
    "performance re-export is runtime-tested; observer and event-loop instrumentation refuse explicitly",
);

pub(super) const NODE_PROCESS: RuntimeCapability = module(
    "node:process",
    CapabilityStatus::Supported,
    "minimal process metadata, env, nextTick, hrtime, uptime, and refusal paths are runtime-tested",
);

pub(super) const NODE_PUNYCODE: RuntimeCapability = module(
    "node:punycode",
    CapabilityStatus::Supported,
    "toASCII, toUnicode, ucs2 encode/decode, and legacy encode/decode helpers are runtime-tested for package compatibility",
);

pub(super) const NODE_QUERYSTRING: RuntimeCapability = module(
    "node:querystring",
    CapabilityStatus::Supported,
    "parse/stringify/escape/unescape compatibility helpers are runtime-tested",
);

pub(super) const NODE_STREAM: RuntimeCapability = module(
    "node:stream",
    CapabilityStatus::Partial,
    "Web Stream constructors are re-exported and classic Node stream constructors refuse explicitly with Worker guidance; runtime-tested for package import compatibility; remaining gap is classic Node stream runtime semantics",
);

pub(super) const NODE_STREAM_CONSUMERS: RuntimeCapability = module(
    "node:stream/consumers",
    CapabilityStatus::Supported,
    "buffer, arrayBuffer, text, json, and blob consumers for Web streams and byte-like inputs are runtime-tested",
);

pub(super) const NODE_STREAM_PROMISES: RuntimeCapability = module(
    "node:stream/promises",
    CapabilityStatus::Partial,
    "pipeline and finished support Web Streams and byte-like sources in runtime tests; remaining gap is classic Node stream runtime semantics",
);

pub(super) const NODE_STREAM_WEB: RuntimeCapability = module(
    "node:stream/web",
    CapabilityStatus::Supported,
    "WHATWG stream constructors are re-exported and runtime-tested against global stream constructors",
);

pub(super) const NODE_STRING_DECODER: RuntimeCapability = module(
    "node:string_decoder",
    CapabilityStatus::Supported,
    "StringDecoder UTF-8 split-codepoint buffering is runtime-tested",
);

pub(super) const NODE_TEST: RuntimeCapability = module(
    "node:test",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_TIMERS: RuntimeCapability = module(
    "node:timers",
    CapabilityStatus::Supported,
    "timer exports delegate to runtime timer globals and are runtime-tested",
);

pub(super) const NODE_TIMERS_PROMISES: RuntimeCapability = module(
    "node:timers/promises",
    CapabilityStatus::Supported,
    "promise timers for timeout/immediate/interval are runtime-tested for timeout import",
);

pub(super) const NODE_TLS: RuntimeCapability = module(
    "node:tls",
    CapabilityStatus::Unsupported,
    "module imports resolve to explicit Worker-isolate refusal stubs; runtime behavior remains unsupported",
);

pub(super) const NODE_URL: RuntimeCapability = module(
    "node:url",
    CapabilityStatus::Supported,
    "WHATWG URL re-exports, legacy parse/format/resolve, file URL helpers, HTTP options, and domain ASCII conversion are runtime-tested",
);

pub(super) const NODE_UTIL: RuntimeCapability = module(
    "node:util",
    CapabilityStatus::Supported,
    "format, inspect, promisify, callbackify, deprecate, type predicates, and text codecs are runtime-tested",
);

pub(super) const NODE_ZLIB: RuntimeCapability = module(
    "node:zlib",
    CapabilityStatus::Partial,
    "async gzip/gunzip/deflate/inflate helpers use Web CompressionStream and are runtime-tested; remaining gap is synchronous APIs and classic transform stream constructors",
);
