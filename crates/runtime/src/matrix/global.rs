use super::{CapabilityStatus, RuntimeCapability, global};

pub(super) const ABORTCONTROLLER: RuntimeCapability = global(
    "AbortController",
    CapabilityStatus::Supported,
    "isolate exposes web globals through deno_web",
);

pub(super) const ABORTSIGNAL: RuntimeCapability = global(
    "AbortSignal",
    CapabilityStatus::Supported,
    "isolate exposes web globals through deno_web",
);

pub(super) const AI: RuntimeCapability = global(
    "Ai",
    CapabilityStatus::Supported,
    "AI bindings are instances of Ai and expose run plus provider-normalized embed, generateText and chat helpers across HTTP and local gateways in isolate tests",
);

pub(super) const ANALYTICSENGINEDATASET: RuntimeCapability = global(
    "AnalyticsEngineDataset",
    CapabilityStatus::Supported,
    "env analytics bindings are instances of AnalyticsEngineDataset and writeDataPoint is hydrated as a local process buffer",
);

pub(super) const BROADCASTCHANNEL: RuntimeCapability = global(
    "BroadcastChannel",
    CapabilityStatus::Supported,
    "BroadcastChannel is exposed through deno_web, uses the in-memory runtime channel host, and same-runtime message delivery is isolate-tested",
);

pub(super) const BYTELENGTHQUEUINGSTRATEGY: RuntimeCapability = global(
    "ByteLengthQueuingStrategy",
    CapabilityStatus::Supported,
    "size/highWaterMark behavior is runtime-tested and exported through node:stream/web",
);

pub(super) const BLOB: RuntimeCapability = global(
    "Blob",
    CapabilityStatus::Supported,
    "File API Blob constructor, size/type, text, arrayBuffer, stream, and slice behavior are runtime-tested",
);

pub(super) const COMPRESSIONSTREAM: RuntimeCapability = global(
    "CompressionStream",
    CapabilityStatus::Supported,
    "gzip compression and decompression stream round-trip is runtime-tested",
);

pub(super) const COUNTQUEUINGSTRATEGY: RuntimeCapability = global(
    "CountQueuingStrategy",
    CapabilityStatus::Supported,
    "size/highWaterMark behavior is runtime-tested and exported through node:stream/web",
);

pub(super) const CRYPTOKEY: RuntimeCapability = global(
    "CryptoKey",
    CapabilityStatus::Partial,
    "CryptoKey covers raw HMAC with SHA-1/SHA-2, PBKDF2/HKDF deriveBits/deriveKey, AES-GCM and AES-CBC 128/192/256-bit encrypt/decrypt, Ed25519, and ECDSA P-256 public-key generate/import/export public/sign/verify; RSA and portable private-key formats are unsupported and refuse with WebCrypto NotSupportedError",
);

pub(super) const D1DATABASE: RuntimeCapability = global(
    "D1Database",
    CapabilityStatus::Supported,
    "env D1 bindings are instances of the D1Database constructor and preserve native_sqlite provider behavior in isolate tests",
);

pub(super) const D1PREPAREDSTATEMENT: RuntimeCapability = global(
    "D1PreparedStatement",
    CapabilityStatus::Supported,
    "D1Database.prepare returns D1PreparedStatement instances and bind/run/first/all/raw behavior is isolate-tested through native D1 bindings",
);

pub(super) const DECOMPRESSIONSTREAM: RuntimeCapability = global(
    "DecompressionStream",
    CapabilityStatus::Supported,
    "gzip compression and decompression stream round-trip is runtime-tested",
);

pub(super) const DOMEXCEPTION: RuntimeCapability = global(
    "DOMException",
    CapabilityStatus::Supported,
    "isolate exposes web globals through deno_web",
);

pub(super) const DISPATCHNAMESPACE: RuntimeCapability = global(
    "DispatchNamespace",
    CapabilityStatus::Supported,
    "dispatch namespace bindings expose configured scripts and route fetch through the service host",
);

pub(super) const DURABLEOBJECTNAMESPACE: RuntimeCapability = global(
    "DurableObjectNamespace",
    CapabilityStatus::Supported,
    "namespace id/string/stub shape and stub fetch routing to durable object service cells are process-tested",
);

pub(super) const EVENT: RuntimeCapability = global(
    "Event",
    CapabilityStatus::Supported,
    "Event construction, bubbling/cancelable metadata, listener dispatch, preventDefault, and EventTarget removal are runtime-tested",
);

pub(super) const EVENTSOURCE: RuntimeCapability = global(
    "EventSource",
    CapabilityStatus::Supported,
    "EventSource parses host-gated text/event-stream responses and dispatches open, message, named events, error, readyState and close behavior in isolate tests",
);

pub(super) const EVENTTARGET: RuntimeCapability = global(
    "EventTarget",
    CapabilityStatus::Supported,
    "EventTarget add/remove/dispatch listener behavior is runtime-tested",
);

pub(super) const FILE: RuntimeCapability = global(
    "File",
    CapabilityStatus::Supported,
    "File constructor, name, lastModified, inherited Blob text and type behavior are runtime-tested",
);

pub(super) const FILEREADER: RuntimeCapability = global(
    "FileReader",
    CapabilityStatus::Supported,
    "readAsText, readAsArrayBuffer, readAsDataURL, constants, readyState, and load/loadend events are runtime-tested",
);

pub(super) const FORMDATA: RuntimeCapability = global(
    "FormData",
    CapabilityStatus::Supported,
    "append/set/delete/get/getAll/iteration, File entries, Request body conversion, and formData parsing are runtime-tested",
);

pub(super) const HEADERS: RuntimeCapability = global(
    "Headers",
    CapabilityStatus::Supported,
    "HTTP dispatch tests exercise Headers/Response",
);

pub(super) const HYPERDRIVE: RuntimeCapability = global(
    "Hyperdrive",
    CapabilityStatus::Supported,
    "env Hyperdrive bindings are instances of Hyperdrive and expose redacted connection string, cache policy and pool metadata",
);

pub(super) const KVNAMESPACE: RuntimeCapability = global(
    "KvNamespace",
    CapabilityStatus::Supported,
    "env KV bindings are instances of the KvNamespace constructor and preserve native get/put/delete/list plus metadata and expiration behavior in isolate tests",
);

pub(super) const LOADER: RuntimeCapability = global(
    "Loader",
    CapabilityStatus::Supported,
    "Loader bindings expose guarded dynamic import for bundled relative modules",
);

pub(super) const MESSAGECHANNEL: RuntimeCapability = global(
    "MessageChannel",
    CapabilityStatus::Supported,
    "MessageChannel delivery, queuing, close, cloned payloads, transferred ports, and sender-reference detachment are isolate-tested",
);

pub(super) const MESSAGEEVENT: RuntimeCapability = global(
    "MessageEvent",
    CapabilityStatus::Supported,
    "MessageEvent is exposed through deno_web and exercised by BroadcastChannel and MessageChannel delivery tests",
);

pub(super) const MESSAGEPORT: RuntimeCapability = global(
    "MessagePort",
    CapabilityStatus::Supported,
    "MessagePort delivery, queuing, close, cloned payloads, transferred ports, and sender-reference detachment are isolate-tested",
);

pub(super) const NONRETRYABLEERROR: RuntimeCapability = global(
    "NonRetryableError",
    CapabilityStatus::Supported,
    "queue handlers can throw NonRetryableError to skip retries; isolate and node delivery tests cover the contract",
);

pub(super) const QUEUE: RuntimeCapability = global(
    "Queue",
    CapabilityStatus::Supported,
    "env queue bindings are instances of the Queue constructor and preserve provider-backed send behavior in isolate tests",
);

pub(super) const R2BUCKET: RuntimeCapability = global(
    "R2Bucket",
    CapabilityStatus::Supported,
    "env R2 bindings are instances of the R2Bucket constructor, preserve provider-backed object behavior in isolate tests, and enqueue configured object notifications in node tests",
);

pub(super) const PERFORMANCE: RuntimeCapability = global(
    "Performance",
    CapabilityStatus::Supported,
    "performance object is an instance of Performance and monotonic now/timeOrigin behavior is runtime-tested",
);

pub(super) const PROGRESSEVENT: RuntimeCapability = global(
    "ProgressEvent",
    CapabilityStatus::Supported,
    "FileReader load/loadend ProgressEvent dispatch is runtime-tested",
);

pub(super) const RATELIMITER: RuntimeCapability = global(
    "RateLimiter",
    CapabilityStatus::Supported,
    "env rate limiter bindings are instances of RateLimiter and limit local process-window behavior is isolate-tested",
);

pub(super) const READABLESTREAM: RuntimeCapability = global(
    "ReadableStream",
    CapabilityStatus::Supported,
    "isolate exposes stream constructors through deno_web",
);

pub(super) const REQUEST: RuntimeCapability = global(
    "Request",
    CapabilityStatus::Supported,
    "HTTP dispatch tests exercise Request",
);

pub(super) const RESPONSE: RuntimeCapability = global(
    "Response",
    CapabilityStatus::Supported,
    "HTTP dispatch materializes Response bodies under a byte budget; oversized streamed bodies are refused before host return",
);

pub(super) const TEXTDECODER: RuntimeCapability = global(
    "TextDecoder",
    CapabilityStatus::Supported,
    "bootstrap exposes encoding globals",
);

pub(super) const TEXTENCODER: RuntimeCapability = global(
    "TextEncoder",
    CapabilityStatus::Supported,
    "bootstrap exposes encoding globals",
);

pub(super) const TEXTDECODERSTREAM: RuntimeCapability = global(
    "TextDecoderStream",
    CapabilityStatus::Supported,
    "streaming text decode across chunk boundaries is runtime-tested",
);

pub(super) const TEXTENCODERSTREAM: RuntimeCapability = global(
    "TextEncoderStream",
    CapabilityStatus::Supported,
    "streaming text encode is runtime-tested",
);

pub(super) const TRANSFORMSTREAM: RuntimeCapability = global(
    "TransformStream",
    CapabilityStatus::Supported,
    "isolate exposes stream constructors through deno_web",
);

pub(super) const URL: RuntimeCapability = global(
    "URL",
    CapabilityStatus::Supported,
    "bootstrap exposes URL globals",
);

pub(super) const URLPATTERN: RuntimeCapability = global(
    "URLPattern",
    CapabilityStatus::Supported,
    "URLPattern construction, test, exec, base URL, ignoreCase, and named path groups are runtime-tested",
);

pub(super) const URLSEARCHPARAMS: RuntimeCapability = global(
    "URLSearchParams",
    CapabilityStatus::Supported,
    "bootstrap exposes URL globals",
);

pub(super) const VECTORIZEINDEX: RuntimeCapability = global(
    "VectorizeIndex",
    CapabilityStatus::Supported,
    "env vector bindings are instances of VectorizeIndex and support cell-storage-backed local indexes plus provider-routed and normalized Qdrant, Pinecone, Weaviate, and HTTP behavior",
);

pub(super) const WEBSOCKET: RuntimeCapability = global(
    "WebSocket",
    CapabilityStatus::Supported,
    "in-isolate WebSocketPeer instances returned by WebSocketPair are runtime-tested",
);

pub(super) const WEBSOCKETPAIR: RuntimeCapability = global(
    "WebSocketPair",
    CapabilityStatus::Partial,
    "in-isolate WebSocketPair accept/send/message/close lifecycle is runtime-tested; node HTTP upgrade handshake, selected subprotocol preservation, message frame bridging, stable Peren session handle, and session cleanup are process-tested; DurableObjectState acceptWebSocket/getWebSockets/auto-response, persisted accepted-socket metadata, dispatch hydration, and close cleanup are runtime-tested for the local Peren host model; full Cloudflare edge WebSocketPair parity outside that local host model remains unsupported",
);

pub(super) const WEBSOCKETREQUESTRESPONSEPAIR: RuntimeCapability = global(
    "WebSocketRequestResponsePair",
    CapabilityStatus::Supported,
    "cloudflare:workers exports WebSocketRequestResponsePair; DurableObjectState auto-response and accepted WebSocket lookup are runtime-tested",
);

pub(super) const WORKFLOW: RuntimeCapability = global(
    "Workflow",
    CapabilityStatus::Supported,
    "Workflow constructor, binding instance state, durable step replay and activity dispatch are runtime-tested",
);

pub(super) const WRITABLESTREAM: RuntimeCapability = global(
    "WritableStream",
    CapabilityStatus::Supported,
    "isolate exposes stream constructors through deno_web",
);

pub(super) const ADDEVENTLISTENER: RuntimeCapability = global(
    "addEventListener",
    CapabilityStatus::Supported,
    "fetch, scheduled, queue, and tail listener dispatch is isolate-tested",
);

pub(super) const ATOB: RuntimeCapability = global(
    "atob",
    CapabilityStatus::Supported,
    "base64 encode/decode behavior is dispatch-tested",
);

pub(super) const BTOA: RuntimeCapability = global(
    "btoa",
    CapabilityStatus::Supported,
    "base64 encode/decode behavior is dispatch-tested",
);

pub(super) const CACHE: RuntimeCapability = global(
    "Cache",
    CapabilityStatus::Supported,
    "Cache is a native constructor shape for provider-backed cache namespaces; match/put/delete behavior is dispatch-tested",
);

pub(super) const CACHESTORAGE: RuntimeCapability = global(
    "CacheStorage",
    CapabilityStatus::Supported,
    "CacheStorage is a native constructor shape; caches.default and caches.open return Cache instances",
);

pub(super) const CACHES: RuntimeCapability = global(
    "caches",
    CapabilityStatus::Supported,
    "caches is a CacheStorage instance; node hosts support memory, bucket-backed and Redis-backed cache stores with runtime-tested Worker behavior",
);

pub(super) const CLEARIMMEDIATE: RuntimeCapability = global(
    "clearImmediate",
    CapabilityStatus::Supported,
    "setImmediate and clearImmediate are dispatch-tested",
);

pub(super) const CLEARINTERVAL: RuntimeCapability = global(
    "clearInterval",
    CapabilityStatus::Supported,
    "web timer globals are exposed and dispatch-tested",
);

pub(super) const CLEARTIMEOUT: RuntimeCapability = global(
    "clearTimeout",
    CapabilityStatus::Supported,
    "web timer globals are exposed and dispatch-tested",
);

pub(super) const CONSOLE: RuntimeCapability = global(
    "console",
    CapabilityStatus::Supported,
    "console methods are exposed to worker dispatch; structured tail capture remains a node contract",
);

pub(super) const CRYPTO: RuntimeCapability = global(
    "crypto",
    CapabilityStatus::Partial,
    "getRandomValues, randomUUID, SHA-1/SHA-2 subtle.digest, HMAC, PBKDF2/HKDF deriveBits/deriveKey, AES-GCM and AES-CBC 128/192/256-bit encrypt/decrypt, Ed25519, and ECDSA P-256 are isolate-tested; RSA, ECDH, and the broader WebCrypto algorithm set remain unsupported",
);

pub(super) const FETCH: RuntimeCapability = global(
    "fetch",
    CapabilityStatus::Supported,
    "global fetch routes through the same host-gated outbound capability as Peren.fetch and is isolate-tested for request/response behavior and refusal without a host",
);

pub(super) const NAVIGATOR: RuntimeCapability = global(
    "navigator",
    CapabilityStatus::Supported,
    "navigator.userAgent is dispatch-tested",
);

pub(super) const PERFORMANCE_2: RuntimeCapability = global(
    "performance",
    CapabilityStatus::Supported,
    "performance.now and timeOrigin are exposed and dispatch-tested",
);

pub(super) const SETIMMEDIATE: RuntimeCapability = global(
    "setImmediate",
    CapabilityStatus::Supported,
    "setImmediate and clearImmediate are dispatch-tested",
);

pub(super) const SETINTERVAL: RuntimeCapability = global(
    "setInterval",
    CapabilityStatus::Supported,
    "web timer globals are exposed and dispatch-tested",
);

pub(super) const SETTIMEOUT: RuntimeCapability = global(
    "setTimeout",
    CapabilityStatus::Supported,
    "web timer globals are exposed and dispatch-tested",
);

pub(super) const STRUCTUREDCLONE: RuntimeCapability = global(
    "structuredClone",
    CapabilityStatus::Supported,
    "structuredClone is exposed and deep-copy behavior is dispatch-tested",
);
