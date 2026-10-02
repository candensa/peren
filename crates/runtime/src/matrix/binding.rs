use super::{CapabilityStatus, RuntimeCapability, binding};

pub(super) const AI: RuntimeCapability = binding(
    "ai",
    CapabilityStatus::Supported,
    "AI bindings expose raw env.AI.run plus provider-normalized embed { embeddings, raw } and generateText/chat { text, raw } helpers; OpenAI responses/embeddings, Anthropic messages, Gemini generateContent/embedContent, Workers AI, HTTP, and local gateways are provider-tested",
);

pub(super) const ANALYTICS_ENGINE: RuntimeCapability = binding(
    "analytics_engine",
    CapabilityStatus::Supported,
    "AnalyticsEngineDataset.writeDataPoint is hydrated as a local process buffer",
);

pub(super) const ASSETS: RuntimeCapability = binding(
    "assets",
    CapabilityStatus::Supported,
    "static assets are routed through the node process and covered by process tests",
);

pub(super) const CONTAINER: RuntimeCapability = binding(
    "container",
    CapabilityStatus::Partial,
    "experimental Containers/Sandbox-compatible bindings expose instance get/fetch through a host-gated local-port bridge; managed sandbox lifecycle, exec and file APIs are intentionally rejected until backed by a real supervisor provider",
);

pub(super) const D1_DATABASE: RuntimeCapability = binding(
    "d1_database",
    CapabilityStatus::Supported,
    "native SQLite prepare/bind/run/all/first/raw/exec/batch are runtime-tested; Turso/local libSQL routing and credential handling are provider-tested",
);

pub(super) const DISPATCHER: RuntimeCapability = binding(
    "dispatcher",
    CapabilityStatus::Supported,
    "dispatch namespace bindings expose configured scripts and route fetch through the service host",
);

pub(super) const DURABLE_OBJECT_NAMESPACE: RuntimeCapability = binding(
    "durable_object_namespace",
    CapabilityStatus::Supported,
    "config hydration, namespace id/stub shape, stub fetch routing to durable object service cells, in-isolate state id/props/exports ergonomics, facet RPC, and decoded storage transactions are tested; host-routed startup props require a future host protocol extension",
);

pub(super) const HYPERDRIVE: RuntimeCapability = binding(
    "hyperdrive",
    CapabilityStatus::Supported,
    "Hyperdrive exposes configured connection string, cache policy and pool metadata as a worker binding",
);

pub(super) const IMAGES: RuntimeCapability = binding(
    "images",
    CapabilityStatus::Supported,
    "Images bindings expose local passthrough and HTTP gateway transform contracts",
);

pub(super) const KV: RuntimeCapability = binding(
    "kv",
    CapabilityStatus::Supported,
    "native get/getWithMetadata/put/delete/list behavior, metadata, expiration, versioned compare-and-set, cursor normalization, and invalid-list-limit errors are runtime-tested and node-routed",
);

pub(super) const LOADER: RuntimeCapability = binding(
    "loader",
    CapabilityStatus::Supported,
    "Loader bindings expose guarded dynamic import for bundled relative modules",
);

pub(super) const MTLS_CERTIFICATE: RuntimeCapability = binding(
    "mtls_certificate",
    CapabilityStatus::Supported,
    "opaque worker binding, startup material validation, and outbound fetch identity propagation are tested",
);

pub(super) const OUTBOUND: RuntimeCapability = binding(
    "outbound",
    CapabilityStatus::Supported,
    "Peren.fetch host capability tests",
);

pub(super) const QUEUE: RuntimeCapability = binding(
    "queue",
    CapabilityStatus::Supported,
    "worker producer binding, memory/file/record-oriented cell/external broker lifecycle, Cloudflare-style limits, deterministic shard leasing, max-concurrency consumer planning, lease expiry, stale settlement fencing, retention, purge markers and node delivery are tested",
);

pub(super) const R2_BUCKET: RuntimeCapability = binding(
    "r2_bucket",
    CapabilityStatus::Supported,
    "put/get/delete/list behavior, object metadata, delete consistency, list cursor/truncation shape, explicit non-KV method surface, filtered object-create/object-delete queue notifications, provider adapter, node memory route, file-backed queue proof, and explicit S3-compatible credential routing are tested",
);

pub(super) const RATE_LIMITER: RuntimeCapability = binding(
    "rate_limiter",
    CapabilityStatus::Supported,
    "RateLimiter.limit local process-window behavior is hydrated as a worker binding",
);

pub(super) const SECRETS_STORE_SECRET: RuntimeCapability = binding(
    "secrets_store_secret",
    CapabilityStatus::Supported,
    "Secrets Store bindings resolve through provider-managed secret names and fail before listeners open when missing",
);

pub(super) const SERVICE: RuntimeCapability = binding(
    "service",
    CapabilityStatus::Supported,
    "worker service binding fetch reaches the runtime host boundary and node process routes fetches to the target service worker",
);

pub(super) const VECTORIZE: RuntimeCapability = binding(
    "vectorize",
    CapabilityStatus::Supported,
    "Vectorize bindings support cell-storage-backed local upsert/query/getByIds/deleteByIds plus normalized Qdrant, Pinecone and Weaviate query/fetch/delete result shapes with tested HTTP request routing",
);

pub(super) const WORKFLOW: RuntimeCapability = binding(
    "workflow",
    CapabilityStatus::Supported,
    "workflow event dispatch, step.do replay, durable step.sleep replay and activity dispatch are runtime-tested through runtime and node paths",
);
