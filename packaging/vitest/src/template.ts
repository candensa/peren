import type { ResolvedOptions } from "./options.js";

export function render(options: ResolvedOptions): string {
  return `
import { afterAll, beforeAll } from "vitest";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

const PEREN_BIN = ${JSON.stringify(options.perenBin)};
const CONFIG_PATH = ${JSON.stringify(options.configPath)};
const CONFIG_CWD = ${JSON.stringify(options.configCwd)};
const WRANGLER_PATHS = ${JSON.stringify(options.wranglerPaths)};
const READY_TIMEOUT_MS = ${JSON.stringify(options.readyTimeoutMs)};
const SOCKET_NAME = ${JSON.stringify(options.socketName)};
const SERVICE = ${JSON.stringify(options.service)};
const BINDINGS = ${JSON.stringify(options.bindings)};
const CACHE_PROVIDER = Object.freeze(${JSON.stringify(options.cache)});

let child = null;
let baseUrl = null;
let bootError = null;

function waitForReady(proc, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const lines = createInterface({ input: proc.stdout });
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      lines.close();
      reject(new Error(\`peren test-server did not become ready within \${timeoutMs}ms\`));
    }, timeoutMs);

    proc.stderr.on("data", (chunk) => process.stderr.write(\`[peren test-server] \${chunk}\`));
    proc.once("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      lines.close();
      reject(error);
    });
    proc.once("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      lines.close();
      reject(new Error(\`peren test-server exited before readiness (code=\${code}, signal=\${signal})\`));
    });
    lines.on("line", (line) => {
      if (settled) return;
      let parsed;
      try { parsed = JSON.parse(line); } catch { return; }
      if (parsed?.ready === true) {
        settled = true;
        clearTimeout(timer);
        lines.close();
        resolve(parsed);
      }
    });
  });
}

beforeAll(async () => {
  const args = ["test-server", CONFIG_PATH];
  for (const path of WRANGLER_PATHS) args.push("--wrangler", path);
  const proc = spawn(PEREN_BIN, args, { cwd: CONFIG_CWD, stdio: ["ignore", "pipe", "pipe"] });
  child = proc;
  try {
    const ready = await waitForReady(proc, READY_TIMEOUT_MS);
    const port = ready.sockets?.[SOCKET_NAME];
    if (port === undefined) throw new Error(\`ready record has no socket \${SOCKET_NAME}\`);
    baseUrl = \`http://127.0.0.1:\${port}\`;
  } catch (error) {
    bootError = error;
    throw error;
  }
}, READY_TIMEOUT_MS + 10_000);

afterAll(async () => {
  const proc = child;
  child = null;
  baseUrl = null;
  if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
  proc.kill("SIGTERM");
  await new Promise((resolve) => {
    const timer = setTimeout(() => { proc.kill("SIGKILL"); resolve(undefined); }, 5000);
    proc.once("exit", () => { clearTimeout(timer); resolve(undefined); });
  });
});

process.once("exit", () => {
  if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
});

function requireBaseUrl() {
  if (bootError) throw new Error(\`peren test-server failed: \${bootError.message ?? bootError}\`);
  if (!baseUrl) throw new Error("peren test-server is not ready; use test helpers inside a test or hook");
  return baseUrl;
}

function rewrite(input, base) {
  const source = typeof input === "string" ? new URL(input, base)
    : input instanceof URL ? input
    : input?.url ? new URL(input.url, base)
    : null;
  if (source === null) throw new TypeError("expected string, URL, or Request-like input");
  const target = new URL(base);
  target.pathname = source.pathname;
  target.search = source.search;
  target.hash = source.hash;
  return target;
}

function plain(input) {
  return typeof input === "string" || input instanceof URL;
}

export const SELF = {
  fetch(input, init) {
    const target = rewrite(input, requireBaseUrl());
    if (!plain(input) && input?.url) return fetch(new Request(target, input), init);
    return fetch(target, init);
  },
};

export function getPerenTestServerUrl() {
  return requireBaseUrl();
}

function cacheKey(input, options = {}) {
  const request = input instanceof Request ? input : new Request(input);
  const url = new URL(request.url);
  if (options.ignoreSearch) url.search = "";
  return request.method.toUpperCase() + " " + url.toString();
}

function cacheVaryNames(response) {
  const value = response.headers.get("vary");
  if (value === null) return [];
  return value
    .split(",")
    .map((name) => name.trim().toLowerCase())
    .filter((name, index, names) => name.length > 0 && names.indexOf(name) === index)
    .sort();
}

function cacheVariantKey(base, request, vary) {
  const headers = vary.map((name) => [name, request.headers.get(name) ?? ""]);
  return base + "\\nvary:" + JSON.stringify(headers);
}

function matchingCacheVariant(request, base, variants) {
  for (const variant of variants) {
    if (!Array.isArray(variant?.vary)) continue;
    if (cacheVariantKey(base, request, variant.vary) === variant.key) return variant;
  }
  return undefined;
}

function validateCachePut(request, response) {
  const req = request instanceof Request ? request : new Request(request);
  if (req.method.toUpperCase() !== "GET") throw new TypeError("Cache.put request method must be GET");
  if (response.status === 206) throw new TypeError("Cache.put does not accept partial responses");
  if (response.headers.has("vary") && response.headers.get("vary").split(",").some((value) => value.trim() === "*")) {
    throw new TypeError("Cache.put does not accept Vary: * responses");
  }
  return req;
}

function buildCache(name) {
  const entries = new Map();
  const variants = new Map();
  return Object.freeze({
    provider: CACHE_PROVIDER,
    async match(request, options = {}) {
      const req = request instanceof Request ? request : new Request(request);
      const key = cacheKey(req, options);
      const indexed = variants.get(key) ?? [];
      let response;
      if (indexed.length > 0) {
        const variant = options.ignoreVary ? indexed[0] : matchingCacheVariant(req, key, indexed);
        if (variant !== undefined) response = entries.get(variant.key);
      }
      response ??= entries.get(key);
      return response === undefined ? undefined : response.clone();
    },
    async put(request, response) {
      const res = response instanceof Response ? response : new Response(response);
      const req = validateCachePut(request, res);
      const key = cacheKey(req);
      const vary = cacheVaryNames(res);
      if (vary.length === 0) {
        entries.set(key, res.clone());
        return;
      }
      const storedKey = cacheVariantKey(key, req, vary);
      entries.set(storedKey, res.clone());
      const indexed = (variants.get(key) ?? []).filter((variant) => variant.key !== storedKey);
      indexed.push({ vary, key: storedKey });
      variants.set(key, indexed);
    },
    async delete(request, options = {}) {
      const req = request instanceof Request ? request : new Request(request);
      const key = cacheKey(req, options);
      const indexed = variants.get(key) ?? [];
      if (indexed.length === 0) return entries.delete(key);
      if (options.ignoreVary) {
        let deleted = entries.delete(key);
        for (const variant of indexed) deleted = entries.delete(variant.key) || deleted;
        variants.delete(key);
        return deleted;
      }
      const variant = matchingCacheVariant(req, key, indexed);
      if (variant === undefined) return false;
      const deleted = entries.delete(variant.key);
      variants.set(key, indexed.filter((entry) => entry.key !== variant.key));
      return deleted;
    },
  });
}

const namedCaches = new Map([["default", buildCache("default")]]);
export const caches = Object.freeze({
  default: namedCaches.get("default"),
  async open(name) {
    const value = String(name);
    if (value.length === 0) throw new TypeError("cache name cannot be empty");
    let cache = namedCaches.get(value);
    if (cache === undefined) {
      cache = buildCache(value);
      namedCaches.set(value, cache);
    }
    return cache;
  },
});

globalThis.caches ??= caches;


async function invoke(name, method, args) {
  const response = await fetch(new URL(\`/__test/binding/\${encodeURIComponent(SERVICE)}/\${encodeURIComponent(name)}/invoke\`, requireBaseUrl()), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ method, args }),
  });
  const body = await response.json();
  if (!response.ok) throw new Error(\`env.\${name}.\${method}: \${body.error ?? response.statusText}\`);
  return body.result;
}

const PROVIDER_SECRET_KEYS = new Set([
  "authorization",
  "apiKey",
  "api_key",
  "xApiKey",
  "x_api_key",
  "token",
  "accessKey",
  "access_key",
  "secretKey",
  "secret_key",
  "secretAccessKey",
  "secret_access_key",
  "connectionString",
  "connection_string",
]);

function publicProvider(provider) {
  if (provider == null || typeof provider !== "object") return undefined;
  const visible = {};
  for (const [key, value] of Object.entries(provider)) {
    if (PROVIDER_SECRET_KEYS.has(key)) continue;
    visible[key === "url" ? "endpoint" : key] = value;
  }
  return Object.freeze(visible);
}

function provider(name, fallback) {
  return publicProvider(BINDINGS[name]?.provider) ?? Object.freeze(fallback);
}

function decodeKvValue(encoded, opts) {
  if (encoded === null) return null;
  if (encoded && typeof encoded === "object" && Array.isArray(encoded.__perenBytes)) {
    const bytes = new Uint8Array(encoded.__perenBytes);
    if (opts?.type === "arrayBuffer") return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    if (opts?.type === "stream") return new Response(bytes).body;
  }
  return encoded;
}

function kv(name) {
  return {
    provider: provider(name, { kind: "native" }),
    async get(key, opts) {
      return decodeKvValue(await invoke(name, "kvGet", opts === undefined ? [key] : [key, opts]), opts);
    },
    async getWithMetadata(key, opts) {
      const result = await invoke(name, "kvGetWithMetadata", opts === undefined ? [key] : [key, opts]);
      return { value: decodeKvValue(result.value, opts), metadata: result.metadata ?? null, version: result.version ?? null };
    },
    compareAndSet: (key, expected, value, opts) => invoke(name, "compareAndSet", opts === undefined ? [key, expected, value] : [key, expected, value, opts]),
    put: (key, value, opts) => invoke(name, "put", opts === undefined ? [key, value] : [key, value, opts]),
    delete: (key) => invoke(name, "delete", [key]),
    list: (opts) => invoke(name, "list", opts === undefined ? [] : [opts]),
  };
}

function d1(name) {
  return {
    provider: provider(name, { kind: "native_sqlite" }),
    prepare(sql) {
      let params = [];
      const statement = {
        get __perenSql() { return sql; },
        get __perenParams() { return params; },
        bind(...values) { params = values; return statement; },
        all: () => invoke(name, "queryAll", [sql, params]),
        first: () => invoke(name, "queryFirst", [sql, params]),
        run: () => invoke(name, "queryRun", [sql, params]),
        raw: () => invoke(name, "queryRaw", [sql, params]),
      };
      return statement;
    },
    exec: (sql) => invoke(name, "exec", [sql]),
    batch: (statements) => invoke(name, "batch", [Array.from(statements ?? [], (statement) => {
      if (!statement || typeof statement !== "object" || statement.__perenSql === undefined) {
        throw new TypeError("D1.batch expects statements created by env.DB.prepare()");
      }
      return { sql: statement.__perenSql, params: statement.__perenParams ?? [] };
    })]),
  };
}


function r2Object(object) {
  if (object === null) return null;
  const bytes = new Uint8Array(object.__perenBody ?? []);
  return Object.freeze({
    key: object.key,
    size: object.size ?? bytes.byteLength,
    httpMetadata: Object.freeze({ contentType: object.contentType ?? undefined }),
    customMetadata: Object.freeze(object.customMetadata ?? {}),
    arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    text: async () => new TextDecoder().decode(bytes),
    json: async () => JSON.parse(new TextDecoder().decode(bytes)),
  });
}

function r2(name) {
  return {
    provider: provider(name, { kind: "memory" }),
    get: async (key, opts) => r2Object(await invoke(name, "r2Get", opts === undefined ? [key] : [key, opts])),
    put: (key, value, opts) => invoke(name, "put", opts === undefined ? [key, value] : [key, value, opts]),
    delete: (keys) => invoke(name, "delete", [keys]),
    list: (opts) => invoke(name, "list", opts === undefined ? [] : [opts]),
  };
}

async function encodeRequest(input, init = {}) {
  const request = input instanceof Request ? input : new Request(input, init);
  const body = request.body === null ? [] : Array.from(new Uint8Array(await request.arrayBuffer()));
  return {
    method: request.method,
    url: request.url,
    headers: Array.from(request.headers.entries()),
    body,
  };
}

function decodeResponse(response) {
  return new Response(new Uint8Array(response.body ?? []), {
    status: response.status ?? 200,
    headers: response.headers ?? [],
  });
}

function service(name) {
  return Object.freeze({
    async fetch(input, init = {}) {
      return decodeResponse(await invoke(name, "fetch", [await encodeRequest(input, init)]));
    },
  });
}

function dispatcher(name, binding) {
  const namespace = String(binding?.namespace ?? "");
  const scripts = Object.freeze(Array.from(binding?.scripts ?? [], String));
  return Object.freeze({
    namespace,
    scripts,
    get(script) {
      const value = String(script);
      if (!scripts.includes(value)) throw new TypeError("unknown dispatch script " + value);
      return Object.freeze({
        async fetch(input, init = {}) {
          return decodeResponse(await invoke(name, "dispatchFetch", [value, await encodeRequest(input, init)]));
        },
      });
    },
  });
}

const workflowStates = new Map();
const workflowId = () => "workflow-" + Date.now() + "-" + Math.random().toString(16).slice(2);

class Workflow {
  constructor(binding, id) {
    this.binding = binding;
    this.id = id;
  }
  async status() {
    return workflowStates.get(this.binding + "/" + this.id) ?? { id: this.id, status: "unknown" };
  }
  async terminate(reason) {
    return writeWorkflowState(this.binding, this.id, "terminated", reason);
  }
  async restart() {
    return writeWorkflowState(this.binding, this.id, "running", undefined);
  }
}

function writeWorkflowState(binding, id, status, reason) {
  const state = { id, status, reason: reason == null ? undefined : String(reason), updatedAt: Date.now() };
  workflowStates.set(binding + "/" + id, state);
  return state;
}

function workflow(name) {
  return Object.freeze({
    provider: provider(name, { kind: "native" }),
    async create(options = {}) {
      const id = options.id == null ? workflowId() : String(options.id);
      writeWorkflowState(name, id, "running", undefined);
      return new Workflow(name, id);
    },
    get(id) {
      return new Workflow(name, String(id));
    },
  });
}

globalThis.Workflow ??= Workflow;

function queue(name) {
  return {
    provider: provider(name, { kind: "memory" }),
    send: (body, opts) => invoke(name, "send", opts === undefined ? [body] : [body, opts]),
    sendBatch: (messages) => invoke(name, "sendBatch", [messages]),
  };
}

function vectorize(name) {
  return {
    provider: provider(name, { kind: "local" }),
    upsert: (vectors) => invoke(name, "upsert", [vectors]),
    query: (vector, opts) => invoke(name, "query", opts === undefined ? [vector] : [vector, opts]),
    getByIds: (ids) => invoke(name, "getByIds", [ids]),
    deleteByIds: (ids) => invoke(name, "deleteByIds", [ids]),
  };
}

function aiTextOutput(raw) {
  if (raw && typeof raw === "object" && "text" in raw && "raw" in raw) return raw;
  if (typeof raw === "string") return { text: raw, raw };
  if (raw?.output_text !== undefined) return { text: String(raw.output_text), raw };
  if (Array.isArray(raw?.output)) {
    const text = raw.output
      .flatMap((item) => Array.isArray(item?.content) ? item.content : [])
      .filter((item) => item?.type === "output_text" || item?.text !== undefined)
      .map((item) => String(item.text ?? ""))
      .join("");
    if (text.length > 0) return { text, raw };
  }
  if (raw?.content !== undefined) {
    if (typeof raw.content === "string") return { text: raw.content, raw };
    if (Array.isArray(raw.content)) {
      const text = raw.content
        .filter((item) => item?.type === "text" || item?.text !== undefined)
        .map((item) => String(item.text ?? ""))
        .join("");
      if (text.length > 0) return { text, raw };
    }
  }
  const parts = raw?.candidates?.[0]?.content?.parts;
  if (Array.isArray(parts)) {
    const text = parts.map((part) => String(part.text ?? "")).join("");
    if (text.length > 0) return { text, raw };
  }
  if (raw?.response !== undefined) return { text: String(raw.response), raw };
  if (raw?.result?.response !== undefined) return { text: String(raw.result.response), raw };
  if (raw?.result?.text !== undefined) return { text: String(raw.result.text), raw };
  if (raw?.text !== undefined) return { text: String(raw.text), raw };
  return { text: "", raw };
}

function aiEmbeddingOutput(raw) {
  if (raw && typeof raw === "object" && "embeddings" in raw && "raw" in raw) return raw;
  if (Array.isArray(raw)) return { embeddings: raw, raw };
  if (Array.isArray(raw?.data)) return { embeddings: raw.data.map((item) => item.embedding ?? item.values ?? item.vector ?? item), raw };
  if (Array.isArray(raw?.embedding)) return { embeddings: [raw.embedding], raw };
  if (Array.isArray(raw?.embeddings)) return { embeddings: raw.embeddings, raw };
  if (Array.isArray(raw?.result?.data)) return { embeddings: raw.result.data.map((item) => item.embedding ?? item.values ?? item.vector ?? item), raw };
  if (Array.isArray(raw?.result?.embedding)) return { embeddings: [raw.result.embedding], raw };
  return { embeddings: [], raw };
}

function ai(name) {
  return {
    provider: provider(name, { kind: "http" }),
    run: (model, input, opts) => invoke(name, "run", opts === undefined ? [model, input] : [model, input, opts]),
    embed: async (model, input, opts) => aiEmbeddingOutput(await invoke(name, "embed", opts === undefined ? [model, input] : [model, input, opts])),
    generateText: async (model, prompt, opts) => aiTextOutput(await invoke(name, "generateText", opts === undefined ? [model, prompt] : [model, prompt, opts])),
    chat: async (model, messages, opts) => aiTextOutput(await invoke(name, "chat", opts === undefined ? [model, messages] : [model, messages, opts])),
  };
}

function rateLimiter(name) {
  return {
    provider: provider(name, { kind: "memory" }),
    limit: (request) => invoke(name, "limit", [request]),
  };
}

function analytics(name) {
  return {
    provider: provider(name, { kind: "buffer" }),
    writeDataPoint: (point) => invoke(name, "writeDataPoint", [point]),
  };
}

function durableTarget(stub) {
  if (!stub || typeof stub !== "object" || !stub.__perenNamespace) {
    throw new TypeError("expected a Durable Object stub created by env.NAMESPACE.get(id)");
  }
  return { namespace: stub.__perenNamespace, id: stub.__perenId };
}

function durableNameId(value) {
  return "name:" + btoa(String(value));
}

function durableUniqueId() {
  const random = globalThis.crypto?.randomUUID?.();
  if (random) return "unique:" + random;
  return "unique:" + Date.now().toString(36) + Math.random().toString(36).slice(2);
}


function bindingKind(binding) {
  return typeof binding === "string" ? binding : binding?.type ?? binding?.kind;
}

function outbound(name, binding) {
  const allowedHosts = Object.freeze(Array.from(binding?.allowedHosts ?? binding?.allowed_hosts ?? [], String));
  return Object.freeze({
    allowedHosts,
    async fetch(input, init = {}) {
      const request = input instanceof Request ? input : new Request(input, init);
      const host = new URL(request.url).host;
      if (allowedHosts.length > 0 && !allowedHosts.includes(host)) {
        throw new TypeError("env." + name + ".fetch blocked host " + host);
      }
      return await fetch(request);
    },
  });
}

function loader() {
  return Object.freeze({
    async import(specifier) {
      const value = String(specifier);
      if (/^(?:https?:|node:|npm:|jsr:)/.test(value)) throw new TypeError("unsupported dynamic module specifier " + value);
      return await import(value);
    },
  });
}

function hyperdrive(binding) {
  const connectionString = String(binding?.connectionString ?? binding?.pgcat_endpoint ?? "");
  return Object.freeze({
    provider: Object.freeze({ kind: "pgcat" }),
    connectionString,
    host: connectionString,
    cachingDisabled: Boolean(binding?.caching_disabled ?? binding?.cachingDisabled),
    maxAge: Number(binding?.max_age_secs ?? binding?.maxAge ?? 0),
    staleWhileRevalidate: Number(binding?.stale_while_revalidate_secs ?? binding?.staleWhileRevalidate ?? 0),
    poolMaxConnections: Number(binding?.pool_max_connections ?? binding?.poolMaxConnections ?? 1),
  });
}

function container(binding) {
  const visible = publicProvider(binding?.provider) ?? Object.freeze({ kind: "local" });
  const port = Number(binding?.default_port ?? binding?.port);
  const target = Number.isFinite(port) && port > 0 ? "http://127.0.0.1:" + port : null;
  const unsupportedControl = (operation) => {
    throw new TypeError("Container." + operation + " requires a managed sandbox provider; this binding currently supports fetch() through the configured local port");
  };
  const instance = (name = "default", freeze = true) => {
    const id = String(name);
    const api = {
      id,
      image: String(binding?.image ?? ""),
      port,
      memoryMb: binding?.memory_mb ?? binding?.memoryMb ?? null,
      cpuMillis: binding?.cpu_millis ?? binding?.cpuMillis ?? null,
      idleSleepSecs: Number(binding?.idle_sleep_secs ?? binding?.idleSleepSecs ?? 0),
      allowNetworkEgress: Boolean(binding?.allow_network_egress ?? binding?.allowNetworkEgress),
      experimental: true,
      provider: visible,
      async fetch(input, init = {}) {
        if (target === null) throw new Error("container binding has no valid default_port");
        const request = input instanceof Request ? input : new Request(input, init);
        const source = new URL(request.url);
        const destination = new URL(target);
        destination.pathname = source.pathname;
        destination.search = source.search;
        return await fetch(new Request(destination, request));
      },
      async status() {
        unsupportedControl("status");
      },
      async start() {
        unsupportedControl("start");
      },
      async stop() {
        unsupportedControl("stop");
      },
      async destroy() {
        unsupportedControl("destroy");
      },
      async exec(_command, _options = {}) {
        unsupportedControl("exec");
      },
      async writeFile(_path, _contents) {
        unsupportedControl("writeFile");
      },
      async readFile(_path) {
        unsupportedControl("readFile");
      },
    };
    return freeze ? Object.freeze(api) : api;
  };
  const root = instance("default", false);
  return Object.freeze(Object.assign(root, {
    get(name) {
      return instance(name);
    },
  }));
}
function images(name, binding) {
  const provider = binding?.provider ?? { kind: "local" };
  const visible = publicProvider(provider) ?? Object.freeze({ kind: "local" });
  return Object.freeze({
    provider: visible,
    input(source) {
      const transforms = {};
      return Object.freeze({
        transform(options = {}) { Object.assign(transforms, options); return this; },
        async output() {
          if (provider.kind === "http") {
            const headers = { "content-type": "application/json" };
            if (provider.authorization) headers.authorization = provider.authorization;
            return await fetch(String(provider.url).replace(/\\/$/, "") + "/transform", {
              method: "POST",
              headers,
              body: JSON.stringify({ source, transforms }),
            });
          }
          const body = typeof source === "string" ? source : JSON.stringify(source);
          return new Response(body, { headers: { "content-type": "application/octet-stream" } });
        },
      });
    },
  });
}

function durable(name) {
  return {
    idFromName: (id) => {
      const raw = durableNameId(id);
      return { __perenNamespace: name, __perenId: raw, toString: () => name + ":" + raw };
    },
    idFromString: (id) => {
      const value = String(id);
      const prefix = name + ":";
      const raw = value.startsWith(prefix) ? value.slice(prefix.length) : value;
      return { __perenNamespace: name, __perenId: raw, toString: () => name + ":" + raw };
    },
    newUniqueId() {
      const raw = durableUniqueId();
      return { __perenNamespace: name, __perenId: raw, toString: () => name + ":" + raw };
    },
    get(id) {
      const target = { __perenNamespace: name, __perenId: id.__perenId };
      return {
        ...target,
        fetch(input, init) {
          const url = rewrite(input, requireBaseUrl());
          const rest = url.pathname.replace(/^\\//, "");
          url.pathname = \`/\${target.__perenId}\${rest ? \`/\${rest}\` : ""}\`;
          if (!plain(input) && input?.url) return fetch(new Request(url, input), init);
          return fetch(url, init);
        },
      };
    },
  };
}

const proxies = new Map();

export const env = new Proxy({}, {
  get(_target, prop) {
    if (typeof prop !== "string") return undefined;
    if (proxies.has(prop)) return proxies.get(prop);
    const binding = BINDINGS[prop];
    const kind = bindingKind(binding);
    let proxy;
    if (kind === "kv") proxy = kv(prop);
    else if (kind === "r2" || kind === "r2_bucket") proxy = r2(prop);
    else if (kind === "d1" || kind === "d1_database") proxy = d1(prop);
    else if (kind === "queue") proxy = queue(prop);
    else if (kind === "vectorize") proxy = vectorize(prop);
    else if (kind === "ai") proxy = ai(prop);
    else if (kind === "rate_limiter") proxy = rateLimiter(prop);
    else if (kind === "analytics_engine") proxy = analytics(prop);
    else if (kind === "service") proxy = service(prop);
    else if (kind === "dispatcher") proxy = dispatcher(prop, binding);
    else if (kind === "workflow") proxy = workflow(prop);
    else if (kind === "durable_object_namespace") proxy = durable(prop);
    else if (kind === "hyperdrive") proxy = hyperdrive(binding);
    else if (kind === "container") proxy = container(binding);
    else if (kind === "outbound") proxy = outbound(prop, binding);
    else if (kind === "loader") proxy = loader();
    else if (kind === "images") proxy = images(prop, binding);
    else if (kind === undefined) throw new Error(\`env.\${prop}: no binding on service \${SERVICE}\`);
    else throw new Error(\`env.\${prop}: unrecognized binding kind \${kind}\`);
    proxies.set(prop, proxy);
    return proxy;
  },
});

export function createExecutionContext() {
  const promises = [];
  return { waitUntil(promise) { promises.push(promise); }, passThroughOnException() {}, props: {}, __perenPromises: promises };
}

export async function waitOnExecutionContext(ctx) {
  await Promise.all(ctx.__perenPromises ?? []);
}

export async function runDurableObjectAlarm(stub) {
  const { namespace, id } = durableTarget(stub);
  const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(namespace)}/\${encodeURIComponent(id)}/alarm/run?service=\${encodeURIComponent(SERVICE)}\`, requireBaseUrl()), { method: "POST" });
  const body = await response.json();
  if (!response.ok) throw new Error(\`runDurableObjectAlarm: \${body.error ?? response.statusText}\`);
  return body.ran === true;
}

export async function evictDurableObject(stub) {
  const { namespace, id } = durableTarget(stub);
  const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(namespace)}/\${encodeURIComponent(id)}/evict?service=\${encodeURIComponent(SERVICE)}\`, requireBaseUrl()), { method: "POST" });
  const body = await response.json();
  if (!response.ok) throw new Error(\`evictDurableObject: \${body.error ?? response.statusText}\`);
  return body.evicted === true;
}

export async function listDurableObjectIds(namespace) {
  const name = typeof namespace === "string" ? namespace : namespace?.__perenNamespace;
  const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(name)}?service=\${encodeURIComponent(SERVICE)}\`, requireBaseUrl()));
  const body = await response.json();
  if (!response.ok) throw new Error(\`listDurableObjectIds: \${body.error ?? response.statusText}\`);
  return (body.cells ?? []).map((hex) => ({ toString: () => hex, valueOf: () => hex }));
}

export async function runInDurableObject(stub, callback) {
  const { namespace, id } = durableTarget(stub);
  const query = \`?service=\${encodeURIComponent(SERVICE)}\`;
  const storage = {
    id,
    async get(key) {
      const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(namespace)}/\${encodeURIComponent(id)}/storage/\${encodeURIComponent(key)}\${query}\`, requireBaseUrl()));
      const body = await response.json();
      if (!response.ok) throw new Error(\`state.storage.get: \${body.error ?? response.statusText}\`);
      return body.found ? body.value : undefined;
    },
    async list() {
      const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(namespace)}/\${encodeURIComponent(id)}/storage\${query}\`, requireBaseUrl()));
      const body = await response.json();
      if (!response.ok) throw new Error(\`state.storage.list: \${body.error ?? response.statusText}\`);
      return body;
    },
    async put(key, value) {
      const body = value instanceof Uint8Array ? value : String(value);
      const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(namespace)}/\${encodeURIComponent(id)}/storage/\${encodeURIComponent(key)}\${query}\`, requireBaseUrl()), {
        method: "PUT",
        body,
      });
      const result = await response.json();
      if (!response.ok) throw new Error(\`state.storage.put: \${result.error ?? response.statusText}\`);
      return result;
    },
    async delete(key) {
      const response = await fetch(new URL(\`/__test/do/\${encodeURIComponent(namespace)}/\${encodeURIComponent(id)}/storage/\${encodeURIComponent(key)}\${query}\`, requireBaseUrl()), { method: "DELETE" });
      const result = await response.json();
      if (!response.ok) throw new Error(\`state.storage.delete: \${result.error ?? response.statusText}\`);
      return result.deleted === true;
    },
  };
  const instance = new Proxy({}, { get(_target, prop) { throw new Error(\`direct Durable Object instance access is not available: \${String(prop)}\`); } });
  return callback(instance, storage);
}
`;
}
