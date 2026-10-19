const analyticsDatasets = new Map();
function analyticsEngineDataset(dataset, provider = ObjectFreeze({ kind: "buffer", dataset: String(dataset ?? "default") })) {
  const name = String(dataset ?? "default");
  if (!analyticsDatasets.has(name)) analyticsDatasets.set(name, []);
  return ObjectFreeze(Object.assign(ObjectCreate(AnalyticsEngineDataset.prototype), {
    provider,
    writeDataPoint(point = {}) {
      const rows = analyticsDatasets.get(name);
      rows.push({
        blobs: ArrayFrom(point.blobs ?? []),
        doubles: ArrayFrom(point.doubles ?? []),
        indexes: ArrayFrom(point.indexes ?? []),
        timestamp: Date.now(),
      });
      if (rows.length > 1024) rows.splice(0, rows.length - 1024);
    },
  }));
}

const redactConnectionString = (value) => {
  const text = String(value ?? "");
  try {
    const url = new URL(text);
    url.username = url.username.length === 0 ? "" : "redacted";
    url.password = url.password.length === 0 ? "" : "redacted";
    return url.toString();
  } catch {
    return text.replace(/:\/\/([^:@/]+):([^@/]+)@/, "://redacted:redacted@");
  }
};


function outboundBinding(binding) {
  const allowedHosts = ObjectFreeze(ArrayFrom(binding.allowedHosts ?? [], String));
  const allowed = new Set(allowedHosts);
  return ObjectFreeze({
    allowedHosts,
    async fetch(input, init = {}) {
      const request = input instanceof Request ? input : new Request(input, init);
      const host = new URL(request.url).host;
      if (!allowed.has(host)) throw new TypeError(`outbound host ${host} is not allowed by this binding`);
      return await outboundFetch(request, init);
    },
  });
}

function awsSigv4Binding(name, binding) {
  const allowedHosts = ObjectFreeze(ArrayFrom(binding.allowedHosts ?? [], String));
  const allowed = new Set(allowedHosts);
  const region = String(binding.region ?? "");
  const service = String(binding.service ?? "");
  return ObjectFreeze({
    allowedHosts,
    region,
    service,
    async fetch(input, init = {}) {
      const request = input instanceof Request ? input : new Request(input, init);
      const host = new URL(request.url).host;
      if (!allowed.has(host)) throw new TypeError(`AWS SigV4 host ${host} is not allowed by this binding`);
      const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
      const response = await core.ops.op_aws_sigv4_fetch({
        binding: name,
        region,
        service,
        allowedHosts,
        request: {
          method: request.method,
          url: request.url,
          headers: ArrayFrom(request.headers.entries()),
          body,
          mtls: null,
        },
      });
      return hostResponse(response);
    },
  });
}

function hyperdrive(binding) {
  const connectionString = redactConnectionString(binding.connectionString ?? "");
  const value = ObjectCreate(Hyperdrive.prototype);
  ObjectDefineProperties(value, {
    connectionString: { value: connectionString, enumerable: true },
    host: { value: connectionString, enumerable: true },
    provider: { value: ObjectFreeze({ kind: "pgcat" }), enumerable: true },
    cachingDisabled: { value: Boolean(binding.cachingDisabled), enumerable: true },
    maxAge: { value: Number(binding.maxAge ?? 0), enumerable: true },
    staleWhileRevalidate: { value: Number(binding.staleWhileRevalidate ?? 0), enumerable: true },
    poolMaxConnections: { value: Number(binding.poolMaxConnections ?? 1), enumerable: true },
  });
  return ObjectFreeze(value);
}

const rateLimiterWindows = new Map();
function rateLimiter(name, limit, periodSecs, provider = ObjectFreeze({ kind: "memory" })) {
  const capacity = Math.max(1, Number(limit) || 1);
  const periodMs = Math.max(1, Number(periodSecs) || 1) * 1000;
  return ObjectFreeze(Object.assign(ObjectCreate(RateLimiter.prototype), {
    provider,
    async limit(options = {}) {
      const key = String(options.key ?? "default");
      const now = Date.now();
      const id = `${name}:${key}`;
      let window = rateLimiterWindows.get(id);
      if (window == null || now >= window.reset) {
        window = { count: 0, reset: now + periodMs };
      }
      window.count += 1;
      rateLimiterWindows.set(id, window);
      const remaining = Math.max(0, capacity - window.count);
      return ObjectFreeze({
        success: window.count <= capacity,
        limit: capacity,
        remaining,
        reset: Math.ceil(window.reset / 1000),
      });
    },
  }));
}

const serviceBinding = (service) => {
  const binding = {
    async fetch(input, init = {}) {
      const request = new Request(input, init);
      const body = await requestBodyBytes(request);
      const response = await core.ops.op_service_fetch({
        service,
        request: {
          method: request.method,
          url: request.url,
          headers: ArrayFrom(request.headers.entries()),
          body,
        },
      });
      return hostResponse(response);
    },
  };
  return rpcProxy(binding, async (method, args) => {
    const response = await binding.fetch(rpcRequest(method, args));
    return await readRpcResponse(response);
  });
};

globalThis.DurableObject = DurableObject;
globalThis.DurableObjectNamespace = DurableObjectNamespace;
globalThis.DurableObjectState = DurableObjectState;
globalThis.RpcTarget = RpcTarget;
globalThis.WebSocketRequestResponsePair = WebSocketRequestResponsePair;
globalThis.WorkerEntrypoint = WorkerEntrypoint;
