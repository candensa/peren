function scopedStorage(scope) {
  return ObjectFreeze({
    get: (key, options = {}) => storage.get(key, { ...options, scope }),
    put: (key, value, options = {}) => storage.transaction((transaction) => transaction.put(key, value, { ...options, scope })),
    delete: (key, options = {}) => storage.delete(key, { ...options, scope }),
    deleteAll: () => storage.deleteAll({ scope }),
    transaction: (callback) => storage.transaction((transaction) => callback(ObjectFreeze({
      get: (key, options = {}) => transaction.get(key, { ...options, scope }),
      put: (key, value, options = {}) => transaction.put(key, value, { ...options, scope }),
      delete: (key, options = {}) => transaction.delete(key, { ...options, scope }),
      deleteAll: () => transaction.deleteAll({ scope }),
    }))),
    mutation: (id, callback) => storage.mutation(id, (transaction) => callback(ObjectFreeze({
      get: (key, options = {}) => transaction.get(key, { ...options, scope }),
      put: (key, value, options = {}) => transaction.put(key, value, { ...options, scope }),
      delete: (key, options = {}) => transaction.delete(key, { ...options, scope }),
      deleteAll: () => transaction.deleteAll({ scope }),
    }))),
  });
}

class DurableObjectFacetStub {
  constructor(name, instance, record) {
    this.name = name;
    this.instance = instance;
    this.record = record;
  }
  async fetch(input, init = {}) {
    if (this.record.aborted) throw new Error(`Durable Object facet ${this.name} has been aborted`);
    if (typeof this.instance.fetch !== "function") throw new TypeError(`Durable Object facet ${this.name} does not export fetch`);
    return await Promise.resolve(this.instance.fetch(new Request(input, init)));
  }
}

const durableObjectExports = (target) => rpcProxy(ObjectFreeze({}), async (method, args) => {
  const instance = typeof target === "function" ? target() : target;
  const member = instance?.[method];
  if (typeof member !== "function") throw new TypeError(`Durable Object does not export ${method}`);
  return await Promise.resolve(member.apply(instance, args));
});

class DurableObjectFacets {
  constructor(env) {
    this.env = env;
    this.records = new Map();
  }
  get(name, callback) {
    const key = String(name);
    let record = this.records.get(key);
    if (record === undefined) {
      if (typeof callback !== "function") throw new TypeError("ctx.facets.get requires a callback for a new facet");
      const scope = `facet:${doEncode(key)}`;
      const ctx = new DurableObjectState(scopedStorage(scope), this.env);
      const target = callback();
      const instance = typeof target === "function" ? new target(ctx, this.env) : target;
      if (instance == null || typeof instance !== "object") throw new TypeError("ctx.facets.get callback must return an object or class");
      record = { instance, aborted: false, scope };
      this.records.set(key, record);
    }
    record.aborted = false;
    const stub = new DurableObjectFacetStub(key, record.instance, record);
    return rpcProxy(stub, async (method, args) => {
      if (record.aborted) throw new Error(`Durable Object facet ${key} has been aborted`);
      const target = record.instance?.[method];
      if (typeof target !== "function") throw new TypeError(`Durable Object facet ${key} does not export ${method}`);
      return await Promise.resolve(target.apply(record.instance, args));
    });
  }
  abort(name) {
    const record = this.records.get(String(name));
    if (record === undefined) return false;
    record.aborted = true;
    return true;
  }
  async delete(name) {
    const key = String(name);
    const record = this.records.get(key);
    const scope = record?.scope ?? `facet:${doEncode(key)}`;
    this.records.delete(key);
    await scopedStorage(scope).deleteAll();
    return true;
  }
}

function durableValue(value) {
  if (value instanceof Uint8Array) {
    return JSON.stringify({ __perenDurable: 1, bytes: ArrayFrom(value) });
  }
  const encoded = JSON.stringify({ __perenDurable: 1, json: value });
  if (encoded === undefined) throw new TypeError("storage value must be JSON or Uint8Array");
  return encoded;
}

function readDurableValue(bytes) {
  if (bytes === undefined) return undefined;
  try {
    const parsed = JSON.parse(decodeText(bytes));
    if (parsed && parsed.__perenDurable === 1) {
      if (Array.isArray(parsed.bytes)) return new Uint8Array(parsed.bytes);
      return parsed.json;
    }
  } catch (_) {}
  return bytes;
}

function durableObjectStorage(inner) {
  const wrapTransaction = (callback) => inner.transaction(async (transaction) => {
    const scoped = durableObjectTransactionStorage(transaction);
    return await callback(scoped);
  });
  const wrapMutation = (id, callback) => inner.mutation(id, async (transaction) => {
    const scoped = durableObjectTransactionStorage(transaction);
    return await callback(scoped);
  });
  return ObjectFreeze({
    get: async (key, options = {}) => readDurableValue(await inner.get(key, options)),
    put: async (key, value, options = {}) => {
      await inner.transaction(async (txn) => {
        await txn.put(key, durableValue(value), options);
      });
    },
    delete: (key, options) => inner.delete(key, options),
    deleteAll: (options) => inner.deleteAll(options),
    transaction: (callback) => wrapTransaction(callback),
    mutation: (id, callback) => wrapMutation(id, callback),
  });
}

function durableObjectTransactionStorage(transaction) {
  return ObjectFreeze({
    get: async (key, options = {}) => readDurableValue(await transaction.get(key, options)),
    put: (key, value, options = {}) => transaction.put(key, durableValue(value), options),
    delete: (key, options) => transaction.delete(key, options),
    deleteAll: (options) => transaction.deleteAll(options),
  });
}

class DurableObjectState {
  constructor(storageBinding = storage, env = {}, options = {}) {
    this.storage = durableObjectStorage(storageBinding);
    this.facets = ObjectFreeze(new DurableObjectFacets(env));
    this.env = env;
    this.id = options.id ?? null;
    this.props = ObjectFreeze({ ...(options.props ?? {}) });
    this.exports = durableObjectExports(options.exports ?? {});
  }

  acceptWebSocket(socket, tags = []) {
    if (!(socket instanceof WebSocketPeer)) throw new TypeError("acceptWebSocket requires a WebSocket");
    socket.accept();
    const normalized = ArrayFrom(tags ?? [], String);
    durableWebSockets.set(socket, ObjectFreeze(normalized));
    persistDurableWebSocket(socket, normalized);
    return socket;
  }

  getWebSockets(tag = undefined) {
    const selected = [];
    for (const [socket, tags] of durableWebSockets.entries()) {
      if (socket.readyState === WebSocketPeer.CLOSED) {
        durableWebSockets.delete(socket);
        continue;
      }
      if (tag === undefined || tags.includes(String(tag))) selected.push(socket);
    }
    return ObjectFreeze(selected);
  }

  setWebSocketAutoResponse(pair) {
    if (pair != null && !(pair instanceof WebSocketRequestResponsePair)) {
      throw new TypeError("setWebSocketAutoResponse requires a WebSocketRequestResponsePair");
    }
    durableWebSocketAutoResponse = pair ?? null;
    durableWebSocketAutoResponseTimestamp = pair == null ? null : Date.now();
    persistDurableWebSocketAutoResponse();
  }

  getWebSocketAutoResponse() {
    return durableWebSocketAutoResponse;
  }

  getWebSocketAutoResponseTimestamp() {
    return durableWebSocketAutoResponseTimestamp;
  }
}

class DurableObject {
  constructor(ctx = new DurableObjectState(), env = {}) {
    this.ctx = ctx;
    this.env = env;
  }
}

class WorkerEntrypoint {
  constructor(ctx = {}, env = {}) {
    this.ctx = ctx;
    this.env = env;
  }
}

class RpcTarget {}

const rpcHeader = "x-peren-rpc-method";
const rpcContentType = "application/vnd.peren.rpc+json";

async function readRpcResponse(response) {
  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json") || contentType.includes(rpcContentType)
    ? await response.json()
    : { ok: response.ok, value: await response.text() };
  if (!response.ok || payload?.ok === false) {
    const message = payload?.error?.message ?? payload?.error ?? `RPC call failed with status ${response.status}`;
    throw new Error(String(message));
  }
  return payload?.value;
}

function rpcProxy(target, invoke) {
  return new Proxy(target, {
    get(receiver, property, value) {
      if (typeof property !== "string") return Reflect.get(receiver, property, value);
      if (property in receiver) {
        const member = Reflect.get(receiver, property, value);
        return typeof member === "function" ? member.bind(receiver) : member;
      }
      if (property === "then") return undefined;
      return async (...args) => await invoke(property, args);
    },
  });
}

function rpcRequest(method, args, url = "https://rpc.internal/") {
  return new Request(url, {
    method: "POST",
    headers: {
      [rpcHeader]: method,
      "content-type": rpcContentType,
    },
    body: JSON.stringify({ args }),
  });
}

class WebSocketRequestResponsePair {
  constructor(request, response) {
    this.request = String(request);
    this.response = String(response);
  }
}

let durableWebSocketAutoResponse = null;
let durableWebSocketAutoResponseTimestamp = null;
const durableWebSockets = new Map();
const durableWebSocketScope = "__peren:websocket";
const durableWebSocketAutoResponseKey = "__auto_response";

function persistDurableWebSocketAutoResponse() {
  const task = (async () => {
    try {
      await core.ops.op_storage_begin();
      let committed = false;
      try {
        if (durableWebSocketAutoResponse === null) {
          await core.ops.op_storage_delete(durableWebSocketScope, ArrayFrom(encodeStorageBytes(durableWebSocketAutoResponseKey)));
        } else {
          await core.ops.op_storage_put(
            durableWebSocketScope,
            ArrayFrom(encodeStorageBytes(durableWebSocketAutoResponseKey)),
            ArrayFrom(encodeStorageBytes(JSON.stringify({
              request: durableWebSocketAutoResponse.request,
              response: durableWebSocketAutoResponse.response,
              timestamp: durableWebSocketAutoResponseTimestamp,
            }))),
          );
        }
        await core.ops.op_storage_commit();
        committed = true;
      } finally {
        if (!committed) await core.ops.op_storage_rollback();
      }
    } catch {
      // Stateless runtimes keep auto-response metadata in isolate memory only.
    }
  })();
  if (typeof globalThis.__perenRegisterWaitUntil === "function") globalThis.__perenRegisterWaitUntil(task);
}

function persistDurableWebSocket(socket, tags) {
  const id = globalThis.__perenWebSocketAttachmentId(socket);
  if (id == null) return;
  const task = (async () => {
    try {
      await core.ops.op_storage_begin();
      let committed = false;
      try {
        await core.ops.op_storage_put(
          durableWebSocketScope,
          ArrayFrom(encodeStorageBytes(id)),
          ArrayFrom(encodeStorageBytes(JSON.stringify({ id, tags: ArrayFrom(tags, String) }))),
        );
        await core.ops.op_storage_commit();
        committed = true;
      } finally {
        if (!committed) await core.ops.op_storage_rollback();
      }
    } catch {
      // Stateless runtimes keep accepted sockets in isolate memory only.
    }
  })();
  if (typeof globalThis.__perenRegisterWaitUntil === "function") globalThis.__perenRegisterWaitUntil(task);
}

globalThis.__perenMaybeAutoRespondWebSocket = (socket, message) => {
  const pair = durableWebSocketAutoResponse;
  if (pair === null || String(message) !== pair.request) return false;
  socket.send(pair.response);
  durableWebSocketAutoResponseTimestamp = Date.now();
  persistDurableWebSocketAutoResponse();
  return true;
};

globalThis.__perenDeleteDurableWebSocket = async (id) => {
  const key = String(id);
  try {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_storage_delete(durableWebSocketScope, ArrayFrom(encodeStorageBytes(key)));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  } catch {
    // Stateless runtimes have no durable hibernation record to delete.
  }
  const socket = socketByAttachment.get(key);
  if (socket !== undefined) durableWebSockets.delete(socket);
};

globalThis.__perenHydrateDurableWebSockets = async () => {
  try {
    let cursor;
    do {
      const options = { limit: 1000 };
      if (cursor !== undefined) options.cursor = ArrayFrom(encodeStorageBytes(cursor));
      const page = await core.ops.op_storage_list(durableWebSocketScope, options);
      for (const key of page.keys) {
        const id = key.name;
        const bytes = await core.ops.op_storage_get(durableWebSocketScope, ArrayFrom(encodeStorageBytes(id)));
        if (bytes === null) continue;
        const record = JSON.parse(decodeText(new Uint8Array(bytes)));
        if (id === durableWebSocketAutoResponseKey) {
          durableWebSocketAutoResponse = new WebSocketRequestResponsePair(record.request, record.response);
          durableWebSocketAutoResponseTimestamp = record.timestamp ?? null;
          continue;
        }
        const socket = globalThis.__perenHostWebSocket(String(record.id ?? id));
        durableWebSockets.set(socket, ObjectFreeze(ArrayFrom(record.tags ?? [], String)));
      }
      cursor = page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor));
    } while (cursor !== undefined);
  } catch {
    // Stateless runtimes have no durable storage host; hibernated sockets hydrate when storage exists.
  }
};

class DurableObjectId {
  constructor(namespace, value, name = undefined) {
    this.namespace = namespace;
    this.value = value;
    this.name = name;
  }
  toString() {
    return `${this.namespace}:${this.value}`;
  }
}

class DurableObjectStub {
  constructor(id, className) {
    this.id = id;
    this.name = id.name;
    this.className = className;
  }
  async fetch(input, init = {}) {
    const request = new Request(input, init);
    const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
    const response = await core.ops.op_durable_object_fetch({
      namespace: this.id.namespace,
      id: this.id.value,
      name: this.id.name ?? null,
      className: this.className,
      request: {
        method: request.method,
        url: request.url,
        headers: ArrayFrom(request.headers.entries()),
        body,
      },
    });
    return hostResponse(response);
  }
}

class DispatchNamespace {}

class DurableObjectNamespace {
  constructor(binding, className) {
    this.binding = binding;
    this.className = className;
  }
  idFromName(name) {
    const value = String(name);
    return ObjectFreeze(new DurableObjectId(this.binding, `name:${doEncode(value)}`, value));
  }
  idFromString(id) {
    const value = String(id);
    const prefix = `${this.binding}:`;
    if (!value.startsWith(prefix)) throw new TypeError("Durable Object id belongs to a different namespace");
    const raw = value.slice(prefix.length);
    const name = raw.startsWith("name:") ? doDecode(raw.slice(5)) : undefined;
    return ObjectFreeze(new DurableObjectId(this.binding, raw, name));
  }
  newUniqueId() {
    return ObjectFreeze(new DurableObjectId(this.binding, `unique:${workflowId()}`));
  }
  get(id) {
    if (!(id instanceof DurableObjectId) || id.namespace !== this.binding) {
      throw new TypeError("Durable Object id belongs to a different namespace");
    }
    const stub = new DurableObjectStub(id, this.className);
    return rpcProxy(stub, async (method, args) => {
      const response = await stub.fetch(rpcRequest(method, args));
      return await readRpcResponse(response);
    });
  }
}

const durableObjectNamespace = (binding, className) => ObjectFreeze(new DurableObjectNamespace(binding, className));

const dispatchNamespace = (binding) => {
  const scripts = ArrayFrom(binding.scripts ?? [], String);
  const namespace = String(binding.namespace ?? "");
  return ObjectFreeze(Object.assign(ObjectCreate(DispatchNamespace.prototype), {
    namespace,
    scripts: ObjectFreeze(scripts),
    get(name) {
      const script = String(name);
      if (!scripts.includes(script)) throw new TypeError(`unknown dispatch script ${script}`);
      return serviceBinding(`${namespace}/${script}`);
    },
  }));
};
