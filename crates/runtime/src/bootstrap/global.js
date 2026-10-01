class NonRetryableError extends Error {
  constructor(message = "non-retryable queue failure") {
    super(message);
    this.name = "NonRetryableError";
  }
}

performanceApi.setTimeOrigin();

ObjectDefineProperties(globalThis, {
  addEventListener: core.propNonEnumerable((type, listener) => {
    if (listener != null) listenerSet(type).add(listener);
  }),
  AbortController: core.propNonEnumerable(abort.AbortController),
  AbortSignal: core.propNonEnumerable(abort.AbortSignal),
  atob: core.propNonEnumerable(base64.atob),
  btoa: core.propNonEnumerable(base64.btoa),
  Blob: core.propNonEnumerable(file.Blob),
  BroadcastChannel: core.propNonEnumerable(broadcast.BroadcastChannel),
  ByteLengthQueuingStrategy: core.propNonEnumerable(streams.ByteLengthQueuingStrategy),
  console: core.propNonEnumerable(consoleCompat),
  crypto: core.propNonEnumerable(cryptoCompat),
  CryptoKey: core.propNonEnumerable(CryptoKey),
  DOMException: core.propNonEnumerable(exception.DOMException),
  Event: core.propNonEnumerable(event.Event),
  EventTarget: core.propNonEnumerable(event.EventTarget),
  File: core.propNonEnumerable(file.File),
  FileReader: core.propNonEnumerable(filereader.FileReader),
  fetch: core.propNonEnumerable(outboundFetch),
  FormData: core.propNonEnumerable(formdata.FormData),
  clearImmediate: core.propNonEnumerable(clearImmediateCompat),
  clearInterval: core.propNonEnumerable(timers.clearInterval),
  clearTimeout: core.propNonEnumerable(timers.clearTimeout),
  CompressionStream: core.propNonEnumerable(compression.CompressionStream),
  CountQueuingStrategy: core.propNonEnumerable(streams.CountQueuingStrategy),
  DecompressionStream: core.propNonEnumerable(compression.DecompressionStream),
  Headers: core.propNonEnumerable(headers.Headers),
  MessageChannel: core.propNonEnumerable(MessageChannel),
  MessageEvent: core.propNonEnumerable(event.MessageEvent),
  MessagePort: core.propNonEnumerable(MessagePort),
  navigator: core.propNonEnumerable(navigatorCompat),
  NonRetryableError: core.propNonEnumerable(NonRetryableError),
  performance: core.propNonEnumerable(performanceApi.performance),
  ProgressEvent: core.propNonEnumerable(event.ProgressEvent),
  Performance: core.propNonEnumerable(performanceApi.Performance),
  ReadableStream: core.propNonEnumerable(streams.ReadableStream),
  Request: core.propNonEnumerable(request.Request),
  Response: core.propNonEnumerable(response.Response),
  setImmediate: core.propNonEnumerable(setImmediateCompat),
  setInterval: core.propNonEnumerable(timers.setInterval),
  setTimeout: core.propNonEnumerable(timers.setTimeout),
  structuredClone: core.propNonEnumerable(clone.structuredClone),
  TextDecoder: core.propNonEnumerable(encoding.TextDecoder),
  TextDecoderStream: core.propNonEnumerable(encoding.TextDecoderStream),
  TextEncoder: core.propNonEnumerable(encoding.TextEncoder),
  TextEncoderStream: core.propNonEnumerable(encoding.TextEncoderStream),
  TransformStream: core.propNonEnumerable(streams.TransformStream),
  URL: core.propNonEnumerable(url.URL),
  URLPattern: core.propNonEnumerable(urlPattern.URLPattern),
  URLSearchParams: core.propNonEnumerable(url.URLSearchParams),
  WebSocket: core.propNonEnumerable(WebSocketPeer),
  WebSocketPair: core.propNonEnumerable(WebSocketPair),
  WritableStream: core.propNonEnumerable(streams.WritableStream),
});

const encodeStorageBytes = (value) => {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (typeof value === "string") {
    return new TextEncoder().encode(value);
  }
  throw new TypeError("storage values must be strings or Uint8Array instances");
};

const decodeStorageBytes = (value) => value === null ? undefined : new Uint8Array(value);

function hostResponse(response) {
  const status = Number(response.status);
  const body = status === 101 || status === 204 || status === 205 || status === 304
    ? null
    : new Uint8Array(response.body);
  return new Response(body, {
    status,
    headers: response.headers,
  });
}

async function outboundFetch(input, init = {}) {
  const request = new Request(input, init);
  const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
  const mtls = init?.cf?.mtlsCertificate?.__perenMtlsBinding;
  const response = await core.ops.op_outbound_fetch({
    method: request.method,
    url: request.url,
    headers: ArrayFrom(request.headers.entries()),
    body,
    mtls: typeof mtls === "string" ? mtls : null,
  });
  return hostResponse(response);
}


const deleteAllStorage = async (scope) => {
  let cursor = undefined;
  let deleted = 0;
  do {
    const options = { limit: 1000 };
    if (cursor !== undefined) options.cursor = ArrayFrom(encodeStorageBytes(cursor));
    const page = await core.ops.op_storage_list(scope, options);
    for (const entry of page.keys) {
      await core.ops.op_storage_delete(scope, ArrayFrom(encodeStorageBytes(entry.name)));
      deleted += 1;
    }
    cursor = page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor));
  } while (cursor !== undefined);
  return deleted;
};


const storageTransaction = () => ObjectFreeze({
  get: async (key, options = {}) => {
    const scope = options.scope ?? "do";
    return decodeStorageBytes(await core.ops.op_storage_get(scope, ArrayFrom(encodeStorageBytes(String(key)))));
  },
  put: (key, value, options = {}) => {
    const scope = options.scope ?? "do";
    return core.ops.op_storage_put(
      scope,
      ArrayFrom(encodeStorageBytes(String(key))),
      ArrayFrom(encodeStorageBytes(value)),
    );
  },
  delete: (key, options = {}) => {
    const scope = options.scope ?? "do";
    return core.ops.op_storage_delete(scope, ArrayFrom(encodeStorageBytes(String(key))));
  },
  deleteAll: (options = {}) => deleteAllStorage(options.scope ?? "do"),
});

const storage = {
  async get(key, options = {}) {
    const scope = options.scope ?? "do";
    return decodeStorageBytes(await core.ops.op_storage_get(scope, Array.from(encodeStorageBytes(String(key)))));
  },
  async delete(key, options = {}) {
    const scope = options.scope ?? "do";
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_storage_delete(scope, Array.from(encodeStorageBytes(String(key))));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  },
  async deleteAll(options = {}) {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const deleted = await deleteAllStorage(options.scope ?? "do");
      await core.ops.op_storage_commit();
      committed = true;
      return deleted;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  },
  async transaction(callback) {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const transaction = storageTransaction();
      const result = await callback(transaction);
      await core.ops.op_storage_commit();
      committed = true;
      return result;
    } finally {
      if (!committed) {
        await core.ops.op_storage_rollback();
      }
    }
  },
  async mutation(id, callback) {
    const mutationId = String(id);
    const existing = await core.ops.op_storage_mutation_outcome_get(mutationId);
    if (existing !== null) return JSON.parse(decodeText(new Uint8Array(existing)));
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const result = await callback(storageTransaction());
      await core.ops.op_storage_mutation_outcome_record(
        mutationId,
        ArrayFrom(encodeStorageBytes(JSON.stringify(result))),
      );
      await core.ops.op_storage_commit();
      committed = true;
      return result;
    } finally {
      if (!committed) {
        await core.ops.op_storage_rollback();
      }
    }
  },
};


const decodeText = (value) => new TextDecoder().decode(value);

const kvValue = (value) => Object.freeze({
  arrayBuffer: async () => value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
  json: async () => JSON.parse(decodeText(value)),
  text: async () => decodeText(value),
});

