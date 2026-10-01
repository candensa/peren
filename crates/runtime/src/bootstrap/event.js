globalThis.__perenQueueDispositions = [];
const queueDisposition = (id, outcome, options = {}) => ({
  id: String(id),
  outcome,
  delaySeconds: options?.delaySeconds == null ? null : Number(options.delaySeconds),
});
globalThis.__perenPrepareQueue = (event) => {
  globalThis.__perenQueueDispositions = [];
  const messages = event.messages.map((message) => {
    const id = String(message.id);
    return Object.freeze({
      ...message,
      ack() {
        globalThis.__perenQueueDispositions.push(queueDisposition(id, "ack"));
      },
      retry(options = {}) {
        globalThis.__perenQueueDispositions.push(queueDisposition(id, "retry", options));
      },
    });
  });
  return Object.freeze({
    ...event,
    metrics: Object.freeze(event.metrics ?? { ready: 0, delayed: 0, leased: messages.length, oldestReadyTimestamp: null }),
    messages,
    ackAll() {
      for (const message of messages) globalThis.__perenQueueDispositions.push(queueDisposition(message.id, "ack"));
    },
    retryAll(options = {}) {
      for (const message of messages) globalThis.__perenQueueDispositions.push(queueDisposition(message.id, "retry", options));
    },
  });
};

globalThis.__perenDispatchListener = dispatchListener;

globalThis.__perenWaitUntil = [];
globalThis.__perenResetWaitUntil = () => {
  globalThis.__perenWaitUntil = [];
};
globalThis.__perenRegisterWaitUntil = (promise) => {
  const tracked = PromiseResolve(promise);
  globalThis.__perenWaitUntil.push(tracked);
  return tracked;
};
globalThis.__perenDrainWaitUntil = async () => {
  while (globalThis.__perenWaitUntil.length > 0) {
    const pending = globalThis.__perenWaitUntil;
    globalThis.__perenWaitUntil = [];
    await PromiseAll(pending);
  }
};

globalThis.__perenCreateFetchEvent = (request) => {
  let responsePromise;
  return {
    request,
    respondWith(response) {
      responsePromise = PromiseResolve(response);
    },
    waitUntil(promise) {
      return globalThis.__perenRegisterWaitUntil(promise);
    },
    async __perenResponse() {
      return responsePromise;
    },
  };
};

globalThis.__perenCreateExtendableEvent = (name, fields) => ({
  type: name,
  ...fields,
  waitUntil(promise) {
    return globalThis.__perenRegisterWaitUntil(promise);
  },
});

const workflowStepKey = (instance, sequence) => `${String(instance)}/steps/${sequence}.json`;

globalThis.__perenCreateWorkflowStep = (event) => {
  let sequence = 0;
  const read = async (name, kind) => {
    const stepName = String(name);
    const key = workflowStepKey(event.instance, sequence);
    const existing = await storage.get(key, { scope: "workflowSteps" });
    if (existing === undefined) return { stepName, key };
    const entry = JSON.parse(decodeText(existing));
    const entryKind = entry.kind ?? "step";
    if (entry.name !== stepName || entryKind !== kind) {
      throw new Error(`workflow replay mismatch at step ${sequence}: journal has ${entry.name}, code called ${stepName}`);
    }
    sequence += 1;
    return { entry };
  };
  return ObjectFreeze({
    async do(name, optionsOrCallback, maybeCallback) {
      const callback = typeof optionsOrCallback === "function" ? optionsOrCallback : maybeCallback;
      if (typeof callback !== "function") throw new TypeError("workflow step requires a callback");
      const loaded = await read(name, "step");
      if (loaded.entry !== undefined) return loaded.entry.value;
      const value = await callback();
      await storage.transaction(async (txn) => {
        await txn.put(loaded.key, JSON.stringify({ name: loaded.stepName, kind: "step", value }), { scope: "workflowSteps" });
      });
      sequence += 1;
      return value;
    },
    async sleep(name, delayMs = 0) {
      const loaded = await read(name, "sleep");
      if (loaded.entry !== undefined) return loaded.entry.value;
      const delay = Math.max(0, Number(delayMs) || 0);
      const value = { dueAt: Date.now() + delay, delayMs: delay };
      if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
      await storage.transaction(async (txn) => {
        await txn.put(loaded.key, JSON.stringify({ name: loaded.stepName, kind: "sleep", value }), { scope: "workflowSteps" });
      });
      sequence += 1;
      return value;
    },
  });
};


const cacheRequest = (request) => request instanceof Request ? request : new Request(String(request));
const cacheKey = (request, options = {}) => {
  const req = cacheRequest(request);
  const method = options.ignoreMethod === true ? "GET" : req.method.toUpperCase();
  const url = new URL(req.url);
  if (options.ignoreSearch === true) url.search = "";
  return `${method} ${url.href}`;
};
