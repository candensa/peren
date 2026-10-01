class Workflow {
  constructor(binding, id) {
    this.binding = binding;
    this.id = id;
  }
  async status() {
    return await workflowState(this.binding, this.id) ?? { id: this.id, status: "unknown" };
  }
  async terminate(reason) {
    return await writeWorkflowState(this.binding, this.id, "terminated", reason);
  }
  async restart() {
    return await writeWorkflowState(this.binding, this.id, "running", undefined);
  }
}

const workflowKey = (binding, id) => `${binding}/${id}.json`;
const workflowState = async (binding, id) => {
  const bytes = await storage.get(workflowKey(binding, id), { scope: "workflows" });
  return bytes === undefined ? undefined : JSON.parse(decodeText(bytes));
};
const writeWorkflowState = async (binding, id, status, reason) => {
  const state = { id, status, reason: reason == null ? undefined : String(reason), updatedAt: Date.now() };
  await storage.transaction(async (txn) => {
    await txn.put(workflowKey(binding, id), JSON.stringify(state), { scope: "workflows" });
  });
  return state;
};
const workflowId = () => `workflow-${Date.now()}-${Math.random().toString(16).slice(2)}`;

const workflowBinding = (binding, provider = ObjectFreeze({ kind: "native" })) => ObjectFreeze({
  provider,
  async create(options = {}) {
    const id = options.id == null ? workflowId() : String(options.id);
    await writeWorkflowState(binding, id, "running", undefined);
    return new Workflow(binding, id);
  },
  get(id) {
    return new Workflow(binding, String(id));
  },
});

globalThis.Workflow = Workflow;

const QUEUE_MAX_MESSAGE_BYTES = 128000;
const QUEUE_MAX_BATCH_MESSAGES = 100;
const QUEUE_MAX_BATCH_BYTES = 256000;
const QUEUE_MAX_DELAY_SECONDS = 86400;

const queueBody = (body) => {
  if (body instanceof Uint8Array) return ArrayFrom(body);
  if (body instanceof ArrayBuffer) return ArrayFrom(new Uint8Array(body));
  if (typeof body === "string") return ArrayFrom(encodeStorageBytes(body));
  return ArrayFrom(encodeStorageBytes(JSON.stringify(body)));
};

const queueDelay = (value) => {
  if (value == null) return null;
  const delay = Number(value);
  if (!Number.isFinite(delay) || delay < 0 || delay > QUEUE_MAX_DELAY_SECONDS) {
    throw new RangeError(`Queue delaySeconds must be between 0 and ${QUEUE_MAX_DELAY_SECONDS}`);
  }
  return delay;
};

const queueMessage = (queue, body, options = {}) => {
  const bytes = queueBody(body);
  if (bytes.length > QUEUE_MAX_MESSAGE_BYTES) {
    throw new RangeError(`Queue message body exceeds ${QUEUE_MAX_MESSAGE_BYTES} bytes`);
  }
  return {
    queue,
    body: bytes,
    contentType: options.contentType == null ? null : String(options.contentType),
    delaySeconds: queueDelay(options.delaySeconds),
    dedupId: options.dedupId == null ? null : String(options.dedupId),
  };
};

class Queue {
  constructor(queue, provider = ObjectFreeze({ kind: "memory" })) {
    this.queue = String(queue);
    this.provider = provider;
  }
  async send(body, options = {}) {
    await core.ops.op_queue_send(queueMessage(this.queue, body, options));
  }
  async sendBatch(messages) {
    const batch = ArrayFrom(messages ?? [], (message) => queueMessage(this.queue, message.body, message));
    if (batch.length > QUEUE_MAX_BATCH_MESSAGES) {
      throw new RangeError(`Queue sendBatch accepts at most ${QUEUE_MAX_BATCH_MESSAGES} messages`);
    }
    const bytes = batch.reduce((sum, message) => sum + message.body.length, 0);
    if (bytes > QUEUE_MAX_BATCH_BYTES) {
      throw new RangeError(`Queue sendBatch body total exceeds ${QUEUE_MAX_BATCH_BYTES} bytes`);
    }
    for (const message of batch) {
      await core.ops.op_queue_send(message);
    }
  }
}

globalThis.Queue = Queue;

const queueProducer = (queue, provider = ObjectFreeze({ kind: "memory" })) => ObjectFreeze(new Queue(queue, provider));



