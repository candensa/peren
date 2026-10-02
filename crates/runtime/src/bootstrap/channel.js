const portState = new WeakMap();
const socketState = new WeakMap();
const socketByAttachment = new Map();
let nextSocketAttachmentId = 1;

const newPortState = () => ({
  other: null,
  onmessage: null,
  listeners: new Set(),
  started: false,
  closed: false,
  detached: false,
  pending: [],
});

const port = (instance) => portState.get(instance);

const detachedPortState = () => ({
  other: null,
  onmessage: null,
  listeners: new Set(),
  started: false,
  closed: true,
  detached: true,
  pending: [],
});

class MessagePort {
  constructor() {
    portState.set(this, newPortState());
  }

  static pair() {
    const left = new MessagePort();
    const right = new MessagePort();
    port(left).other = right;
    port(right).other = left;
    return [left, right];
  }

  postMessage(data, transfer) {
    const state = port(this);
    if (state.closed || state.other === null) return;
    const ports = transferPorts(transfer);
    const cloned = transfer == null ? structuredClone(data) : structuredClone(data, { transfer: nonPortTransfers(transfer) });
    receivePortMessage(state.other, cloned, ports);
  }

  start() {
    const state = port(this);
    if (state.started || state.closed) return;
    state.started = true;
    const pending = state.pending;
    state.pending = [];
    for (const message of pending) deliverPortMessage(this, message.data, message.ports);
  }

  close() {
    const state = port(this);
    if (state.closed) return;
    state.closed = true;
    state.pending = [];
    const other = state.other;
    state.other = null;
    if (other !== null) port(other).other = null;
  }

  get onmessage() {
    return port(this).onmessage;
  }

  set onmessage(listener) {
    const state = port(this);
    state.onmessage = typeof listener === "function" ? listener : null;
    if (state.onmessage !== null) this.start();
  }

  addEventListener(type, listener) {
    if (type !== "message" || typeof listener !== "function") return;
    port(this).listeners.add(listener);
    this.start();
  }

  removeEventListener(type, listener) {
    if (type === "message") port(this).listeners.delete(listener);
  }
}

const nonPortTransfers = (transfer) => transfer == null
  ? undefined
  : ArrayFrom(transfer).filter((item) => !(item instanceof MessagePort));

const transferPorts = (transfer) => {
  if (transfer == null) return [];
  return ArrayFrom(transfer).filter((item) => item instanceof MessagePort).map((item) => {
    const state = port(item);
    if (state == null || state.detached) throw new DOMException("MessagePort is already detached", "DataCloneError");
    const moved = new MessagePort();
    portState.set(moved, state);
    portState.set(item, detachedPortState());
    if (state.other !== null) port(state.other).other = moved;
    return moved;
  });
};

const receivePortMessage = (target, data, ports = []) => {
  const state = port(target);
  if (state.closed) return;
  if (!state.started) {
    state.pending.push({ data, ports });
    return;
  }
  deliverPortMessage(target, data, ports);
};

const deliverPortMessage = (target, data, ports = []) => {
  const state = port(target);
  const event = ObjectFreeze({ data, ports: ObjectFreeze(ports), target });
  const listener = state.onmessage;
  const listeners = ArrayFrom(state.listeners);
  PromiseResolve().then(() => {
    if (port(target).closed) return;
    if (typeof listener === "function") listener.call(target, event);
    for (const item of listeners) item.call(target, event);
  });
};

class MessageChannel {
  constructor() {
    const [port1, port2] = MessagePort.pair();
    this.port1 = port1;
    this.port2 = port2;
  }
}

class WebSocketMessageEvent extends event.Event {
  constructor(data) {
    super("message");
    this.data = data;
  }
}

class WebSocketCloseEvent extends event.Event {
  constructor(code, reason, wasClean) {
    super("close");
    this.code = code;
    this.reason = reason;
    this.wasClean = wasClean;
  }
}

const newSocketState = () => ({
  accepted: false,
  attachmentId: `socket-${nextSocketAttachmentId++}`,
  closeQueued: false,
  other: null,
  readyState: WebSocketPeer.CONNECTING,
  pending: [],
  outbound: [],
  host: false,
  onmessage: null,
  onclose: null,
});

const socket = (instance) => socketState.get(instance);

class WebSocketPeer extends event.EventTarget {
  constructor() {
    super();
    socketState.set(this, newSocketState());
  }

  pair(other) {
    socket(this).other = other;
  }

  get readyState() {
    return socket(this).readyState;
  }

  get onmessage() {
    return socket(this).onmessage;
  }

  set onmessage(listener) {
    socket(this).onmessage = typeof listener === "function" ? listener : null;
  }

  get onclose() {
    return socket(this).onclose;
  }

  set onclose(listener) {
    socket(this).onclose = typeof listener === "function" ? listener : null;
  }

  accept() {
    const state = socket(this);
    if (state.readyState === WebSocketPeer.CLOSED) throw new Error("WebSocket is closed");
    if (state.accepted) return;
    state.accepted = true;
    state.readyState = WebSocketPeer.OPEN;
    socketByAttachment.set(state.attachmentId, this);
    const pending = state.pending;
    state.pending = [];
    for (const event of pending) emitSocketEvent(this, event);
  }

  send(data) {
    const state = socket(this);
    if (state.readyState !== WebSocketPeer.OPEN) throw new Error("WebSocket is not open");
    const other = state.other;
    if (other === null) {
      if (!state.host) throw new Error("peer WebSocket is closed");
      state.outbound.push(String(data));
      return;
    }
    if (socket(other).readyState === WebSocketPeer.CLOSED) throw new Error("peer WebSocket is closed");
    receiveSocketEvent(other, new WebSocketMessageEvent(data));
  }

  async serializeAttachment(value) {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) throw new TypeError("WebSocket attachment must be JSON-serializable");
    const bytes = encodeStorageBytes(encoded);
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_ws_attachment_set(socket(this).attachmentId, ArrayFrom(bytes));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  }

  async deserializeAttachment() {
    const bytes = await core.ops.op_ws_attachment_get(socket(this).attachmentId);
    if (bytes === null) return undefined;
    return JSON.parse(decodeText(new Uint8Array(bytes)));
  }

  async deleteAttachment() {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_ws_attachment_delete(socket(this).attachmentId);
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  }

  close(code = 1000, reason = "") {
    const state = socket(this);
    if (state.readyState === WebSocketPeer.CLOSING || state.readyState === WebSocketPeer.CLOSED) return;
    state.readyState = WebSocketPeer.CLOSING;
    queueSocketClose(this, code, String(reason), true);
    const other = state.other;
    if (other !== null && socket(other).readyState !== WebSocketPeer.CLOSING && socket(other).readyState !== WebSocketPeer.CLOSED) {
      socket(other).readyState = WebSocketPeer.CLOSING;
      queueSocketClose(other, code, String(reason), true);
    }
  }
}

WebSocketPeer.CONNECTING = 0;
WebSocketPeer.OPEN = 1;
WebSocketPeer.CLOSING = 2;
WebSocketPeer.CLOSED = 3;

const receiveSocketEvent = (target, event) => {
  const state = socket(target);
  if (state.readyState === WebSocketPeer.CLOSING || state.readyState === WebSocketPeer.CLOSED) return;
  if (!state.accepted) {
    state.pending.push(event);
    return;
  }
  emitSocketEvent(target, event);
};

const queueSocketClose = (target, code, reason, wasClean) => {
  const state = socket(target);
  if (state.closeQueued) return;
  state.closeQueued = true;
  state.pending = [];
  emitSocketEvent(target, new WebSocketCloseEvent(code, reason, wasClean), () => {
    state.readyState = WebSocketPeer.CLOSED;
    socketByAttachment.delete(state.attachmentId);
  });
};

const finishSocketClose = (target, code, reason, wasClean) => {
  const state = socket(target);
  if (state.readyState === WebSocketPeer.CLOSED) return;
  state.readyState = WebSocketPeer.CLOSED;
  state.pending = [];
  socketByAttachment.delete(state.attachmentId);
  emitSocketEvent(target, new WebSocketCloseEvent(code, reason, wasClean));
};

const emitSocketEvent = (target, event, after = undefined) => {
  PromiseResolve().then(() => {
    const state = socket(target);
    try {
      if (event.type === "message" && state.onmessage !== null) state.onmessage.call(target, event);
      if (event.type === "close" && state.onclose !== null) state.onclose.call(target, event);
      target.dispatchEvent(event);
    } finally {
      if (typeof after === "function") after();
    }
  });
};

globalThis.__perenWebSocketAttachmentId = (instance) => {
  const state = socketState.get(instance);
  if (state?.other !== null && state?.other !== undefined && durableWebSockets.has(state.other)) {
    return socket(state.other).attachmentId;
  }
  return state?.attachmentId ?? null;
};

globalThis.__perenHostWebSocket = (id) => {
  const key = String(id);
  const existing = socketByAttachment.get(key);
  if (existing !== undefined) return existing;
  const peer = new WebSocketPeer();
  const state = socket(peer);
  state.accepted = true;
  state.attachmentId = key;
  state.host = true;
  state.readyState = WebSocketPeer.OPEN;
  socketByAttachment.set(key, peer);
  return peer;
};

globalThis.__perenDrainWebSocketOutbound = (instance) => {
  const state = socket(instance);
  const outbound = state.outbound;
  state.outbound = [];
  return outbound;
};

globalThis.__perenReleaseWebSocket = (id, code = 1000, reason = "") => {
  const peer = socketByAttachment.get(String(id));
  if (peer === undefined) return false;
  finishSocketClose(peer, code, String(reason), true);
  return true;
};

class WebSocketPair {
  constructor() {
    const left = new WebSocketPeer();
    const right = new WebSocketPeer();
    left.pair(right);
    right.pair(left);
    this[0] = left;
    this[1] = right;
  }
}
