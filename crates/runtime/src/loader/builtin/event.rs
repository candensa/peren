pub(super) const NODE_EVENTS_SOURCE: &str = r#"
export function once(emitter, type) {
  return new Promise((resolve, reject) => {
    const onEvent = (...args) => { emitter.removeListener?.("error", onError); resolve(args.length > 1 ? args : args[0]); };
    const onError = (error) => { emitter.removeListener(type, onEvent); reject(error); };
    emitter.once(type, onEvent);
    if (type !== "error") emitter.once?.("error", onError);
  });
}
export class EventEmitter {
  static defaultMaxListeners = 10;
  static listenerCount(emitter, type) { return emitter.listenerCount(type); }
  static once(emitter, type) { return once(emitter, type); }
  constructor() { this._events = new Map(); this._maxListeners = undefined; }
  setMaxListeners(value) { this._maxListeners = Number(value); return this; }
  getMaxListeners() { return this._maxListeners ?? EventEmitter.defaultMaxListeners; }
  _add(type, listener, prepend, once) {
    if (typeof listener !== "function") throw new TypeError('The "listener" argument must be of type Function');
    const wrapped = once ? (...args) => { this.removeListener(type, wrapped); listener.apply(this, args); } : listener;
    if (once) wrapped.listener = listener;
    this.emit("newListener", type, listener);
    const list = this._events.get(type) ?? [];
    prepend ? list.unshift(wrapped) : list.push(wrapped);
    this._events.set(type, list);
    return this;
  }
  on(type, listener) { return this._add(type, listener, false, false); }
  addListener(type, listener) { return this.on(type, listener); }
  prependListener(type, listener) { return this._add(type, listener, true, false); }
  once(type, listener) { return this._add(type, listener, false, true); }
  prependOnceListener(type, listener) { return this._add(type, listener, true, true); }
  removeListener(type, listener) {
    const list = this._events.get(type);
    if (!list) return this;
    const index = list.findIndex((item) => item === listener || item.listener === listener);
    if (index >= 0) {
      const [removed] = list.splice(index, 1);
      if (list.length === 0) this._events.delete(type);
      this.emit("removeListener", type, removed.listener ?? removed);
    }
    return this;
  }
  off(type, listener) { return this.removeListener(type, listener); }
  removeAllListeners(type) { type === undefined ? this._events.clear() : this._events.delete(type); return this; }
  listeners(type) { return (this._events.get(type) ?? []).map((listener) => listener.listener ?? listener); }
  rawListeners(type) { return [...(this._events.get(type) ?? [])]; }
  listenerCount(type) { return (this._events.get(type) ?? []).length; }
  eventNames() { return [...this._events.keys()]; }
  emit(type, ...args) {
    const list = this._events.get(type);
    if (!list || list.length === 0) {
      if (type === "error") throw (args[0] instanceof Error ? args[0] : new Error(`Unhandled error. (${String(args[0])})`));
      return false;
    }
    for (const listener of [...list]) listener.apply(this, args);
    return true;
  }
}
export default EventEmitter;
"#;
