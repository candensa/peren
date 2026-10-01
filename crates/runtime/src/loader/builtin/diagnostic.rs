pub(super) const NODE_DIAGNOSTICS_CHANNEL_SOURCE: &str = r#"
const channels = new Map();
const subscribers = new Set();
class Channel {
  constructor(name) { this.name = String(name); this._listeners = new Set(); }
  get hasSubscribers() { return this._listeners.size > 0 || subscribers.size > 0; }
  subscribe(listener) { if (typeof listener !== "function") throw new TypeError("listener must be a function"); this._listeners.add(listener); }
  unsubscribe(listener) { this._listeners.delete(listener); }
  bindStore(store) { return store; }
  unbindStore() {}
  runStores(context, callback, ...args) { return callback.apply(context, args); }
  publish(message) {
    for (const listener of this._listeners) listener(message, this.name);
    for (const listener of subscribers) listener(message, this.name);
  }
}
export function channel(name) {
  const key = String(name);
  let value = channels.get(key);
  if (value === undefined) { value = new Channel(key); channels.set(key, value); }
  return value;
}
export function hasSubscribers(name) { return channel(name).hasSubscribers; }
export function subscribe(name, listener) { channel(name).subscribe(listener); }
export function unsubscribe(name, listener) { channel(name).unsubscribe(listener); }
export function tracingChannel(nameOrChannels) {
  const base = typeof nameOrChannels === "string" ? nameOrChannels : "trace";
  return Object.freeze({
    start: channel(`${base}:start`),
    end: channel(`${base}:end`),
    asyncStart: channel(`${base}:asyncStart`),
    asyncEnd: channel(`${base}:asyncEnd`),
    error: channel(`${base}:error`),
  });
}
export function subscribeAll(listener) { if (typeof listener !== "function") throw new TypeError("listener must be a function"); subscribers.add(listener); }
export function unsubscribeAll(listener) { subscribers.delete(listener); }
export default { channel, hasSubscribers, subscribe, unsubscribe, tracingChannel, subscribeAll, unsubscribeAll, Channel };
"#;
