pub(super) const NODE_ASYNC_HOOKS_SOURCE: &str = r#"
let nextId = 1;
const stack = [];
export class AsyncLocalStorage {
  run(store, callback, ...args) {
    if (typeof callback !== "function") throw new TypeError("callback must be a function");
    stack.push(store);
    try { return callback(...args); } finally { stack.pop(); }
  }
  enterWith(store) { stack.push(store); }
  getStore() { return stack.length === 0 ? undefined : stack[stack.length - 1]; }
  exit(callback, ...args) {
    if (typeof callback !== "function") throw new TypeError("callback must be a function");
    const saved = stack.splice(0, stack.length);
    try { return callback(...args); } finally { stack.push(...saved); }
  }
  disable() { stack.length = 0; }
}
export class AsyncResource {
  constructor(type = "AsyncResource") { this.type = String(type); this._id = nextId++; }
  asyncId() { return this._id; }
  triggerAsyncId() { return 0; }
  runInAsyncScope(callback, thisArg, ...args) {
    if (typeof callback !== "function") throw new TypeError("callback must be a function");
    return callback.apply(thisArg, args);
  }
  bind(callback, thisArg) {
    if (typeof callback !== "function") throw new TypeError("callback must be a function");
    return (...args) => this.runInAsyncScope(callback, thisArg, ...args);
  }
  emitDestroy() {}
}
export function createHook() { return Object.freeze({ enable() { return this; }, disable() { return this; } }); }
export function executionAsyncId() { return stack.length; }
export function triggerAsyncId() { return 0; }
export function executionAsyncResource() { return {}; }
export default { AsyncLocalStorage, AsyncResource, createHook, executionAsyncId, triggerAsyncId, executionAsyncResource };
"#;
