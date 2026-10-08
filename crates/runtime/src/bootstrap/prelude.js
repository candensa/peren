import { core, primordials } from "ext:core/mod.js";

const { ArrayFrom, ObjectCreate, ObjectDefineProperties, ObjectDefineProperty, ObjectFreeze, ObjectIsExtensible, PromiseAll, PromiseResolve, Uint8Array } = primordials;
const event = core.loadExtScript("ext:deno_web/02_event.js");
const exception = core.loadExtScript("ext:deno_web/01_dom_exception.js");
const abort = core.loadExtScript("ext:deno_web/03_abort_signal.js");
const encoding = core.loadExtScript("ext:deno_web/08_text_encoding.js");
const base64 = core.loadExtScript("ext:deno_web/05_base64.js");
const streams = core.loadExtScript("ext:deno_web/06_streams.js");
const file = core.loadExtScript("ext:deno_web/09_file.js");
const filereader = core.loadExtScript("ext:deno_web/10_filereader.js");
const compression = core.loadExtScript("ext:deno_web/14_compression.js");
const urlPattern = core.loadExtScript("ext:deno_web/01_urlpattern.js");
const url = core.loadExtScript("ext:deno_web/00_url.js");
const timers = core.loadExtScript("ext:deno_web/02_timers.js");
const broadcast = core.loadExtScript("ext:deno_web/01_broadcast_channel.js");
const clone = core.loadExtScript("ext:deno_web/02_structured_clone.js");
const performanceApi = core.loadExtScript("ext:deno_web/15_performance.js");
const headers = core.loadExtScript("ext:deno_fetch/20_headers.js");
const formdata = core.loadExtScript("ext:deno_fetch/21_formdata.js");
const request = core.loadExtScript("ext:deno_fetch/23_request.js");
const response = core.loadExtScript("ext:deno_fetch/23_response.js");

const listeners = new Map();

const listenerSet = (type) => {
  const name = String(type);
  let set = listeners.get(name);
  if (set === undefined) {
    set = new Set();
    listeners.set(name, set);
  }
  return set;
};

const dispatchListener = async (type, event) => {
  const set = listeners.get(String(type));
  if (set === undefined || set.size === 0) return undefined;
  let result;
  for (const listener of set) {
    if (typeof listener === "function") {
      result = await listener.call(globalThis, event);
    } else if (typeof listener?.handleEvent === "function") {
      result = await listener.handleEvent(event);
    }
  }
  return result;
};

const immediateIds = new Map();
let nextImmediateId = 1;

const setImmediateCompat = (callback, ...args) => {
  if (typeof callback !== "function") {
    throw new TypeError("setImmediate callback must be a function");
  }
  const id = nextImmediateId++;
  const timeout = setTimeout(() => {
    immediateIds.delete(id);
    callback(...args);
  }, 0);
  immediateIds.set(id, timeout);
  return id;
};

const clearImmediateCompat = (id) => {
  const timeout = immediateIds.get(id);
  if (timeout !== undefined) {
    clearTimeout(timeout);
    immediateIds.delete(id);
  }
};

const consoleFormat = (value) => {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean" || value === undefined || value === null) return String(value);
  if (value instanceof Error) return value.stack || value.message || String(value);
  try { return JSON.stringify(value); } catch { return String(value); }
};

const consoleWrite = (level, args) => {
  core.ops.op_console_log(level, ArrayFrom(args, consoleFormat).join(" "));
};

const consoleCompat = ObjectFreeze({
  debug(...args) { consoleWrite("debug", args); },
  error(...args) { consoleWrite("error", args); },
  info(...args) { consoleWrite("info", args); },
  log(...args) { consoleWrite("info", args); },
  warn(...args) { consoleWrite("warn", args); },
});

const navigatorCompat = ObjectFreeze({
  userAgent: "Peren/0.1",
});
