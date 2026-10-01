pub(super) const NODE_UTIL_SOURCE: &str = r#"
function quote(value) { return `'${String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`; }
export function inspect(value, options = {}) {
  const seen = new Set();
  const depth = options.depth === undefined ? 2 : options.depth === null ? Infinity : Number(options.depth);
  function render(item, level) {
    if (item === null) return "null";
    if (item === undefined) return "undefined";
    const kind = typeof item;
    if (kind === "string") return quote(item);
    if (kind === "number" || kind === "boolean") return String(item);
    if (kind === "bigint") return `${item}n`;
    if (kind === "symbol") return item.toString();
    if (kind === "function") return `[Function${item.name ? `: ${item.name}` : " (anonymous)"}]`;
    if (item instanceof Error) return item.stack || `${item.name}: ${item.message}`;
    if (item instanceof Date) return item.toISOString();
    if (item instanceof RegExp) return item.toString();
    if (seen.has(item)) return "[Circular *1]";
    if (level >= depth) return Array.isArray(item) ? "[Array]" : "[Object]";
    seen.add(item);
    let out;
    if (Array.isArray(item)) out = item.length ? `[ ${item.map((entry) => render(entry, level + 1)).join(", ")} ]` : "[]";
    else if (item instanceof Map) out = `Map(${item.size}) {${[...item].map(([key, entry]) => ` ${render(key, level + 1)} => ${render(entry, level + 1)}`).join(",")} }`;
    else if (item instanceof Set) out = `Set(${item.size}) {${[...item].map((entry) => ` ${render(entry, level + 1)}`).join(",")} }`;
    else out = `{${Object.keys(item).map((key) => ` ${/^[A-Za-z_$][\w$]*$/.test(key) ? key : quote(key)}: ${render(item[key], level + 1)}`).join(",")} }`;
    seen.delete(item);
    return out;
  }
  return render(value, 0);
}
inspect.custom = Symbol.for("nodejs.util.inspect.custom");
export function format(...args) {
  if (args.length === 0) return "";
  if (typeof args[0] !== "string") return args.map((arg) => typeof arg === "string" ? arg : inspect(arg)).join(" ");
  let index = 1;
  const out = args[0].replace(/%[sdifjoO%c%]/g, (token) => {
    if (token === "%%") return "%";
    if (index >= args.length) return token;
    const value = args[index++];
    if (token === "%s") return typeof value === "string" ? value : inspect(value);
    if (token === "%d") return String(Number(value));
    if (token === "%i") return String(parseInt(value, 10));
    if (token === "%f") return String(parseFloat(value));
    if (token === "%j") { try { return JSON.stringify(value); } catch { return "[Circular]"; } }
    if (token === "%o" || token === "%O") return inspect(value);
    if (token === "%c") return "";
    return token;
  });
  return out + args.slice(index).map((arg) => ` ${typeof arg === "string" ? arg : inspect(arg)}`).join("");
}
export function inherits(ctor, superCtor) { Object.setPrototypeOf(ctor.prototype, superCtor.prototype); Object.defineProperty(ctor, "super_", { value: superCtor, configurable: true, writable: true }); }
export function promisify(fn) {
  if (typeof fn !== "function") throw new TypeError('The "original" argument must be of type Function');
  if (fn[promisify.custom]) return fn[promisify.custom];
  return function promisified(...args) { return new Promise((resolve, reject) => fn.call(this, ...args, (error, ...values) => error ? reject(error) : resolve(values.length > 1 ? values : values[0]))); };
}
promisify.custom = Symbol.for("nodejs.util.promisify.custom");
export function callbackify(fn) {
  if (typeof fn !== "function") throw new TypeError('The "original" argument must be of type Function');
  return function callbackified(...args) { const callback = args.pop(); if (typeof callback !== "function") throw new TypeError("callback must be a function"); Promise.resolve(fn.apply(this, args)).then((value) => callback(null, value), (error) => callback(error instanceof Error ? error : new Error(String(error)))); };
}
export function deprecate(fn, message, code) { let warned = false; return function deprecated(...args) { if (!warned) { warned = true; console.warn(`${code ? `[${code}] ` : ""}DeprecationWarning: ${message}`); } return fn.apply(this, args); }; }
export const types = { isArrayBuffer: (v) => v instanceof ArrayBuffer, isTypedArray: (v) => ArrayBuffer.isView(v) && !(v instanceof DataView), isUint8Array: (v) => v instanceof Uint8Array, isDate: (v) => v instanceof Date, isRegExp: (v) => v instanceof RegExp, isPromise: (v) => v instanceof Promise, isMap: (v) => v instanceof Map, isSet: (v) => v instanceof Set, isAsyncFunction: (v) => typeof v === "function" && v.constructor?.name === "AsyncFunction", isGeneratorFunction: (v) => typeof v === "function" && v.constructor?.name === "GeneratorFunction", isNativeError: (v) => v instanceof Error };
export const isArray = Array.isArray;
export const isBoolean = (v) => typeof v === "boolean";
export const isNull = (v) => v === null;
export const isNullOrUndefined = (v) => v === null || v === undefined;
export const isNumber = (v) => typeof v === "number";
export const isString = (v) => typeof v === "string";
export const isSymbol = (v) => typeof v === "symbol";
export const isUndefined = (v) => v === undefined;
export const isObject = (v) => typeof v === "object" && v !== null;
export const isFunction = (v) => typeof v === "function";
export const isRegExp = (v) => v instanceof RegExp;
export const isDate = (v) => v instanceof Date;
export const isError = (v) => v instanceof Error;
export const isBuffer = (v) => v?.constructor?.name === "Buffer";
export const TextEncoder = globalThis.TextEncoder;
export const TextDecoder = globalThis.TextDecoder;
export default { inspect, format, inherits, promisify, callbackify, deprecate, types, isArray, isBoolean, isNull, isNullOrUndefined, isNumber, isString, isSymbol, isUndefined, isObject, isFunction, isRegExp, isDate, isError, isBuffer, TextEncoder, TextDecoder };
"#;
