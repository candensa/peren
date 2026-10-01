pub(super) const NODE_QUERYSTRING_SOURCE: &str = r#"
export function parse(str, sep = "&", eq = "=", options = {}) { const out = Object.create(null); if (typeof str !== "string" || str.length === 0) return out; const decode = options.decodeURIComponent || decodeURIComponent; const max = options.maxKeys === undefined ? 1000 : options.maxKeys; let count = 0; for (const pair of str.split(sep)) { if (!pair || (max > 0 && count >= max)) continue; count++; const index = pair.indexOf(eq); let key = index < 0 ? pair : pair.slice(0, index); let value = index < 0 ? "" : pair.slice(index + eq.length); try { key = decode(key); } catch {} try { value = decode(value); } catch {} if (Object.hasOwn(out, key)) out[key] = Array.isArray(out[key]) ? [...out[key], value] : [out[key], value]; else out[key] = value; } return out; }
export const decode = parse;
export function stringify(obj, sep = "&", eq = "=", options = {}) { if (obj === null || typeof obj !== "object") return ""; const encode = options.encodeURIComponent || encodeURIComponent; const parts = []; for (const key of Object.keys(obj)) { const value = obj[key]; const encoded = encode(String(key)); if (Array.isArray(value)) for (const item of value) parts.push(`${encoded}${eq}${encode(String(item))}`); else if (value !== undefined) parts.push(`${encoded}${eq}${encode(String(value))}`); } return parts.join(sep); }
export const encode = stringify;
export const escape = (value) => encodeURIComponent(String(value));
export function unescape(value) { try { return decodeURIComponent(String(value)); } catch { return String(value); } }
export default { parse, decode, stringify, encode, escape, unescape };
"#;

pub(super) const NODE_UNSUPPORTED_SOURCE: &str = r#"
function unavailable(name) { return function () { throw new Error(`${name} is not available inside a Worker isolate`); }; }
export const createServer = unavailable("createServer");
export const connect = unavailable("connect");
export const request = unavailable("request");
export const get = unavailable("get");
export const readFile = unavailable("readFile");
export const writeFile = unavailable("writeFile");
export const promises = Object.freeze({ readFile, writeFile });
export const createGzip = unavailable("createGzip");
export const createGunzip = unavailable("createGunzip");
export const test = unavailable("test");
export const channel = unavailable("channel");
export const lookup = unavailable("lookup");
export const createHook = unavailable("createHook");
export const toASCII = unavailable("toASCII");
export const toUnicode = unavailable("toUnicode");
export const Readable = unavailable("Readable");
export const Writable = unavailable("Writable");
export const Transform = unavailable("Transform");
export const buffer = unavailable("buffer");
export const text = unavailable("text");
export const json = unavailable("json");
export const pipeline = unavailable("pipeline");
export const finished = unavailable("finished");
export const builtin = false;
export default { createServer, connect, request, get, readFile, writeFile, promises, createGzip, createGunzip, test, channel, lookup, createHook, toASCII, toUnicode, Readable, Writable, Transform, buffer, text, json, pipeline, finished };
"#;

pub(super) const NODE_STRING_DECODER_SOURCE: &str = r#"
function tail(bytes) { for (let size = 1; size <= Math.min(3, bytes.length); size++) { const byte = bytes[bytes.length - size]; if ((byte & 0xc0) === 0xc0) { const sequence = byte >= 0xf0 ? 4 : byte >= 0xe0 ? 3 : 2; return sequence > size ? size : 0; } if ((byte & 0xc0) !== 0x80) return 0; } return 0; }
export class StringDecoder { constructor(encoding = "utf8") { const value = String(encoding).toLowerCase(); this.encoding = value === "utf-8" ? "utf8" : value; this._buffer = new Uint8Array(0); } write(input) { const bytes = input instanceof Uint8Array ? input : new Uint8Array(input); const all = new Uint8Array(this._buffer.length + bytes.length); all.set(this._buffer); all.set(bytes, this._buffer.length); if (this.encoding !== "utf8") { this._buffer = new Uint8Array(0); return new TextDecoder(this.encoding).decode(all); } const keep = tail(all); const ready = all.length - keep; this._buffer = all.slice(ready); return new TextDecoder().decode(all.slice(0, ready)); } end(input) { let out = input === undefined ? "" : this.write(input); if (this._buffer.length > 0) { out += new TextDecoder().decode(this._buffer); this._buffer = new Uint8Array(0); } return out; } text(input) { return this.write(input); } }
export default { StringDecoder };
"#;

pub(super) const NODE_CONSOLE_SOURCE: &str = r"
const console = globalThis.console;
export const Console = class Console { constructor(stdout, stderr) { this.stdout = stdout; this.stderr = stderr; } log(...args) { console.log(...args); } info(...args) { console.info(...args); } warn(...args) { console.warn(...args); } error(...args) { console.error(...args); } debug(...args) { console.debug(...args); } }
export const log = console.log.bind(console);
export const info = console.info.bind(console);
export const warn = console.warn.bind(console);
export const error = console.error.bind(console);
export const debug = console.debug.bind(console);
export default { Console, log, info, warn, error, debug };
";
