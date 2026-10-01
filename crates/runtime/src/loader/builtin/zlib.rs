pub(super) const NODE_ZLIB_SOURCE: &str = r#"
import { Buffer } from "node:buffer";
const byteView = (value) => value instanceof ArrayBuffer ? new Uint8Array(value) : ArrayBuffer.isView(value) ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength) : typeof value === "string" ? new TextEncoder().encode(value) : value instanceof Blob ? value : (() => { throw new TypeError("zlib input must be byte-like"); })();
async function transform(format, input, compression) {
  const body = input instanceof Blob ? input.stream() : new Response(byteView(input)).body;
  const stream = body.pipeThrough(compression ? new CompressionStream(format) : new DecompressionStream(format));
  return Buffer.from(await new Response(stream).arrayBuffer());
}
function callback(promise, cb) {
  if (typeof cb === "function") promise.then((value) => cb(null, value), cb);
  return promise;
}
export function gzip(input, options, cb) { if (typeof options === "function") cb = options; return callback(transform("gzip", input, true), cb); }
export function gunzip(input, options, cb) { if (typeof options === "function") cb = options; return callback(transform("gzip", input, false), cb); }
export function deflate(input, options, cb) { if (typeof options === "function") cb = options; return callback(transform("deflate", input, true), cb); }
export function inflate(input, options, cb) { if (typeof options === "function") cb = options; return callback(transform("deflate", input, false), cb); }
function refuse(name) { return function () { throw new Error(`zlib.${name}() is not supported synchronously inside a Worker isolate; use the async helper instead`); }; }
export const gzipSync = refuse("gzipSync");
export const gunzipSync = refuse("gunzipSync");
export const deflateSync = refuse("deflateSync");
export const inflateSync = refuse("inflateSync");
export const createGzip = refuse("createGzip");
export const createGunzip = refuse("createGunzip");
export const createDeflate = refuse("createDeflate");
export const createInflate = refuse("createInflate");
export const constants = Object.freeze({});
export default { gzip, gunzip, deflate, inflate, gzipSync, gunzipSync, deflateSync, inflateSync, createGzip, createGunzip, createDeflate, createInflate, constants };
"#;
