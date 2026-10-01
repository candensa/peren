pub(super) const NODE_BUFFER_SOURCE: &str = r#"
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const hex = "0123456789abcdef";
const encodings = new Set(["utf8", "utf-8", "hex", "base64", "base64url", "ascii", "latin1", "binary"]);
function normalize(encoding = "utf8") {
  const name = String(encoding).toLowerCase();
  if (!encodings.has(name)) throw new TypeError(`Unknown encoding: ${encoding}`);
  if (name === "utf-8") return "utf8";
  if (name === "binary") return "latin1";
  return name;
}
function bytesToBinary(bytes) {
  let out = "";
  for (let offset = 0; offset < bytes.length; offset += 8192) {
    out += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
  }
  return out;
}
function binaryToBytes(value) {
  const out = new Uint8Array(value.length);
  for (let i = 0; i < value.length; i++) out[i] = value.charCodeAt(i) & 0xff;
  return out;
}
function bytesToHex(bytes) {
  let out = "";
  for (const byte of bytes) out += hex[byte >> 4] + hex[byte & 0xf];
  return out;
}
function hexToBytes(value) {
  const clean = value.length % 2 === 0 ? value : value.slice(0, -1);
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  return out;
}
function base64urlToBase64(value) {
  let out = value.replace(/-/g, "+").replace(/_/g, "/");
  while (out.length % 4 !== 0) out += "=";
  return out;
}
function fromString(value, encoding) {
  switch (normalize(encoding)) {
    case "utf8": return encoder.encode(value);
    case "hex": return hexToBytes(value);
    case "base64": return binaryToBytes(atob(value));
    case "base64url": return binaryToBytes(atob(base64urlToBase64(value)));
    case "ascii":
    case "latin1": return binaryToBytes(value);
    default: throw new TypeError(`Unknown encoding: ${encoding}`);
  }
}
function toString(bytes, encoding) {
  switch (normalize(encoding)) {
    case "utf8": return decoder.decode(bytes);
    case "hex": return bytesToHex(bytes);
    case "base64": return btoa(bytesToBinary(bytes));
    case "base64url": return btoa(bytesToBinary(bytes)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    case "ascii":
    case "latin1": return bytesToBinary(bytes);
    default: throw new TypeError(`Unknown encoding: ${encoding}`);
  }
}
function view(value) {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  if (Array.isArray(value) || typeof value?.length === "number") return Uint8Array.from(value);
  throw new TypeError("value is not byte-like");
}
export class Buffer extends Uint8Array {
  static poolSize = 8192;
  static alloc(size, fill, encoding = "utf8") {
    const buffer = new Buffer(size);
    if (fill !== undefined && fill !== 0) buffer.fill(fill, 0, size, encoding);
    return buffer;
  }
  static allocUnsafe(size) { return new Buffer(size); }
  static allocUnsafeSlow(size) { return new Buffer(size); }
  static from(value, encodingOrOffset, length) {
    if (typeof value === "string") return new Buffer(fromString(value, encodingOrOffset));
    if (value instanceof Buffer) return new Buffer(value.buffer.slice(value.byteOffset, value.byteOffset + value.length));
    if (value instanceof ArrayBuffer) {
      const offset = encodingOrOffset ?? 0;
      const len = length === undefined ? value.byteLength - offset : length;
      return new Buffer(value, offset, len);
    }
    return new Buffer(view(value));
  }
  static isBuffer(value) { return value instanceof Buffer; }
  static isEncoding(value) { try { normalize(value); return true; } catch { return false; } }
  static byteLength(value, encoding = "utf8") { return typeof value === "string" ? fromString(value, encoding).length : value.byteLength ?? value.length; }
  static concat(list, totalLength = list.reduce((sum, item) => sum + item.length, 0)) {
    const out = new Buffer(totalLength);
    let offset = 0;
    for (const item of list) {
      if (offset >= totalLength) break;
      const bytes = view(item).subarray(0, totalLength - offset);
      out.set(bytes, offset);
      offset += bytes.length;
    }
    return out;
  }
  static compare(left, right) {
    const a = view(left);
    const b = view(right);
    for (let i = 0; i < Math.min(a.length, b.length); i++) if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
    return a.length === b.length ? 0 : a.length < b.length ? -1 : 1;
  }
  toString(encoding = "utf8", start = 0, end = this.length) { return toString(this.subarray(start, end), encoding); }
  toJSON() { return { type: "Buffer", data: Array.from(this) }; }
  equals(other) { return Buffer.compare(this, other) === 0; }
  compare(other) { return Buffer.compare(this, other); }
  slice(start, end) { return this.subarray(start, end); }
  write(value, offset = 0, length, encoding = "utf8") {
    if (typeof offset === "string") { encoding = offset; offset = 0; length = this.length; }
    else if (typeof length === "string") { encoding = length; length = this.length - offset; }
    else if (length === undefined) length = this.length - offset;
    const bytes = fromString(String(value), encoding).subarray(0, length);
    this.set(bytes, offset);
    return bytes.length;
  }
  fill(value, start = 0, end = this.length, encoding = "utf8") {
    if (typeof value === "number") return super.fill(value, start, end);
    const bytes = typeof value === "string" ? fromString(value, encoding) : view(value);
    if (bytes.length === 0) return this;
    for (let offset = start; offset < end; offset += bytes.length) this.set(bytes.subarray(0, Math.min(bytes.length, end - offset)), offset);
    return this;
  }
}
export const INSPECT_MAX_BYTES = 50;
export const kMaxLength = 0x7fffffff;
export default { Buffer, INSPECT_MAX_BYTES, kMaxLength };
"#;
