pub(super) const NODE_CRYPTO_SOURCE: &str = r#"

const { core } = await import("ext:core/mod.js");
import { Buffer } from "node:buffer";

function bytes(value, encoding = "utf8") {
  if (typeof value === "string") return Buffer.from(value, encoding);
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new TypeError("crypto input must be a string, ArrayBuffer, or typed array");
}
function normalize(algorithm) {
  return String(algorithm).replace(/[-_]/g, "").toLowerCase();
}
function digestName(algorithm) {
  switch (normalize(algorithm)) {
    case "sha256": return "SHA-256";
    case "sha384": return "SHA-384";
    case "sha512": return "SHA-512";
    default: throw new Error(`unsupported digest algorithm ${algorithm}`);
  }
}
function output(bytes, encoding) {
  const buffer = Buffer.from(bytes);
  return encoding === undefined ? buffer : buffer.toString(encoding);
}
export class KeyObject {
  #material;
  constructor(material) {
    this.#material = Buffer.from(material);
    this.type = "secret";
    this.symmetricKeySize = this.#material.byteLength;
  }
  export(options = {}) {
    const format = typeof options === "string" ? options : options.format ?? "buffer";
    if (format !== "buffer") throw new Error("unsupported KeyObject export format " + format);
    return Buffer.from(this.#material);
  }
  get [Symbol.toStringTag]() { return "KeyObject"; }
}
function keyBytes(value) {
  return value instanceof KeyObject ? value.export() : bytes(value);
}
export class Hash {
  #algorithm;
  #chunks = [];
  #done = false;
  constructor(algorithm) { this.#algorithm = digestName(algorithm); }
  update(data, inputEncoding) {
    if (this.#done) throw new Error("Hash already digested");
    this.#chunks.push(bytes(data, inputEncoding));
    return this;
  }
  digest(encoding) {
    if (this.#done) throw new Error("Hash already digested");
    this.#done = true;
    const body = Buffer.concat(this.#chunks);
    return output(core.ops.op_crypto_digest(this.#algorithm, Array.from(body)), encoding);
  }
}
export class Hmac {
  #algorithm;
  #key;
  #chunks = [];
  #done = false;
  constructor(algorithm, key) { this.#algorithm = digestName(algorithm); this.#key = keyBytes(key); }
  update(data, inputEncoding) {
    if (this.#done) throw new Error("Hmac already digested");
    this.#chunks.push(bytes(data, inputEncoding));
    return this;
  }
  digest(encoding) {
    if (this.#done) throw new Error("Hmac already digested");
    this.#done = true;
    const body = Buffer.concat(this.#chunks);
    return output(core.ops.op_crypto_hmac(this.#algorithm, Array.from(this.#key), Array.from(body)), encoding);
  }
}
export function createHash(algorithm) { return new Hash(algorithm); }
export function createHmac(algorithm, key) { return new Hmac(algorithm, key); }
export function createSecretKey(key) { return new KeyObject(bytes(key)); }
export function getHashes() { return ["sha256", "sha384", "sha512"]; }
export function randomBytes(size) {
  const length = Number(size);
  if (!Number.isInteger(length) || length < 0) throw new RangeError("randomBytes size must be a non-negative integer");
  return Buffer.from(core.ops.op_crypto_random(length));
}
export function randomUUID() { return globalThis.crypto.randomUUID(); }
export function timingSafeEqual(left, right) {
  const a = bytes(left);
  const b = bytes(right);
  if (a.byteLength !== b.byteLength) throw new RangeError("Input buffers must have the same byte length");
  let diff = 0;
  for (let index = 0; index < a.byteLength; index += 1) diff |= a[index] ^ b[index];
  return diff === 0;
}
export const webcrypto = globalThis.crypto;
export default { createHash, createHmac, createSecretKey, getHashes, randomBytes, randomUUID, timingSafeEqual, webcrypto, Hash, Hmac, KeyObject };
"#;
