// Generated from crates/runtime/src/bootstrap/*.js. Edit those files and rebuild the checked-in bundle.
import { core, primordials } from "ext:core/mod.js";

const { ArrayFrom, ObjectCreate, ObjectDefineProperties, ObjectFreeze, PromiseAll, PromiseResolve, Uint8Array } = primordials;
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

const cryptoBytes = (length) => new Uint8Array(core.ops.op_crypto_random(length));

const bufferSource = (value, name) => {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new TypeError(`${name} requires a BufferSource`);
};

const algorithmName = (algorithm, operation) => {
  const name = typeof algorithm === "string" ? algorithm : algorithm?.name;
  if (typeof name !== "string") throw new TypeError(`crypto.subtle.${operation} requires an algorithm name`);
  return name;
};

const normalizeHash = (hash) => {
  const name = typeof hash === "string" ? hash : hash?.name;
  if (typeof name !== "string") throw new TypeError("HMAC requires a hash algorithm");
  return name.replace(/\s+/g, "").toUpperCase();
};

const aesLength = (algorithm, material = undefined) => {
  const length = material === undefined ? Number(algorithm.length) : material.byteLength * 8;
  if (length !== 128 && length !== 192 && length !== 256) throw new DOMException("AES key length must be 128, 192, or 256 bits", "DataError");
  return length;
};

const aesGcmParams = (algorithm, operation) => {
  const iv = bufferSource(algorithm.iv, `crypto.subtle.${operation}`);
  if (iv.byteLength !== 12) throw new DOMException("AES-GCM requires a 12-byte iv", "OperationError");
  const additionalData = algorithm.additionalData == null ? new Uint8Array() : bufferSource(algorithm.additionalData, `crypto.subtle.${operation}`);
  const tagLength = Number(algorithm.tagLength ?? 128);
  if (tagLength !== 128) throw new DOMException("AES-GCM currently supports a 128-bit tagLength", "OperationError");
  return { iv, additionalData };
};

const aesCbcParams = (algorithm, operation) => {
  const iv = bufferSource(algorithm.iv, `crypto.subtle.${operation}`);
  if (iv.byteLength !== 16) throw new DOMException("AES-CBC requires a 16-byte iv", "OperationError");
  return { iv };
};

const pbkdf2Params = (algorithm) => {
  const hash = normalizeHash(algorithm.hash);
  const salt = bufferSource(algorithm.salt, "crypto.subtle.deriveBits");
  const iterations = Number(algorithm.iterations);
  if (!Number.isSafeInteger(iterations) || iterations <= 0) throw new DOMException("PBKDF2 iterations must be greater than zero", "OperationError");
  return { hash, salt, iterations };
};

const hkdfParams = (algorithm) => ({
  hash: normalizeHash(algorithm.hash),
  salt: bufferSource(algorithm.salt, "crypto.subtle.deriveBits"),
  info: bufferSource(algorithm.info ?? new Uint8Array(), "crypto.subtle.deriveBits"),
});

const derivedKeyAlgorithm = (algorithm, material) => {
  const name = algorithmName(algorithm, "deriveKey").replace(/\s+/g, "").toUpperCase();
  if (name === "AES-GCM" || name === "AES-CBC") {
    const algorithmName = name === "AES-GCM" ? "AES-GCM" : "AES-CBC";
    const length = aesLength(algorithm);
    return CryptoKey.create("secret", { name: algorithmName, length }, Boolean(algorithm.extractable), algorithm.usages, material);
  }
  if (name === "HMAC") {
    const hash = normalizeHash(algorithm.hash);
    return CryptoKey.create("secret", { name: "HMAC", hash: { name: hash }, length: material.byteLength * 8 }, Boolean(algorithm.extractable), algorithm.usages, material);
  }
  throw new DOMException(`unsupported derived key algorithm ${name}`, "NotSupportedError");
};

const constantTimeEqual = (left, right) => {
  if (left.byteLength !== right.byteLength) return false;
  let diff = 0;
  for (let index = 0; index < left.byteLength; index += 1) diff |= left[index] ^ right[index];
  return diff === 0;
};

class CryptoKey {
  constructor() {
    throw new TypeError("CryptoKey cannot be constructed directly");
  }

  static create(type, algorithm, extractable, usages, material) {
    const key = ObjectCreate(CryptoKey.prototype);
    ObjectDefineProperties(key, {
      type: { value: type, enumerable: true },
      algorithm: { value: ObjectFreeze({ ...algorithm }), enumerable: true },
      extractable: { value: extractable, enumerable: true },
      usages: { value: ObjectFreeze([...usages]), enumerable: true },
      material: { value: new Uint8Array(material), enumerable: false },
    });
    return ObjectFreeze(key);
  }
}

const cryptoCompat = ObjectFreeze({
  getRandomValues(view) {
    if (!ArrayBuffer.isView(view) || view instanceof DataView) {
      throw new TypeError("crypto.getRandomValues requires an integer typed array");
    }
    if (view.byteLength > 65536) {
      throw new DOMException("crypto.getRandomValues cannot fill more than 65536 bytes", "QuotaExceededError");
    }
    const bytes = cryptoBytes(view.byteLength);
    new Uint8Array(view.buffer, view.byteOffset, view.byteLength).set(bytes);
    return view;
  },
  randomUUID() {
    const bytes = cryptoBytes(16);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = ArrayFrom(bytes, (byte) => byte.toString(16).padStart(2, "0"));
    return `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}-${hex.slice(10, 16).join("")}`;
  },
  subtle: ObjectFreeze({
    async digest(algorithm, data) {
      const name = algorithmName(algorithm, "digest");
      const bytes = bufferSource(data, "crypto.subtle.digest");
      const digest = core.ops.op_crypto_digest(name, ArrayFrom(bytes));
      return new Uint8Array(digest).buffer;
    },
    async generateKey(algorithm, extractable, keyUsages) {
      const name = algorithmName(algorithm, "generateKey").replace(/\s+/g, "").toUpperCase();
      const usages = ArrayFrom(keyUsages ?? []);
      if (name === "AES-GCM" || name === "AES-CBC") {
        const algorithmName = name === "AES-GCM" ? "AES-GCM" : "AES-CBC";
        const length = aesLength(algorithm);
        for (const usage of usages) {
          if (usage !== "encrypt" && usage !== "decrypt") throw new DOMException(`unsupported ${algorithmName} key usage ${usage}`, "SyntaxError");
        }
        return CryptoKey.create("secret", { name: algorithmName, length }, Boolean(extractable), usages, cryptoBytes(length / 8));
      }
      if (name === "ED25519") {
        for (const usage of usages) {
          if (usage !== "sign" && usage !== "verify") throw new DOMException(`unsupported Ed25519 key usage ${usage}`, "SyntaxError");
        }
        const pair = core.ops.op_crypto_ed25519_generate();
        return ObjectFreeze({
          publicKey: CryptoKey.create("public", { name: "Ed25519" }, true, usages.filter((usage) => usage === "verify"), pair.publicKey),
          privateKey: CryptoKey.create("private", { name: "Ed25519" }, Boolean(extractable), usages.filter((usage) => usage === "sign"), pair.privateKey),
        });
      }
      if (name === "ECDSA") {
        const curve = String(algorithm.namedCurve ?? "").toUpperCase();
        if (curve !== "P-256") throw new DOMException(`unsupported ECDSA curve ${algorithm.namedCurve}`, "NotSupportedError");
        for (const usage of usages) {
          if (usage !== "sign" && usage !== "verify") throw new DOMException(`unsupported ECDSA key usage ${usage}`, "SyntaxError");
        }
        const pair = core.ops.op_crypto_ecdsa_p256_generate();
        return ObjectFreeze({
          publicKey: CryptoKey.create("public", { name: "ECDSA", namedCurve: "P-256" }, true, usages.filter((usage) => usage === "verify"), pair.publicKey),
          privateKey: CryptoKey.create("private", { name: "ECDSA", namedCurve: "P-256" }, Boolean(extractable), usages.filter((usage) => usage === "sign"), pair.privateKey),
        });
      }
      throw new DOMException(`unsupported key algorithm ${name}`, "NotSupportedError");
    },
    async importKey(format, keyData, algorithm, extractable, keyUsages) {
      const name = algorithmName(algorithm, "importKey").replace(/\s+/g, "").toUpperCase();
      const usages = ArrayFrom(keyUsages ?? []);
      if (name === "AES-GCM" || name === "AES-CBC") {
        const algorithmName = name === "AES-GCM" ? "AES-GCM" : "AES-CBC";
        if (format !== "raw") throw new DOMException(`only raw ${algorithmName} keys are supported`, "NotSupportedError");
        const material = bufferSource(keyData, "crypto.subtle.importKey");
        const length = aesLength(algorithm, material);
        for (const usage of usages) {
          if (usage !== "encrypt" && usage !== "decrypt") throw new DOMException(`unsupported ${algorithmName} key usage ${usage}`, "SyntaxError");
        }
        return CryptoKey.create("secret", { name: algorithmName, length }, Boolean(extractable), usages, material);
      }
      if (name === "HMAC") {
        if (format !== "raw") throw new DOMException("only raw HMAC keys are supported", "NotSupportedError");
        const hash = normalizeHash(algorithm.hash);
        for (const usage of usages) {
          if (usage !== "sign" && usage !== "verify") throw new DOMException(`unsupported HMAC key usage ${usage}`, "SyntaxError");
        }
        const material = bufferSource(keyData, "crypto.subtle.importKey");
        return CryptoKey.create("secret", { name: "HMAC", hash: { name: hash }, length: material.byteLength * 8 }, Boolean(extractable), usages, material);
      }
      if (name === "PBKDF2" || name === "HKDF") {
        const algorithmName = name === "PBKDF2" ? "PBKDF2" : "HKDF";
        if (format !== "raw") throw new DOMException(`only raw ${algorithmName} base keys are supported`, "NotSupportedError");
        for (const usage of usages) {
          if (usage !== "deriveBits" && usage !== "deriveKey") throw new DOMException(`unsupported ${algorithmName} key usage ${usage}`, "SyntaxError");
        }
        const material = bufferSource(keyData, "crypto.subtle.importKey");
        return CryptoKey.create("secret", { name: algorithmName }, false, usages, material);
      }
      if (name === "ED25519") {
        if (format !== "raw") throw new DOMException("only raw Ed25519 public keys are supported", "NotSupportedError");
        const material = bufferSource(keyData, "crypto.subtle.importKey");
        if (material.byteLength !== 32) throw new DOMException("Ed25519 public keys must be 32 bytes", "DataError");
        for (const usage of usages) {
          if (usage !== "verify") throw new DOMException(`unsupported Ed25519 public key usage ${usage}`, "SyntaxError");
        }
        return CryptoKey.create("public", { name: "Ed25519" }, Boolean(extractable), usages, material);
      }
      if (name === "ECDSA") {
        if (format !== "raw") throw new DOMException("only raw ECDSA public keys are supported", "NotSupportedError");
        const curve = String(algorithm.namedCurve ?? "").toUpperCase();
        if (curve !== "P-256") throw new DOMException(`unsupported ECDSA curve ${algorithm.namedCurve}`, "NotSupportedError");
        const material = bufferSource(keyData, "crypto.subtle.importKey");
        if (material.byteLength !== 65 || material[0] !== 4) throw new DOMException("ECDSA P-256 public keys must be uncompressed raw points", "DataError");
        for (const usage of usages) {
          if (usage !== "verify") throw new DOMException(`unsupported ECDSA public key usage ${usage}`, "SyntaxError");
        }
        return CryptoKey.create("public", { name: "ECDSA", namedCurve: "P-256" }, Boolean(extractable), usages, material);
      }
      throw new DOMException(`unsupported key algorithm ${name}`, "NotSupportedError");
    },
    async exportKey(format, key) {
      if (!(key instanceof CryptoKey)) throw new TypeError("crypto.subtle.exportKey requires a CryptoKey");
      if (key.algorithm.name === "AES-GCM" || key.algorithm.name === "AES-CBC") {
        if (format !== "raw") throw new DOMException(`only raw ${key.algorithm.name} keys are supported`, "NotSupportedError");
        if (!key.extractable) throw new DOMException("key is not extractable", "InvalidAccessError");
        return new Uint8Array(key.material).buffer;
      }
      if (key.algorithm.name === "HMAC") {
        if (format !== "raw") throw new DOMException("only raw HMAC keys are supported", "NotSupportedError");
        if (!key.extractable) throw new DOMException("key is not extractable", "InvalidAccessError");
        return new Uint8Array(key.material).buffer;
      }
      if (key.algorithm.name === "Ed25519") {
        if (format !== "raw" || key.type !== "public") throw new DOMException("only raw Ed25519 public keys are supported", "NotSupportedError");
        if (!key.extractable) throw new DOMException("key is not extractable", "InvalidAccessError");
        return new Uint8Array(key.material).buffer;
      }
      if (key.algorithm.name === "ECDSA") {
        if (format !== "raw" || key.type !== "public") throw new DOMException("only raw ECDSA public keys are supported", "NotSupportedError");
        if (!key.extractable) throw new DOMException("key is not extractable", "InvalidAccessError");
        return new Uint8Array(key.material).buffer;
      }
      throw new DOMException(`unsupported key algorithm ${key.algorithm.name}`, "NotSupportedError");
    },
    async encrypt(algorithm, key, data) {
      if (!(key instanceof CryptoKey)) throw new TypeError("crypto.subtle.encrypt requires a CryptoKey");
      const name = algorithmName(algorithm, "encrypt").replace(/\s+/g, "").toUpperCase();
      if (name === "AES-GCM" && key.algorithm.name === "AES-GCM" && key.usages.includes("encrypt")) {
        const params = aesGcmParams(algorithm, "encrypt");
        const encrypted = core.ops.op_crypto_aes_gcm_encrypt(ArrayFrom(key.material), ArrayFrom(params.iv), ArrayFrom(params.additionalData), ArrayFrom(bufferSource(data, "crypto.subtle.encrypt")));
        return new Uint8Array(encrypted).buffer;
      }
      if (name === "AES-CBC" && key.algorithm.name === "AES-CBC" && key.usages.includes("encrypt")) {
        const params = aesCbcParams(algorithm, "encrypt");
        const encrypted = core.ops.op_crypto_aes_cbc_encrypt(ArrayFrom(key.material), ArrayFrom(params.iv), ArrayFrom(bufferSource(data, "crypto.subtle.encrypt")));
        return new Uint8Array(encrypted).buffer;
      }
      throw new DOMException("key cannot be used for encryption", "InvalidAccessError");
    },
    async decrypt(algorithm, key, data) {
      if (!(key instanceof CryptoKey)) throw new TypeError("crypto.subtle.decrypt requires a CryptoKey");
      const name = algorithmName(algorithm, "decrypt").replace(/\s+/g, "").toUpperCase();
      if (name === "AES-GCM" && key.algorithm.name === "AES-GCM" && key.usages.includes("decrypt")) {
        const params = aesGcmParams(algorithm, "decrypt");
        const decrypted = core.ops.op_crypto_aes_gcm_decrypt(ArrayFrom(key.material), ArrayFrom(params.iv), ArrayFrom(params.additionalData), ArrayFrom(bufferSource(data, "crypto.subtle.decrypt")));
        return new Uint8Array(decrypted).buffer;
      }
      if (name === "AES-CBC" && key.algorithm.name === "AES-CBC" && key.usages.includes("decrypt")) {
        const params = aesCbcParams(algorithm, "decrypt");
        const decrypted = core.ops.op_crypto_aes_cbc_decrypt(ArrayFrom(key.material), ArrayFrom(params.iv), ArrayFrom(bufferSource(data, "crypto.subtle.decrypt")));
        return new Uint8Array(decrypted).buffer;
      }
      throw new DOMException("key cannot be used for decryption", "InvalidAccessError");
    },

    async deriveBits(algorithm, baseKey, length) {
      if (!(baseKey instanceof CryptoKey)) throw new TypeError("crypto.subtle.deriveBits requires a CryptoKey");
      const name = algorithmName(algorithm, "deriveBits").replace(/\s+/g, "").toUpperCase();
      if ((name !== "PBKDF2" && name !== "HKDF") || baseKey.algorithm.name !== name || !baseKey.usages.includes("deriveBits")) {
        throw new DOMException("key cannot be used for derivation", "InvalidAccessError");
      }
      const bits = Number(length);
      if (!Number.isSafeInteger(bits) || bits <= 0 || bits % 8 !== 0) throw new DOMException("deriveBits length must be a positive byte-aligned bit length", "OperationError");
      if (name === "PBKDF2") {
        const params = pbkdf2Params(algorithm);
        const derived = core.ops.op_crypto_pbkdf2(params.hash, ArrayFrom(baseKey.material), ArrayFrom(params.salt), params.iterations, bits / 8);
        return new Uint8Array(derived).buffer;
      }
      const params = hkdfParams(algorithm);
      const derived = core.ops.op_crypto_hkdf(params.hash, ArrayFrom(baseKey.material), ArrayFrom(params.salt), ArrayFrom(params.info), bits / 8);
      return new Uint8Array(derived).buffer;
    },
    async deriveKey(algorithm, baseKey, derivedKeyType, extractable, keyUsages) {
      if (!(baseKey instanceof CryptoKey)) throw new TypeError("crypto.subtle.deriveKey requires a CryptoKey");
      if (!baseKey.usages.includes("deriveKey")) throw new DOMException("key cannot be used for key derivation", "InvalidAccessError");
      const usages = ArrayFrom(keyUsages ?? []);
      const targetName = algorithmName(derivedKeyType, "deriveKey").replace(/\s+/g, "").toUpperCase();
      for (const usage of usages) {
        if ((targetName === "AES-GCM" || targetName === "AES-CBC") && usage !== "encrypt" && usage !== "decrypt") throw new DOMException(`unsupported ${targetName} key usage ${usage}`, "SyntaxError");
        if (targetName === "HMAC" && usage !== "sign" && usage !== "verify") throw new DOMException(`unsupported HMAC key usage ${usage}`, "SyntaxError");
      }
      const length = targetName === "HMAC" ? Number(derivedKeyType.length ?? 256) : aesLength(derivedKeyType);
      const material = new Uint8Array(await this.deriveBits(algorithm, baseKey, length));
      return derivedKeyAlgorithm({ ...derivedKeyType, extractable, usages }, material);
    },
    async sign(algorithm, key, data) {
      if (!(key instanceof CryptoKey)) throw new TypeError("crypto.subtle.sign requires a CryptoKey");
      const name = algorithmName(algorithm, "sign").replace(/\s+/g, "").toUpperCase();
      if (name === "HMAC" && key.algorithm.name === "HMAC" && key.usages.includes("sign")) {
        const signature = core.ops.op_crypto_hmac(key.algorithm.hash.name, ArrayFrom(key.material), ArrayFrom(bufferSource(data, "crypto.subtle.sign")));
        return new Uint8Array(signature).buffer;
      }
      if (name === "ED25519" && key.algorithm.name === "Ed25519" && key.type === "private" && key.usages.includes("sign")) {
        const signature = core.ops.op_crypto_ed25519_sign(ArrayFrom(key.material), ArrayFrom(bufferSource(data, "crypto.subtle.sign")));
        return new Uint8Array(signature).buffer;
      }
      if (name === "ECDSA" && key.algorithm.name === "ECDSA" && key.algorithm.namedCurve === "P-256" && key.type === "private" && key.usages.includes("sign")) {
        const signature = core.ops.op_crypto_ecdsa_p256_sign(ArrayFrom(key.material), ArrayFrom(bufferSource(data, "crypto.subtle.sign")));
        return new Uint8Array(signature).buffer;
      }
      throw new DOMException("key cannot be used for signing", "InvalidAccessError");
    },
    async verify(algorithm, key, signature, data) {
      if (!(key instanceof CryptoKey)) throw new TypeError("crypto.subtle.verify requires a CryptoKey");
      const name = algorithmName(algorithm, "verify").replace(/\s+/g, "").toUpperCase();
      if (name === "HMAC" && key.algorithm.name === "HMAC" && key.usages.includes("verify")) {
        const expected = new Uint8Array(core.ops.op_crypto_hmac(key.algorithm.hash.name, ArrayFrom(key.material), ArrayFrom(bufferSource(data, "crypto.subtle.verify"))));
        return constantTimeEqual(expected, bufferSource(signature, "crypto.subtle.verify"));
      }
      if (name === "ED25519" && key.algorithm.name === "Ed25519" && key.type === "public" && key.usages.includes("verify")) {
        return core.ops.op_crypto_ed25519_verify(ArrayFrom(key.material), ArrayFrom(bufferSource(signature, "crypto.subtle.verify")), ArrayFrom(bufferSource(data, "crypto.subtle.verify")));
      }
      if (name === "ECDSA" && key.algorithm.name === "ECDSA" && key.algorithm.namedCurve === "P-256" && key.type === "public" && key.usages.includes("verify")) {
        return core.ops.op_crypto_ecdsa_p256_verify(ArrayFrom(key.material), ArrayFrom(bufferSource(signature, "crypto.subtle.verify")), ArrayFrom(bufferSource(data, "crypto.subtle.verify")));
      }
      throw new DOMException("key cannot be used for verification", "InvalidAccessError");
    },
  }),
});

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
    const bytes = encodeStorageBytes(JSON.stringify(value));
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
    if (state.readyState === WebSocketPeer.CLOSED) return;
    state.readyState = WebSocketPeer.CLOSING;
    finishSocketClose(this, code, String(reason), true);
    const other = state.other;
    if (other !== null && socket(other).readyState !== WebSocketPeer.CLOSED) {
      finishSocketClose(other, code, String(reason), true);
    }
  }
}

WebSocketPeer.CONNECTING = 0;
WebSocketPeer.OPEN = 1;
WebSocketPeer.CLOSING = 2;
WebSocketPeer.CLOSED = 3;

const receiveSocketEvent = (target, event) => {
  const state = socket(target);
  if (state.readyState === WebSocketPeer.CLOSED) return;
  if (!state.accepted) {
    state.pending.push(event);
    return;
  }
  emitSocketEvent(target, event);
};

const finishSocketClose = (target, code, reason, wasClean) => {
  const state = socket(target);
  state.readyState = WebSocketPeer.CLOSED;
  state.pending = [];
  socketByAttachment.delete(state.attachmentId);
  emitSocketEvent(target, new WebSocketCloseEvent(code, reason, wasClean));
};

const emitSocketEvent = (target, event) => {
  PromiseResolve().then(() => {
    const state = socket(target);
    if (event.type === "message" && state.onmessage !== null) state.onmessage.call(target, event);
    if (event.type === "close" && state.onclose !== null) state.onclose.call(target, event);
    target.dispatchEvent(event);
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
class NonRetryableError extends Error {
  constructor(message = "non-retryable queue failure") {
    super(message);
    this.name = "NonRetryableError";
  }
}

performanceApi.setTimeOrigin();

ObjectDefineProperties(globalThis, {
  addEventListener: core.propNonEnumerable((type, listener) => {
    if (listener != null) listenerSet(type).add(listener);
  }),
  AbortController: core.propNonEnumerable(abort.AbortController),
  AbortSignal: core.propNonEnumerable(abort.AbortSignal),
  atob: core.propNonEnumerable(base64.atob),
  btoa: core.propNonEnumerable(base64.btoa),
  Blob: core.propNonEnumerable(file.Blob),
  BroadcastChannel: core.propNonEnumerable(broadcast.BroadcastChannel),
  ByteLengthQueuingStrategy: core.propNonEnumerable(streams.ByteLengthQueuingStrategy),
  console: core.propNonEnumerable(consoleCompat),
  crypto: core.propNonEnumerable(cryptoCompat),
  CryptoKey: core.propNonEnumerable(CryptoKey),
  DOMException: core.propNonEnumerable(exception.DOMException),
  Event: core.propNonEnumerable(event.Event),
  EventTarget: core.propNonEnumerable(event.EventTarget),
  File: core.propNonEnumerable(file.File),
  FileReader: core.propNonEnumerable(filereader.FileReader),
  fetch: core.propNonEnumerable(outboundFetch),
  FormData: core.propNonEnumerable(formdata.FormData),
  clearImmediate: core.propNonEnumerable(clearImmediateCompat),
  clearInterval: core.propNonEnumerable(timers.clearInterval),
  clearTimeout: core.propNonEnumerable(timers.clearTimeout),
  CompressionStream: core.propNonEnumerable(compression.CompressionStream),
  CountQueuingStrategy: core.propNonEnumerable(streams.CountQueuingStrategy),
  DecompressionStream: core.propNonEnumerable(compression.DecompressionStream),
  Headers: core.propNonEnumerable(headers.Headers),
  MessageChannel: core.propNonEnumerable(MessageChannel),
  MessageEvent: core.propNonEnumerable(event.MessageEvent),
  MessagePort: core.propNonEnumerable(MessagePort),
  navigator: core.propNonEnumerable(navigatorCompat),
  NonRetryableError: core.propNonEnumerable(NonRetryableError),
  performance: core.propNonEnumerable(performanceApi.performance),
  ProgressEvent: core.propNonEnumerable(event.ProgressEvent),
  Performance: core.propNonEnumerable(performanceApi.Performance),
  ReadableStream: core.propNonEnumerable(streams.ReadableStream),
  Request: core.propNonEnumerable(request.Request),
  Response: core.propNonEnumerable(response.Response),
  setImmediate: core.propNonEnumerable(setImmediateCompat),
  setInterval: core.propNonEnumerable(timers.setInterval),
  setTimeout: core.propNonEnumerable(timers.setTimeout),
  structuredClone: core.propNonEnumerable(clone.structuredClone),
  TextDecoder: core.propNonEnumerable(encoding.TextDecoder),
  TextDecoderStream: core.propNonEnumerable(encoding.TextDecoderStream),
  TextEncoder: core.propNonEnumerable(encoding.TextEncoder),
  TextEncoderStream: core.propNonEnumerable(encoding.TextEncoderStream),
  TransformStream: core.propNonEnumerable(streams.TransformStream),
  URL: core.propNonEnumerable(url.URL),
  URLPattern: core.propNonEnumerable(urlPattern.URLPattern),
  URLSearchParams: core.propNonEnumerable(url.URLSearchParams),
  WebSocket: core.propNonEnumerable(WebSocketPeer),
  WebSocketPair: core.propNonEnumerable(WebSocketPair),
  WritableStream: core.propNonEnumerable(streams.WritableStream),
});

const encodeStorageBytes = (value) => {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (typeof value === "string") {
    return new TextEncoder().encode(value);
  }
  throw new TypeError("storage values must be strings or Uint8Array instances");
};

const decodeStorageBytes = (value) => value === null ? undefined : new Uint8Array(value);

function hostResponse(response) {
  const status = Number(response.status);
  const body = status === 101 || status === 204 || status === 205 || status === 304
    ? null
    : new Uint8Array(response.body);
  return new Response(body, {
    status,
    headers: response.headers,
  });
}

async function outboundFetch(input, init = {}) {
  const request = new Request(input, init);
  const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
  const mtls = init?.cf?.mtlsCertificate?.__perenMtlsBinding;
  const response = await core.ops.op_outbound_fetch({
    method: request.method,
    url: request.url,
    headers: ArrayFrom(request.headers.entries()),
    body,
    mtls: typeof mtls === "string" ? mtls : null,
  });
  return hostResponse(response);
}


const deleteAllStorage = async (scope) => {
  let cursor = undefined;
  let deleted = 0;
  do {
    const options = { limit: 1000 };
    if (cursor !== undefined) options.cursor = ArrayFrom(encodeStorageBytes(cursor));
    const page = await core.ops.op_storage_list(scope, options);
    for (const entry of page.keys) {
      await core.ops.op_storage_delete(scope, ArrayFrom(encodeStorageBytes(entry.name)));
      deleted += 1;
    }
    cursor = page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor));
  } while (cursor !== undefined);
  return deleted;
};


const storageTransaction = () => ObjectFreeze({
  get: async (key, options = {}) => {
    const scope = options.scope ?? "do";
    return decodeStorageBytes(await core.ops.op_storage_get(scope, ArrayFrom(encodeStorageBytes(String(key)))));
  },
  put: (key, value, options = {}) => {
    const scope = options.scope ?? "do";
    return core.ops.op_storage_put(
      scope,
      ArrayFrom(encodeStorageBytes(String(key))),
      ArrayFrom(encodeStorageBytes(value)),
    );
  },
  delete: (key, options = {}) => {
    const scope = options.scope ?? "do";
    return core.ops.op_storage_delete(scope, ArrayFrom(encodeStorageBytes(String(key))));
  },
  deleteAll: (options = {}) => deleteAllStorage(options.scope ?? "do"),
});

const storage = {
  async get(key, options = {}) {
    const scope = options.scope ?? "do";
    return decodeStorageBytes(await core.ops.op_storage_get(scope, Array.from(encodeStorageBytes(String(key)))));
  },
  async delete(key, options = {}) {
    const scope = options.scope ?? "do";
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_storage_delete(scope, Array.from(encodeStorageBytes(String(key))));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  },
  async deleteAll(options = {}) {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const deleted = await deleteAllStorage(options.scope ?? "do");
      await core.ops.op_storage_commit();
      committed = true;
      return deleted;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  },
  async transaction(callback) {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const transaction = storageTransaction();
      const result = await callback(transaction);
      await core.ops.op_storage_commit();
      committed = true;
      return result;
    } finally {
      if (!committed) {
        await core.ops.op_storage_rollback();
      }
    }
  },
  async mutation(id, callback) {
    const mutationId = String(id);
    const existing = await core.ops.op_storage_mutation_outcome_get(mutationId);
    if (existing !== null) return JSON.parse(decodeText(new Uint8Array(existing)));
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const result = await callback(storageTransaction());
      await core.ops.op_storage_mutation_outcome_record(
        mutationId,
        ArrayFrom(encodeStorageBytes(JSON.stringify(result))),
      );
      await core.ops.op_storage_commit();
      committed = true;
      return result;
    } finally {
      if (!committed) {
        await core.ops.op_storage_rollback();
      }
    }
  },
};


const decodeText = (value) => new TextDecoder().decode(value);

const kvValue = (value) => Object.freeze({
  arrayBuffer: async () => value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
  json: async () => JSON.parse(decodeText(value)),
  text: async () => decodeText(value),
});

class EventSource extends event.EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;

  #url;
  #readyState = EventSource.CONNECTING;
  #closed = false;

  onopen = null;
  onmessage = null;
  onerror = null;
  withCredentials = false;

  constructor(url, init = {}) {
    super();
    this.#url = String(url);
    this.withCredentials = Boolean(init?.withCredentials);
    PromiseResolve().then(() => this.#connect());
  }

  get url() {
    return this.#url;
  }

  get readyState() {
    return this.#readyState;
  }

  close() {
    this.#closed = true;
    this.#readyState = EventSource.CLOSED;
  }

  async #connect() {
    try {
      const response = await outboundFetch(this.#url, {
        headers: { accept: "text/event-stream" },
      });
      if (this.#closed) return;
      if (response.status < 200 || response.status >= 300) {
        throw new TypeError(`EventSource request failed with status ${response.status}`);
      }
      this.#readyState = EventSource.OPEN;
      this.#emit("open", new event.Event("open"));
      const text = await response.text();
      if (this.#closed) return;
      this.#dispatchStream(text);
      if (!this.#closed) this.#readyState = EventSource.CLOSED;
    } catch (error) {
      if (this.#closed) return;
      this.#readyState = EventSource.CLOSED;
      const failure = new event.Event("error");
      failure.error = error;
      this.#emit("error", failure);
    }
  }

  #dispatchStream(text) {
    let name = "message";
    let data = [];
    let id = "";
    for (const line of String(text).replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n")) {
      if (line === "") {
        if (data.length > 0) {
          this.#emit(name, new event.MessageEvent(name, {
            data: data.join("\n"),
            lastEventId: id,
            origin: new URL(this.#url).origin,
          }));
        }
        name = "message";
        data = [];
        continue;
      }
      if (line.startsWith(":")) continue;
      const colon = line.indexOf(":");
      const field = colon === -1 ? line : line.slice(0, colon);
      const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
      if (field === "event") name = value || "message";
      if (field === "data") data.push(value);
      if (field === "id") id = value;
    }
  }

  #emit(type, dispatched) {
    this.dispatchEvent(dispatched);
    const handler = this[`on${type}`];
    if (typeof handler === "function") handler.call(this, dispatched);
  }
}

globalThis.EventSource = EventSource;
const kvReadValue = (value, options = {}) => {
  if (value === null) return null;
  const bytes = new Uint8Array(value);
  const type = options.type ?? "text";
  if (type === "arrayBuffer") return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  if (type === "json") return JSON.parse(decodeText(bytes));
  if (type === "stream") return new Response(bytes).body;
  return decodeText(bytes);
};

const kvPutBytes = (value) => {
  if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
  if (ArrayBuffer.isView(value)) return Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  return Array.from(encodeStorageBytes(String(value)));
};

const kvEnvelope = (value, options = {}, version = null) => ArrayFrom(encodeStorageBytes(JSON.stringify({ __perenKv: 1, value: kvPutBytes(value), metadata: options.metadata ?? null, expiresAt: kvExpiration(options), version })));
const kvDecodeRecord = (bytes) => {
  if (bytes === null) return { value: null, metadata: null, expiresAt: null };
  try {
    const decoded = JSON.parse(decodeText(new Uint8Array(bytes)));
    if (decoded && decoded.__perenKv === 1) return decoded;
  } catch (_) {}
  return { value: bytes, metadata: null, expiresAt: null, version: null };
};
const kvExpiration = (options = {}) => {
  if (options.expiration !== undefined) return Number(options.expiration) * 1000;
  if (options.expirationTtl !== undefined) return Date.now() + Number(options.expirationTtl) * 1000;
  return null;
};

class KvNamespace {
  constructor(scope, provider = ObjectFreeze({ kind: "native" })) {
    this.scope = String(scope);
    this.provider = provider;
  }
  async #record(key) {
    const encoded = ArrayFrom(encodeStorageBytes(String(key)));
    const bytes = this.provider.kind === "native"
      ? await core.ops.op_storage_get(this.scope, encoded)
      : await core.ops.op_kv_get({ namespace: this.scope, key: String(key) });
    const record = kvDecodeRecord(bytes);
    if (record.expiresAt !== null && Number(record.expiresAt) <= Date.now()) {
      await this.delete(key);
      return { value: null, metadata: null, version: null };
    }
    return { value: record.value, metadata: record.metadata, version: record.version ?? null };
  }
  async get(key, options = {}) {
    return kvReadValue((await this.#record(key)).value, options);
  }
  async getWithMetadata(key, options = {}) {
    const record = await this.#record(key);
    return { value: kvReadValue(record.value, options), metadata: record.value === null ? null : record.metadata, version: record.value === null ? null : record.version };
  }
  async put(key, value, options = {}) {
    if (this.provider.kind !== "native") {
      await core.ops.op_kv_put({ namespace: this.scope, key: String(key), value: kvEnvelope(value, options) });
      return;
    }
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const encoded = ArrayFrom(encodeStorageBytes(String(key)));
      const current = kvDecodeRecord(await core.ops.op_storage_get(this.scope, encoded));
      const version = Number(current.version ?? 0) + 1;
      await core.ops.op_storage_put(this.scope, encoded, kvEnvelope(value, options, version));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  }

  async compareAndSet(key, expected, value, options = {}) {
    if (this.provider.kind !== "native") {
      throw new Error("KV compareAndSet is only supported by native KV");
    }
    const encoded = ArrayFrom(encodeStorageBytes(String(key)));
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const current = kvDecodeRecord(await core.ops.op_storage_get(this.scope, encoded));
      const currentVersion = current.value === null ? null : current.version;
      const expectsMissing = expected?.missing === true;
      const expectedVersion = expected?.version === undefined ? undefined : Number(expected.version);
      const matched = expectsMissing ? current.value === null : expectedVersion !== undefined && currentVersion === expectedVersion;
      if (!matched) {
        await core.ops.op_storage_rollback();
        committed = true;
        return { ok: false, version: currentVersion };
      }
      const version = Number(currentVersion ?? 0) + 1;
      await core.ops.op_storage_put(this.scope, encoded, kvEnvelope(value, options, version));
      await core.ops.op_storage_commit();
      committed = true;
      return { ok: true, version };
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  }
  async delete(key) {
    if (this.provider.kind !== "native") {
      await core.ops.op_kv_delete({ namespace: this.scope, key: String(key) });
      return;
    }
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_storage_delete(this.scope, ArrayFrom(encodeStorageBytes(String(key))));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  }
  async list(options = {}) {
    if (this.provider.kind !== "native") {
      const page = await core.ops.op_kv_list({
        namespace: this.scope,
        prefix: options.prefix === undefined ? undefined : String(options.prefix),
        cursor: options.cursor === undefined ? undefined : String(options.cursor),
        limit: options.limit === undefined ? undefined : Number(options.limit),
      });
      return {
        keys: page.keys,
        cursor: page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor)),
        list_complete: page.listComplete,
      };
    }
    const request = {};
    if (options.prefix !== undefined) request.prefix = ArrayFrom(encodeStorageBytes(String(options.prefix)));
    if (options.cursor !== undefined) request.cursor = ArrayFrom(encodeStorageBytes(String(options.cursor)));
    if (options.limit !== undefined) request.limit = Number(options.limit);
    const page = await core.ops.op_storage_list(this.scope, request);
    return {
      keys: page.keys,
      cursor: page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor)),
      list_complete: page.listComplete,
    };
  }
}

const kvNamespace = (scope, provider = ObjectFreeze({ kind: "native" })) => ObjectFreeze(new KvNamespace(scope, provider));


const sqlValue = (value) => {
  if (value === null || value === undefined) return { type: "null" };
  if (value instanceof Uint8Array) return { type: "blob", value: ArrayFrom(value) };
  if (typeof value === "bigint") return { type: "integer", value: Number(value) };
  if (typeof value === "number") return Number.isInteger(value) ? { type: "integer", value } : { type: "real", value };
  return { type: "text", value: String(value) };
};

const executeSql = async (database, sql, parameters) => {
  await core.ops.op_storage_begin();
  let committed = false;
  try {
    const result = await core.ops.op_storage_sql({ database, sql, parameters });
    await core.ops.op_storage_commit();
    committed = true;
    return result;
  } finally {
    if (!committed) await core.ops.op_storage_rollback();
  }
};

const executeSqlBatch = async (database, statements) => {
  await core.ops.op_storage_begin();
  let committed = false;
  try {
    const results = [];
    for (const statement of statements) {
      results.push(await core.ops.op_storage_sql({
        database,
        sql: statement._sql,
        parameters: statement._parameters ?? [],
      }));
    }
    await core.ops.op_storage_commit();
    committed = true;
    return results;
  } finally {
    if (!committed) await core.ops.op_storage_rollback();
  }
};

const d1Rows = (result) => result.rows.map((row) => Object.fromEntries(result.columns.map((name, index) => [name, sqlCell(row[index])])));
const sqlCell = (cell) => {
  if (cell == null) return null;
  if (Object.hasOwn(cell, "null")) return null;
  if (Object.hasOwn(cell, "integer")) return cell.integer;
  if (Object.hasOwn(cell, "real")) return cell.real;
  if (Object.hasOwn(cell, "text")) return cell.text;
  if (Object.hasOwn(cell, "blob")) return new Uint8Array(cell.blob);
  const type = String(cell.type ?? cell.Type ?? "").toLowerCase();
  if (type === "null") return null;
  if (type === "blob") return new Uint8Array(cell.value);
  return cell.value;
};
const d1Meta = (result) => ({ changes: result.changes, last_row_id: result.last_insert_rowid });

class D1PreparedStatement {
  constructor(database, sql, parameters = []) {
    this.database = database;
    this._sql = String(sql);
    this._parameters = parameters;
  }
  bind(...parameters) {
    return ObjectFreeze(new D1PreparedStatement(this.database, this._sql, parameters.map(sqlValue)));
  }
  async all() {
    const result = await executeSql(this.database, this._sql, this._parameters);
    return { results: d1Rows(result), success: true, meta: d1Meta(result) };
  }
  async first(column) {
    const rows = (await this.all()).results;
    const first = rows[0] ?? null;
    return column == null || first === null ? first : first[column] ?? null;
  }
  async run() {
    const result = await executeSql(this.database, this._sql, this._parameters);
    return { success: true, meta: d1Meta(result), results: d1Rows(result) };
  }
  async raw() {
    const result = await executeSql(this.database, this._sql, this._parameters);
    return result.rows.map((row) => row.map(sqlCell));
  }
}

class D1Database {
  constructor(database, provider = ObjectFreeze({ kind: "native_sqlite" })) {
    this.database = String(database);
    this.provider = provider;
  }
  prepare(sql) {
    return ObjectFreeze(new D1PreparedStatement(this.database, sql));
  }
  async exec(sql) {
    const result = await executeSql(this.database, sql, []);
    return { count: result.changes, duration: 0 };
  }
  async batch(statements) {
    const results = await executeSqlBatch(this.database, ArrayFrom(statements ?? []));
    return results.map((result) => ({ success: true, meta: d1Meta(result), results: d1Rows(result) }));
  }
}

const d1Database = (database, provider = ObjectFreeze({ kind: "native_sqlite" })) => ObjectFreeze(new D1Database(database, provider));

const r2Body = (body) => {
  if (body instanceof Uint8Array) return ArrayFrom(body);
  if (body instanceof ArrayBuffer) return ArrayFrom(new Uint8Array(body));
  if (typeof body === "string") return ArrayFrom(encodeStorageBytes(body));
  return ArrayFrom(encodeStorageBytes(JSON.stringify(body)));
};

const r2Object = (object) => object === null ? null : Object.freeze({
  key: object.key,
  size: object.size,
  httpMetadata: Object.freeze({ contentType: object.contentType ?? undefined }),
  customMetadata: Object.freeze(object.customMetadata ?? {}),
  arrayBuffer: async () => new Uint8Array(object.body).buffer,
  text: async () => decodeText(new Uint8Array(object.body)),
  json: async () => JSON.parse(decodeText(new Uint8Array(object.body))),
});

class R2Bucket {
  constructor(bucket, prefix = "", provider = ObjectFreeze({ kind: "memory" })) {
    this.bucket = String(bucket);
    this.prefix = String(prefix ?? "");
    this.provider = provider;
  }
  async put(key, body, options = {}) {
    await core.ops.op_r2_put({
      bucket: this.bucket,
      key: `${this.prefix}${String(key)}`,
      body: r2Body(body),
      contentType: options.httpMetadata?.contentType == null ? null : String(options.httpMetadata.contentType),
      customMetadata: Object.fromEntries(Object.entries(options.customMetadata ?? {}).map(([key, value]) => [String(key), String(value)])),
    });
  }
  async get(key) {
    return r2Object(await core.ops.op_r2_get({ bucket: this.bucket, key: `${this.prefix}${String(key)}` }));
  }

  async compareAndSet(key, expected, value, options = {}) {
    if (this.provider.kind !== "native") {
      throw new Error("KV compareAndSet is only supported by native KV");
    }
    const encoded = ArrayFrom(encodeStorageBytes(String(key)));
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      const current = kvDecodeRecord(await core.ops.op_storage_get(this.scope, encoded));
      const currentVersion = current.value === null ? null : current.version;
      const expectsMissing = expected?.missing === true;
      const expectedVersion = expected?.version === undefined ? undefined : Number(expected.version);
      const matched = expectsMissing ? current.value === null : expectedVersion !== undefined && currentVersion === expectedVersion;
      if (!matched) {
        await core.ops.op_storage_rollback();
        committed = true;
        return { ok: false, version: currentVersion };
      }
      const version = Number(currentVersion ?? 0) + 1;
      await core.ops.op_storage_put(this.scope, encoded, kvEnvelope(value, options, version));
      await core.ops.op_storage_commit();
      committed = true;
      return { ok: true, version };
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  }
  async delete(key) {
    const keys = Array.isArray(key) ? key : [key];
    for (const item of keys) {
      await core.ops.op_r2_delete({ bucket: this.bucket, key: `${this.prefix}${String(item)}` });
    }
  }
  async list(options = {}) {
    const page = await core.ops.op_r2_list({
      bucket: this.bucket,
      prefix: `${this.prefix}${options.prefix ?? ""}`,
      cursor: options.cursor ?? null,
      limit: options.limit == null ? null : Number(options.limit),
    });
    return {
      objects: page.objects.map((object) => Object.freeze({
        key: object.key,
        size: object.size,
        customMetadata: Object.freeze(object.customMetadata ?? {}),
      })),
      cursor: page.cursor ?? undefined,
      truncated: !page.listComplete,
    };
  }
}

const r2Bucket = (bucket, prefix = "", provider = ObjectFreeze({ kind: "memory" })) => ObjectFreeze(new R2Bucket(bucket, prefix, provider));


globalThis.KvNamespace = KvNamespace;
globalThis.D1Database = D1Database;
globalThis.D1PreparedStatement = D1PreparedStatement;
globalThis.R2Bucket = R2Bucket;

const doEncode = (value) => btoa(String(value));
const doDecode = (value) => atob(String(value));

function scopedStorage(scope) {
  return ObjectFreeze({
    get: (key, options = {}) => storage.get(key, { ...options, scope }),
    put: (key, value, options = {}) => storage.transaction((transaction) => transaction.put(key, value, { ...options, scope })),
    delete: (key, options = {}) => storage.delete(key, { ...options, scope }),
    deleteAll: () => storage.deleteAll({ scope }),
    transaction: (callback) => storage.transaction((transaction) => callback(ObjectFreeze({
      get: (key, options = {}) => transaction.get(key, { ...options, scope }),
      put: (key, value, options = {}) => transaction.put(key, value, { ...options, scope }),
      delete: (key, options = {}) => transaction.delete(key, { ...options, scope }),
      deleteAll: () => transaction.deleteAll({ scope }),
    }))),
    mutation: (id, callback) => storage.mutation(id, (transaction) => callback(ObjectFreeze({
      get: (key, options = {}) => transaction.get(key, { ...options, scope }),
      put: (key, value, options = {}) => transaction.put(key, value, { ...options, scope }),
      delete: (key, options = {}) => transaction.delete(key, { ...options, scope }),
      deleteAll: () => transaction.deleteAll({ scope }),
    }))),
  });
}

class DurableObjectFacetStub {
  constructor(name, instance, record) {
    this.name = name;
    this.instance = instance;
    this.record = record;
  }
  async fetch(input, init = {}) {
    if (this.record.aborted) throw new Error(`Durable Object facet ${this.name} has been aborted`);
    if (typeof this.instance.fetch !== "function") throw new TypeError(`Durable Object facet ${this.name} does not export fetch`);
    return await Promise.resolve(this.instance.fetch(new Request(input, init)));
  }
}

class DurableObjectFacets {
  constructor(env) {
    this.env = env;
    this.records = new Map();
  }
  get(name, callback) {
    const key = String(name);
    let record = this.records.get(key);
    if (record === undefined) {
      if (typeof callback !== "function") throw new TypeError("ctx.facets.get requires a callback for a new facet");
      const scope = `facet:${doEncode(key)}`;
      const ctx = new DurableObjectState(scopedStorage(scope), this.env);
      const target = callback();
      const instance = typeof target === "function" ? new target(ctx, this.env) : target;
      if (instance == null || typeof instance !== "object") throw new TypeError("ctx.facets.get callback must return an object or class");
      record = { instance, aborted: false, scope };
      this.records.set(key, record);
    }
    record.aborted = false;
    const stub = new DurableObjectFacetStub(key, record.instance, record);
    return rpcProxy(stub, async (method, args) => {
      if (record.aborted) throw new Error(`Durable Object facet ${key} has been aborted`);
      const target = record.instance?.[method];
      if (typeof target !== "function") throw new TypeError(`Durable Object facet ${key} does not export ${method}`);
      return await Promise.resolve(target.apply(record.instance, args));
    });
  }
  abort(name) {
    const record = this.records.get(String(name));
    if (record === undefined) return false;
    record.aborted = true;
    return true;
  }
  async delete(name) {
    const key = String(name);
    const record = this.records.get(key);
    const scope = record?.scope ?? `facet:${doEncode(key)}`;
    this.records.delete(key);
    await scopedStorage(scope).deleteAll();
    return true;
  }
}

function durableValue(value) {
  if (value instanceof Uint8Array) {
    return JSON.stringify({ __perenDurable: 1, bytes: ArrayFrom(value) });
  }
  const encoded = JSON.stringify({ __perenDurable: 1, json: value });
  if (encoded === undefined) throw new TypeError("storage value must be JSON or Uint8Array");
  return encoded;
}

function readDurableValue(bytes) {
  if (bytes === undefined) return undefined;
  try {
    const parsed = JSON.parse(decodeText(bytes));
    if (parsed && parsed.__perenDurable === 1) {
      if (Array.isArray(parsed.bytes)) return new Uint8Array(parsed.bytes);
      return parsed.json;
    }
  } catch (_) {}
  return bytes;
}

function durableObjectStorage(inner) {
  return ObjectFreeze({
    get: async (key, options = {}) => readDurableValue(await inner.get(key, options)),
    put: async (key, value, options = {}) => {
      await inner.transaction(async (txn) => {
        await txn.put(key, durableValue(value), options);
      });
    },
    delete: (key, options) => inner.delete(key, options),
    deleteAll: (options) => inner.deleteAll(options),
    transaction: (callback) => inner.transaction(callback),
    mutation: (id, callback) => inner.mutation(id, callback),
  });
}

class DurableObjectState {
  constructor(storageBinding = storage, env = {}) {
    this.storage = durableObjectStorage(storageBinding);
    this.facets = ObjectFreeze(new DurableObjectFacets(env));
    this.env = env;
  }

  acceptWebSocket(socket, tags = []) {
    if (!(socket instanceof WebSocketPeer)) throw new TypeError("acceptWebSocket requires a WebSocket");
    socket.accept();
    const normalized = ArrayFrom(tags ?? [], String);
    durableWebSockets.set(socket, ObjectFreeze(normalized));
    persistDurableWebSocket(socket, normalized);
    return socket;
  }

  getWebSockets(tag = undefined) {
    const selected = [];
    for (const [socket, tags] of durableWebSockets.entries()) {
      if (socket.readyState === WebSocketPeer.CLOSED) {
        durableWebSockets.delete(socket);
        continue;
      }
      if (tag === undefined || tags.includes(String(tag))) selected.push(socket);
    }
    return ObjectFreeze(selected);
  }

  setWebSocketAutoResponse(pair) {
    if (pair != null && !(pair instanceof WebSocketRequestResponsePair)) {
      throw new TypeError("setWebSocketAutoResponse requires a WebSocketRequestResponsePair");
    }
    durableWebSocketAutoResponse = pair ?? null;
    durableWebSocketAutoResponseTimestamp = pair == null ? null : Date.now();
    persistDurableWebSocketAutoResponse();
  }

  getWebSocketAutoResponse() {
    return durableWebSocketAutoResponse;
  }

  getWebSocketAutoResponseTimestamp() {
    return durableWebSocketAutoResponseTimestamp;
  }
}

class DurableObject {
  constructor(ctx = new DurableObjectState(), env = {}) {
    this.ctx = ctx;
    this.env = env;
  }
}

class WorkerEntrypoint {
  constructor(ctx = {}, env = {}) {
    this.ctx = ctx;
    this.env = env;
  }
}

class RpcTarget {}

const rpcHeader = "x-peren-rpc-method";
const rpcContentType = "application/vnd.peren.rpc+json";

async function readRpcResponse(response) {
  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json") || contentType.includes(rpcContentType)
    ? await response.json()
    : { ok: response.ok, value: await response.text() };
  if (!response.ok || payload?.ok === false) {
    const message = payload?.error?.message ?? payload?.error ?? `RPC call failed with status ${response.status}`;
    throw new Error(String(message));
  }
  return payload?.value;
}

function rpcProxy(target, invoke) {
  return new Proxy(target, {
    get(receiver, property, value) {
      if (typeof property !== "string") return Reflect.get(receiver, property, value);
      if (property in receiver) {
        const member = Reflect.get(receiver, property, value);
        return typeof member === "function" ? member.bind(receiver) : member;
      }
      if (property === "then") return undefined;
      return async (...args) => await invoke(property, args);
    },
  });
}

function rpcRequest(method, args, url = "https://rpc.internal/") {
  return new Request(url, {
    method: "POST",
    headers: {
      [rpcHeader]: method,
      "content-type": rpcContentType,
    },
    body: JSON.stringify({ args }),
  });
}

class WebSocketRequestResponsePair {
  constructor(request, response) {
    this.request = String(request);
    this.response = String(response);
  }
}

let durableWebSocketAutoResponse = null;
let durableWebSocketAutoResponseTimestamp = null;
const durableWebSockets = new Map();
const durableWebSocketScope = "__peren:websocket";
const durableWebSocketAutoResponseKey = "__auto_response";

function persistDurableWebSocketAutoResponse() {
  const task = (async () => {
    try {
      await core.ops.op_storage_begin();
      let committed = false;
      try {
        if (durableWebSocketAutoResponse === null) {
          await core.ops.op_storage_delete(durableWebSocketScope, ArrayFrom(encodeStorageBytes(durableWebSocketAutoResponseKey)));
        } else {
          await core.ops.op_storage_put(
            durableWebSocketScope,
            ArrayFrom(encodeStorageBytes(durableWebSocketAutoResponseKey)),
            ArrayFrom(encodeStorageBytes(JSON.stringify({
              request: durableWebSocketAutoResponse.request,
              response: durableWebSocketAutoResponse.response,
              timestamp: durableWebSocketAutoResponseTimestamp,
            }))),
          );
        }
        await core.ops.op_storage_commit();
        committed = true;
      } finally {
        if (!committed) await core.ops.op_storage_rollback();
      }
    } catch {
      // Stateless runtimes keep auto-response metadata in isolate memory only.
    }
  })();
  if (typeof globalThis.__perenRegisterWaitUntil === "function") globalThis.__perenRegisterWaitUntil(task);
}

function persistDurableWebSocket(socket, tags) {
  const id = globalThis.__perenWebSocketAttachmentId(socket);
  if (id == null) return;
  const task = (async () => {
    try {
      await core.ops.op_storage_begin();
      let committed = false;
      try {
        await core.ops.op_storage_put(
          durableWebSocketScope,
          ArrayFrom(encodeStorageBytes(id)),
          ArrayFrom(encodeStorageBytes(JSON.stringify({ id, tags: ArrayFrom(tags, String) }))),
        );
        await core.ops.op_storage_commit();
        committed = true;
      } finally {
        if (!committed) await core.ops.op_storage_rollback();
      }
    } catch {
      // Stateless runtimes keep accepted sockets in isolate memory only.
    }
  })();
  if (typeof globalThis.__perenRegisterWaitUntil === "function") globalThis.__perenRegisterWaitUntil(task);
}

globalThis.__perenMaybeAutoRespondWebSocket = (socket, message) => {
  const pair = durableWebSocketAutoResponse;
  if (pair === null || String(message) !== pair.request) return false;
  socket.send(pair.response);
  durableWebSocketAutoResponseTimestamp = Date.now();
  persistDurableWebSocketAutoResponse();
  return true;
};

globalThis.__perenDeleteDurableWebSocket = async (id) => {
  const key = String(id);
  try {
    await core.ops.op_storage_begin();
    let committed = false;
    try {
      await core.ops.op_storage_delete(durableWebSocketScope, ArrayFrom(encodeStorageBytes(key)));
      await core.ops.op_storage_commit();
      committed = true;
    } finally {
      if (!committed) await core.ops.op_storage_rollback();
    }
  } catch {
    // Stateless runtimes have no durable hibernation record to delete.
  }
  const socket = socketByAttachment.get(key);
  if (socket !== undefined) durableWebSockets.delete(socket);
};

globalThis.__perenHydrateDurableWebSockets = async () => {
  try {
    let cursor;
    do {
      const options = { limit: 1000 };
      if (cursor !== undefined) options.cursor = ArrayFrom(encodeStorageBytes(cursor));
      const page = await core.ops.op_storage_list(durableWebSocketScope, options);
      for (const key of page.keys) {
        const id = key.name;
        const bytes = await core.ops.op_storage_get(durableWebSocketScope, ArrayFrom(encodeStorageBytes(id)));
        if (bytes === null) continue;
        const record = JSON.parse(decodeText(new Uint8Array(bytes)));
        if (id === durableWebSocketAutoResponseKey) {
          durableWebSocketAutoResponse = new WebSocketRequestResponsePair(record.request, record.response);
          durableWebSocketAutoResponseTimestamp = record.timestamp ?? null;
          continue;
        }
        const socket = globalThis.__perenHostWebSocket(String(record.id ?? id));
        durableWebSockets.set(socket, ObjectFreeze(ArrayFrom(record.tags ?? [], String)));
      }
      cursor = page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor));
    } while (cursor !== undefined);
  } catch {
    // Stateless runtimes have no durable storage host; hibernated sockets hydrate when storage exists.
  }
};

class DurableObjectId {
  constructor(namespace, value, name = undefined) {
    this.namespace = namespace;
    this.value = value;
    this.name = name;
  }
  toString() {
    return `${this.namespace}:${this.value}`;
  }
}

class DurableObjectStub {
  constructor(id, className) {
    this.id = id;
    this.name = id.name;
    this.className = className;
  }
  async fetch(input, init = {}) {
    const request = new Request(input, init);
    const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
    const response = await core.ops.op_durable_object_fetch({
      namespace: this.id.namespace,
      id: this.id.value,
      name: this.id.name ?? null,
      className: this.className,
      request: {
        method: request.method,
        url: request.url,
        headers: ArrayFrom(request.headers.entries()),
        body,
      },
    });
    return hostResponse(response);
  }
}

class DispatchNamespace {}

class DurableObjectNamespace {
  constructor(binding, className) {
    this.binding = binding;
    this.className = className;
  }
  idFromName(name) {
    const value = String(name);
    return ObjectFreeze(new DurableObjectId(this.binding, `name:${doEncode(value)}`, value));
  }
  idFromString(id) {
    const value = String(id);
    const prefix = `${this.binding}:`;
    if (!value.startsWith(prefix)) throw new TypeError("Durable Object id belongs to a different namespace");
    const raw = value.slice(prefix.length);
    const name = raw.startsWith("name:") ? doDecode(raw.slice(5)) : undefined;
    return ObjectFreeze(new DurableObjectId(this.binding, raw, name));
  }
  newUniqueId() {
    return ObjectFreeze(new DurableObjectId(this.binding, `unique:${workflowId()}`));
  }
  get(id) {
    if (!(id instanceof DurableObjectId) || id.namespace !== this.binding) {
      throw new TypeError("Durable Object id belongs to a different namespace");
    }
    const stub = new DurableObjectStub(id, this.className);
    return rpcProxy(stub, async (method, args) => {
      const response = await stub.fetch(rpcRequest(method, args));
      return await readRpcResponse(response);
    });
  }
}

const durableObjectNamespace = (binding, className) => ObjectFreeze(new DurableObjectNamespace(binding, className));

const dispatchNamespace = (binding) => {
  const scripts = ArrayFrom(binding.scripts ?? [], String);
  const namespace = String(binding.namespace ?? "");
  return ObjectFreeze(Object.assign(ObjectCreate(DispatchNamespace.prototype), {
    namespace,
    scripts: ObjectFreeze(scripts),
    get(name) {
      const script = String(name);
      if (!scripts.includes(script)) throw new TypeError(`unknown dispatch script ${script}`);
      return serviceBinding(`${namespace}/${script}`);
    },
  }));
};

const mtlsCertificate = (binding) => ObjectFreeze({ __perenMtlsBinding: binding });

function imagesBinding(binding) {
  const provider = binding.provider ?? { kind: "local" };
  const metadata = ObjectFreeze({ kind: provider.kind ?? "local", endpoint: provider.url });
  const makeRequest = (source, transforms = {}) => {
    if (provider.kind === "http") {
      const headers = { "content-type": "application/json" };
      if (provider.authorization) headers.authorization = provider.authorization;
      return outboundFetch(String(provider.url).replace(/\/$/, "") + "/transform", {
        method: "POST",
        headers,
        body: JSON.stringify({ source, transforms }),
      });
    }
    const body = typeof source === "string" ? source : JSON.stringify(source);
    return PromiseResolve(new Response(body, { headers: { "content-type": "application/octet-stream" } }));
  };
  return ObjectFreeze(Object.assign(ObjectCreate(ImagesBinding.prototype), {
    provider: metadata,
    input(source) {
      const transforms = {};
      return ObjectFreeze({
        transform(options = {}) { Object.assign(transforms, options); return this; },
        async output() { return await makeRequest(source, transforms); },
      });
    },
  }));
}

function containerBinding(binding) {
  const provider = ObjectFreeze(binding.provider ?? { kind: "local" });
  const port = Number(binding.port);
  const target = `http://127.0.0.1:${port}`;
  const unsupportedControl = (operation) => {
    throw new TypeError(`Container.${operation} requires a managed sandbox provider; this binding currently supports fetch() through the configured local port`);
  };
  const instance = (name = "default", freeze = true) => {
    const id = String(name);
    const metadata = ObjectFreeze({
      id,
      image: String(binding.image ?? ""),
      port,
      memoryMb: binding.memoryMb ?? null,
      cpuMillis: binding.cpuMillis ?? null,
      idleSleepSecs: Number(binding.idleSleepSecs ?? 0),
      allowNetworkEgress: Boolean(binding.allowNetworkEgress),
      experimental: true,
      provider,
    });
    const api = Object.assign(ObjectCreate(Container.prototype), metadata, {
      async fetch(input, init = {}) {
        try {
          const request = input instanceof Request ? input : new Request(input, init);
          const source = new URL(request.url);
          const destination = new URL(target);
          destination.pathname = source.pathname;
          destination.search = source.search;
          return await outboundFetch(destination, request);
        } catch (error) {
          return new Response(String(error && error.message ? error.message : error), { status: 502 });
        }
      },

      async status() {
        unsupportedControl("status");
      },

      async start() {
        unsupportedControl("start");
      },

      async stop() {
        unsupportedControl("stop");
      },

      async destroy() {
        unsupportedControl("destroy");
      },

      async exec(_command, _options = {}) {
        unsupportedControl("exec");
      },

      async writeFile(_path, _contents) {
        unsupportedControl("writeFile");
      },

      async readFile(_path) {
        unsupportedControl("readFile");
      },
    });
    return freeze ? ObjectFreeze(api) : api;
  };
  const root = instance("default", false);
  return ObjectFreeze(Object.assign(root, {
    get(name) { return instance(name); },
  }));
}

const loaderBinding = () => ObjectFreeze(Object.assign(ObjectCreate(Loader.prototype), {
  async import(specifier) {
    const value = String(specifier);
    if (value.length === 0) throw new TypeError("module specifier cannot be empty");
    if (/^(?:https?:|node:|npm:|jsr:)/.test(value)) {
      throw new TypeError(`unsupported dynamic module specifier ${value}`);
    }
    const base = globalThis.__perenEntryModuleSpecifier;
    if (typeof base !== "string" || base.length === 0) {
      throw new TypeError("Worker entry module is unavailable for dynamic import");
    }
    return await import(new URL(value, base).href);
  },
}));


class Ai {}
class AnalyticsEngineDataset {}
class Container {}
class Hyperdrive {}
class ImagesBinding {}
class Loader {}
class RateLimiter {}
class VectorizeIndex {}


function aiText(prompt) {
  if (typeof prompt === "string") return prompt;
  if (prompt?.prompt !== undefined) return String(prompt.prompt);
  if (prompt?.text !== undefined) return String(prompt.text);
  if (prompt?.input !== undefined) return String(prompt.input);
  return String(prompt ?? "");
}

function aiMessages(messages) {
  return ArrayFrom(messages ?? [], (message) => ({ ...message }));
}

function aiOptions(options = {}) {
  const body = { ...options };
  delete body.headers;
  delete body.maxTokens;
  delete body.maxOutputTokens;
  return body;
}

function aiLimit(options = {}, fallback = undefined) {
  const value = options.max_tokens ?? options.maxTokens ?? options.maxOutputTokens ?? options.max_output_tokens ?? fallback;
  return value === undefined ? undefined : Number(value);
}

function aiTextInput(route, model, prompt, options = {}) {
  const text = aiText(prompt);
  const body = aiOptions(options);
  if (route.kind === "open_ai") {
    const limit = aiLimit(options);
    if (limit !== undefined) body.max_output_tokens = limit;
    return Object.assign({ input: text }, body);
  }
  if (route.kind === "anthropic") {
    return Object.assign({
      messages: [{ role: "user", content: text }],
      max_tokens: aiLimit(options, 1024),
    }, body);
  }
  if (route.kind === "gemini") {
    const generationConfig = {};
    const limit = aiLimit(options);
    if (limit !== undefined) generationConfig.maxOutputTokens = limit;
    if (options.temperature !== undefined) generationConfig.temperature = Number(options.temperature);
    delete body.temperature;
    const request = Object.assign({ contents: [{ role: "user", parts: [{ text }] }] }, body);
    if (Object.keys(generationConfig).length > 0) request.generationConfig = generationConfig;
    return request;
  }
  return Object.assign({ prompt: text }, body);
}

function aiChatInput(route, messages, options = {}) {
  const list = aiMessages(messages);
  const body = aiOptions(options);
  if (route.kind === "open_ai") {
    const limit = aiLimit(options);
    if (limit !== undefined) body.max_output_tokens = limit;
    return Object.assign({ input: list }, body);
  }
  if (route.kind === "anthropic") {
    return Object.assign({
      messages: list,
      max_tokens: aiLimit(options, 1024),
    }, body);
  }
  if (route.kind === "gemini") {
    const generationConfig = {};
    const limit = aiLimit(options);
    if (limit !== undefined) generationConfig.maxOutputTokens = limit;
    if (options.temperature !== undefined) generationConfig.temperature = Number(options.temperature);
    delete body.temperature;
    const request = Object.assign({
      contents: list.map((message) => ({
        role: message.role === "assistant" ? "model" : String(message.role ?? "user"),
        parts: Array.isArray(message.parts) ? message.parts : [{ text: String(message.content ?? message.text ?? "") }],
      })),
    }, body);
    if (Object.keys(generationConfig).length > 0) request.generationConfig = generationConfig;
    return request;
  }
  return Object.assign({ messages: list }, body);
}

function aiEmbedInput(route, input, options = {}) {
  const text = typeof input === "string" ? input : input?.text ?? input?.input ?? input;
  const body = aiOptions(options);
  if (route.kind === "open_ai") return Object.assign({ input: text }, body);
  if (route.kind === "gemini") return Object.assign({ content: { parts: [{ text: String(text ?? "") }] } }, body);
  return Object.assign({ text }, body);
}

function aiTextOutput(raw) {
  if (typeof raw === "string") return { text: raw, raw };
  if (raw?.output_text !== undefined) return { text: String(raw.output_text), raw };
  if (Array.isArray(raw?.output)) {
    const text = raw.output
      .flatMap((item) => Array.isArray(item?.content) ? item.content : [])
      .filter((item) => item?.type === "output_text" || item?.text !== undefined)
      .map((item) => String(item.text ?? ""))
      .join("");
    if (text.length > 0) return { text, raw };
  }
  if (raw?.content !== undefined) {
    if (typeof raw.content === "string") return { text: raw.content, raw };
    if (Array.isArray(raw.content)) {
      const text = raw.content
        .filter((item) => item?.type === "text" || item?.text !== undefined)
        .map((item) => String(item.text ?? ""))
        .join("");
      if (text.length > 0) return { text, raw };
    }
  }
  const gemini = raw?.candidates?.[0]?.content?.parts;
  if (Array.isArray(gemini)) {
    const text = gemini.map((part) => String(part.text ?? "")).join("");
    if (text.length > 0) return { text, raw };
  }
  if (raw?.response !== undefined) return { text: String(raw.response), raw };
  if (raw?.result?.response !== undefined) return { text: String(raw.result.response), raw };
  if (raw?.result?.text !== undefined) return { text: String(raw.result.text), raw };
  if (raw?.text !== undefined) return { text: String(raw.text), raw };
  return { text: "", raw };
}

function aiEmbeddingOutput(raw) {
  if (Array.isArray(raw)) return { embeddings: raw, raw };
  if (Array.isArray(raw?.data)) {
    return {
      embeddings: raw.data.map((item) => item.embedding ?? item.values ?? item.vector ?? item),
      raw,
    };
  }
  if (Array.isArray(raw?.embedding)) return { embeddings: [raw.embedding], raw };
  if (Array.isArray(raw?.embeddings)) return { embeddings: raw.embeddings, raw };
  if (Array.isArray(raw?.result?.data)) {
    return {
      embeddings: raw.result.data.map((item) => item.embedding ?? item.values ?? item.vector ?? item),
      raw,
    };
  }
  if (Array.isArray(raw?.result?.embedding)) return { embeddings: [raw.result.embedding], raw };
  return { embeddings: [], raw };
}

async function aiProviderFetch(route, endpoint, target, body, options = {}) {
  if (endpoint.length === 0) throw new TypeError("AI endpoint is empty");
  const headers = Object.assign({ "content-type": "application/json" }, options.headers ?? {});
  if (route.authorization) headers.authorization = route.authorization;
  if (route.xApiKey) headers["x-api-key"] = route.xApiKey;
  if (route.anthropicVersion) headers["anthropic-version"] = route.anthropicVersion;
  const response = await outboundFetch(target, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) return await response.json();
  return await response.text();
}

function aiBinding(binding) {
  const route = binding.route ?? binding.provider ?? { kind: "http", url: binding.endpoint };
  const endpoint = String(route.url ?? binding.endpoint ?? "").replace(/\/$/, "");
  const publicEndpoint = route.kind === "workers_ai"
    ? endpoint.replace(/\/accounts\/[^/]+\/ai\/run$/, "/accounts/redacted/ai/run")
    : endpoint;
  const provider = ObjectFreeze({ kind: route.kind ?? "http", endpoint: publicEndpoint });
  return ObjectFreeze(Object.assign(ObjectCreate(Ai.prototype), {
    endpoint: publicEndpoint,
    provider,
    async run(model, input = {}, options = {}) {
      if (route.kind === "local") {
        return await core.ops.op_ai_run({
          command: String(route.command ?? ""),
          model: String(model),
          input,
          options,
        });
      }
      if (endpoint.length === 0) throw new TypeError("AI endpoint is empty");
      let target = `${endpoint}/run/${encodeURIComponent(String(model))}`;
      let body = input;
      if (route.kind === "open_ai") {
        target = `${endpoint}/responses`;
        body = Object.assign({ model: String(model) }, input);
      } else if (route.kind === "anthropic") {
        target = `${endpoint}/messages`;
        body = Object.assign({ model: String(model) }, input);
      } else if (route.kind === "gemini") {
        target = `${endpoint}/models/${encodeURIComponent(String(model))}:generateContent?key=${encodeURIComponent(String(route.apiKey ?? ""))}`;
      } else if (route.kind === "workers_ai") {
        target = `${endpoint}/${encodeURIComponent(String(model))}`;
      }
      return await aiProviderFetch(route, endpoint, target, body, options);
    },
    async embed(model, input, options = {}) {
      if (route.kind === "local") {
        const text = typeof input === "string" ? input : input?.text ?? input;
        return await this.run(model, { text }, options);
      }
      const body = aiEmbedInput(route, input, options);
      if (route.kind === "open_ai") {
        return aiEmbeddingOutput(await aiProviderFetch(route, endpoint, `${endpoint}/embeddings`, Object.assign({ model: String(model) }, body), options));
      }
      if (route.kind === "gemini") {
        const target = `${endpoint}/models/${encodeURIComponent(String(model))}:embedContent?key=${encodeURIComponent(String(route.apiKey ?? ""))}`;
        return aiEmbeddingOutput(await aiProviderFetch(route, endpoint, target, body, options));
      }
      return aiEmbeddingOutput(await this.run(model, body, options));
    },
    async generateText(model, prompt, options = {}) {
      if (route.kind === "local") {
        const input = typeof prompt === "string" ? { prompt } : prompt ?? {};
        return await this.run(model, input, options);
      }
      return aiTextOutput(await this.run(model, aiTextInput(route, model, prompt, options)));
    },
    async chat(model, messages = [], options = {}) {
      if (route.kind === "local") return await this.run(model, { messages: ArrayFrom(messages) }, options);
      return aiTextOutput(await this.run(model, aiChatInput(route, messages, options)));
    },
  }));
}
const vectorScore = (left, right) => {
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
    throw new TypeError("vector dimensions must match");
  }
  let dot = 0;
  let leftNorm = 0;
  let rightNorm = 0;
  for (let index = 0; index < left.length; index += 1) {
    const l = Number(left[index]);
    const r = Number(right[index]);
    dot += l * r;
    leftNorm += l * l;
    rightNorm += r * r;
  }
  if (leftNorm === 0 || rightNorm === 0) return 0;
  return dot / Math.sqrt(leftNorm * rightNorm);
};

const vectorScope = (name) => `__peren:vector:${name}`;
const vectorKey = (id) => String(id);
const vectorEncode = (record) => ArrayFrom(encodeStorageBytes(JSON.stringify(record)));
const vectorDecode = (bytes) => JSON.parse(decodeText(new Uint8Array(bytes)));

async function vectorLoad(scope, id) {
  const value = await core.ops.op_storage_get(scope, ArrayFrom(encodeStorageBytes(vectorKey(id))));
  return value === null ? undefined : vectorDecode(value);
}

async function vectorStore(scope, record) {
  await core.ops.op_storage_begin();
  let committed = false;
  try {
    await core.ops.op_storage_put(scope, ArrayFrom(encodeStorageBytes(vectorKey(record.id))), vectorEncode(record));
    await core.ops.op_storage_commit();
    committed = true;
  } finally {
    if (!committed) await core.ops.op_storage_rollback();
  }
}

async function vectorDelete(scope, id) {
  await core.ops.op_storage_begin();
  let committed = false;
  try {
    const deleted = await core.ops.op_storage_delete(scope, ArrayFrom(encodeStorageBytes(vectorKey(id))));
    await core.ops.op_storage_commit();
    committed = true;
    return deleted;
  } finally {
    if (!committed) await core.ops.op_storage_rollback();
  }
}

async function vectorRecords(scope) {
  const records = [];
  let cursor;
  do {
    const page = await core.ops.op_storage_list(scope, { cursor: cursor === undefined ? undefined : ArrayFrom(encodeStorageBytes(cursor)), limit: 1000 });
    for (const key of page.keys) {
      const record = await vectorLoad(scope, key.name);
      if (record !== undefined) records.push(record);
    }
    cursor = page.cursor === null ? undefined : decodeText(new Uint8Array(page.cursor));
  } while (cursor !== undefined);
  return records;
}

async function vectorFetch(route, path, body, headers = {}) {
  const url = `${String(route.url).replace(/\/$/, "")}${path}`;
  const response = await outboundFetch(url, {
    method: "POST",
    headers: Object.assign({ "content-type": "application/json" }, headers),
    body: JSON.stringify(body),
  });
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) return await response.json();
  return await response.text();
}

function vectorRouteHeaders(route) {
  if (route.authorization) return { authorization: route.authorization };
  if (route.apiKey) return route.kind === "qdrant" ? { "api-key": route.apiKey } : { "Api-Key": route.apiKey };
  return {};
}



function vectorFilterEntries(filter) {
  if (filter == null || typeof filter !== "object" || Array.isArray(filter)) return [];
  return Object.entries(filter).filter(([, value]) => value !== undefined);
}

function vectorMatchesFilter(metadata, filter) {
  const entries = vectorFilterEntries(filter);
  if (entries.length === 0) return true;
  const values = metadata ?? {};
  return entries.every(([key, expected]) => values?.[key] === expected);
}

function qdrantFilter(filter) {
  const must = vectorFilterEntries(filter).map(([key, value]) => ({ key, match: { value } }));
  return must.length === 0 ? undefined : { must };
}

function weaviateWhere(filter) {
  const operands = vectorFilterEntries(filter).map(([key, value]) => {
    const clause = { path: [key], operator: "Equal" };
    if (typeof value === "number") clause.valueNumber = value;
    else if (typeof value === "boolean") clause.valueBoolean = value;
    else clause.valueText = String(value);
    return clause;
  });
  if (operands.length === 0) return undefined;
  return operands.length === 1 ? operands[0] : { operator: "And", operands };
}

function weaviateSearchQuery(className, hasWhere) {
  const where = hasWhere ? ", where:$where" : "";
  return "query PerenVectorSearch($vector:[Float!]!,$limit:Int!" + (hasWhere ? ",$where:WhereInput" : "") + "){ Get { " + className + "(nearVector:{vector:$vector}, limit:$limit" + where + "){ _additional { id distance vector } } } }";
}

function pineconeMatches(result) {
  const matches = ArrayFrom(result.matches ?? [], (item) => {
    const match = { id: String(item.id), score: Number(item.score ?? 0) };
    if (item.values !== undefined) match.values = ArrayFrom(item.values, Number);
    if (item.metadata !== undefined) match.metadata = item.metadata;
    return match;
  });
  return ObjectFreeze({ matches, count: matches.length });
}

function pineconeRecords(result) {
  const vectors = result.vectors ?? {};
  return ArrayFrom(Object.keys(vectors), (id) => {
    const record = vectors[id] ?? {};
    return ObjectFreeze({
      id: String(record.id ?? id),
      values: record.values === undefined ? undefined : ArrayFrom(record.values, Number),
      metadata: record.metadata ?? null,
      namespace: record.namespace,
    });
  });
}

function weaviateObjects(result, className) {
  return ArrayFrom(result?.data?.Get?.[className] ?? [], (item) => {
    const additional = item._additional ?? {};
    const metadata = { ...item };
    delete metadata._additional;
    return { item, additional, metadata };
  });
}

function weaviateScore(additional) {
  if (additional.certainty !== undefined) return Number(additional.certainty);
  if (additional.score !== undefined) return Number(additional.score);
  if (additional.distance !== undefined) return 1 - Number(additional.distance);
  return 0;
}

function weaviateMatches(result, className) {
  const matches = weaviateObjects(result, className).map(({ additional, metadata }) => {
    const match = { id: String(additional.id), score: weaviateScore(additional) };
    if (additional.vector !== undefined) match.values = ArrayFrom(additional.vector, Number);
    if (Object.keys(metadata).length > 0) match.metadata = metadata;
    return match;
  });
  return ObjectFreeze({ matches, count: matches.length });
}

function weaviateRecords(result, className) {
  return weaviateObjects(result, className).map(({ additional, metadata }) => ObjectFreeze({
    id: String(additional.id),
    values: additional.vector === undefined ? undefined : ArrayFrom(additional.vector, Number),
    metadata: Object.keys(metadata).length === 0 ? null : metadata,
  }));
}

function vectorizeIndex(indexName, binding = {}) {
  const name = String(indexName ?? "default");
  const route = binding.route ?? { kind: "local", index: name };
  const scope = vectorScope(route.index ?? name);
  const provider = ObjectFreeze({
    kind: route.kind ?? "local",
    endpoint: route.url ?? route.index ?? name,
    collection: route.collection,
    index: route.index,
    className: route.className,
    namespace: route.namespace,
  });
  return ObjectFreeze(Object.assign(ObjectCreate(VectorizeIndex.prototype), {
    provider,
    async upsert(vectors = []) {
      if (route.kind === "qdrant") {
        const points = ArrayFrom(vectors, (vector) => ({ id: String(vector.id), vector: ArrayFrom(vector.values ?? [], Number), payload: vector.metadata ?? {} }));
        await vectorFetch(route, `/collections/${encodeURIComponent(route.collection)}/points?wait=true`, { points }, vectorRouteHeaders(route));
        return ObjectFreeze({ count: vectors.length });
      }
      if (route.kind === "pinecone") {
        await vectorFetch(route, "/vectors/upsert", { vectors, namespace: route.namespace }, vectorRouteHeaders(route));
        return ObjectFreeze({ count: vectors.length });
      }
      if (route.kind === "weaviate") {
        await vectorFetch(route, "/v1/batch/objects", { objects: ArrayFrom(vectors, (vector) => ({ class: route.className, id: String(vector.id), vector: ArrayFrom(vector.values ?? [], Number), properties: vector.metadata ?? {} })) }, vectorRouteHeaders(route));
        return ObjectFreeze({ count: vectors.length });
      }
      if (route.kind === "http") return await vectorFetch(route, "/upsert", { vectors }, vectorRouteHeaders(route));
      for (const vector of vectors) {
        const id = String(vector.id);
        const values = ArrayFrom(vector.values ?? [], Number);
        await vectorStore(scope, { id, values, metadata: vector.metadata ?? null, namespace: vector.namespace ?? "" });
      }
      return ObjectFreeze({ count: vectors.length });
    },
    async query(vector, options = {}) {
      const values = ArrayFrom(vector ?? [], Number);
      const topK = Math.max(1, Number(options.topK ?? options.top_k ?? 3));
      if (route.kind === "qdrant") {
        const body = { vector: values, limit: topK, with_payload: Boolean(options.returnMetadata), with_vector: Boolean(options.returnValues) };
        const filter = qdrantFilter(options.filter);
        if (filter !== undefined) body.filter = filter;
        const result = await vectorFetch(route, `/collections/${encodeURIComponent(route.collection)}/points/search`, body, vectorRouteHeaders(route));
        const matches = ArrayFrom(result.result ?? [], (item) => ({ id: String(item.id), score: Number(item.score ?? 0), metadata: item.payload, values: item.vector }));
        return ObjectFreeze({ matches, count: matches.length });
      }
      if (route.kind === "pinecone") {
        const body = { vector: values, topK, namespace: options.namespace ?? route.namespace, includeMetadata: Boolean(options.returnMetadata), includeValues: Boolean(options.returnValues) };
        if (options.filter !== undefined) body.filter = options.filter;
        const result = await vectorFetch(route, "/query", body, vectorRouteHeaders(route));
        return pineconeMatches(result);
      }
      if (route.kind === "weaviate") {
        const where = weaviateWhere(options.filter);
        const variables = { vector: values, limit: topK };
        if (where !== undefined) variables.where = where;
        const result = await vectorFetch(route, "/v1/graphql", { query: weaviateSearchQuery(route.className, where !== undefined), variables }, vectorRouteHeaders(route));
        return weaviateMatches(result, route.className);
      }
      if (route.kind === "http") return await vectorFetch(route, "/query", { vector: values, options }, vectorRouteHeaders(route));
      const namespace = options.namespace;
      const matches = ArrayFrom(await vectorRecords(scope))
        .filter((record) => namespace === undefined || record.namespace === namespace)
        .filter((record) => vectorMatchesFilter(record.metadata, options.filter))
        .map((record) => {
          const match = { id: record.id, score: vectorScore(values, record.values) };
          if (options.returnValues) match.values = ArrayFrom(record.values);
          if (options.returnMetadata) match.metadata = record.metadata;
          return match;
        })
        .sort((left, right) => right.score - left.score)
        .slice(0, topK);
      return ObjectFreeze({ matches, count: matches.length });
    },
    async getByIds(ids = []) {
      if (route.kind === "qdrant") {
        const result = await vectorFetch(route, `/collections/${encodeURIComponent(route.collection)}/points`, { ids: ArrayFrom(ids, String), with_payload: true, with_vector: true }, vectorRouteHeaders(route));
        return ArrayFrom(result.result ?? [], (record) => ObjectFreeze({ id: String(record.id), values: record.vector, metadata: record.payload }));
      }
      if (route.kind === "pinecone") {
        const result = await vectorFetch(route, "/vectors/fetch", { ids: ArrayFrom(ids, String), namespace: route.namespace }, vectorRouteHeaders(route));
        return pineconeRecords(result);
      }
      if (route.kind === "weaviate") {
        const result = await vectorFetch(route, "/v1/graphql", { query: "query PerenVectorFetch($ids:[String!]!){ Get { " + route.className + "(where:{path:[\"id\"],operator:ContainsAny,valueText:$ids}){ _additional { id vector } } } }", variables: { ids: ArrayFrom(ids, String) } }, vectorRouteHeaders(route));
        return weaviateRecords(result, route.className);
      }
      if (route.kind === "http") return await vectorFetch(route, "/get", { ids: ArrayFrom(ids, String) }, vectorRouteHeaders(route));
      const records = await PromiseAll(ArrayFrom(ids, async (id) => vectorLoad(scope, id)));
      return records.filter((record) => record !== undefined)
        .map((record) => ObjectFreeze({ id: record.id, values: ArrayFrom(record.values), metadata: record.metadata, namespace: record.namespace }));
    },
    async deleteByIds(ids = []) {
      if (route.kind === "qdrant") {
        await vectorFetch(route, `/collections/${encodeURIComponent(route.collection)}/points/delete?wait=true`, { points: ArrayFrom(ids, String) }, vectorRouteHeaders(route));
        return ObjectFreeze({ count: ids.length });
      }
      if (route.kind === "pinecone") {
        await vectorFetch(route, "/vectors/delete", { ids: ArrayFrom(ids, String), namespace: route.namespace }, vectorRouteHeaders(route));
        return ObjectFreeze({ count: ids.length });
      }
      if (route.kind === "weaviate") {
        await vectorFetch(route, "/v1/batch/objects/delete", { match: { class: route.className, where: { path: ["id"], operator: "ContainsAny", valueTextArray: ArrayFrom(ids, String) } } }, vectorRouteHeaders(route));
        return ObjectFreeze({ count: ids.length });
      }
      if (route.kind === "http") return await vectorFetch(route, "/delete", { ids: ArrayFrom(ids, String) }, vectorRouteHeaders(route));
      let count = 0;
      for (const id of ids) if (await vectorDelete(scope, id)) count += 1;
      return ObjectFreeze({ count });
    },
  }));
}
const analyticsDatasets = new Map();
function analyticsEngineDataset(dataset, provider = ObjectFreeze({ kind: "buffer", dataset: String(dataset ?? "default") })) {
  const name = String(dataset ?? "default");
  if (!analyticsDatasets.has(name)) analyticsDatasets.set(name, []);
  return ObjectFreeze(Object.assign(ObjectCreate(AnalyticsEngineDataset.prototype), {
    provider,
    writeDataPoint(point = {}) {
      const rows = analyticsDatasets.get(name);
      rows.push({
        blobs: ArrayFrom(point.blobs ?? []),
        doubles: ArrayFrom(point.doubles ?? []),
        indexes: ArrayFrom(point.indexes ?? []),
        timestamp: Date.now(),
      });
      if (rows.length > 1024) rows.splice(0, rows.length - 1024);
    },
  }));
}

const redactConnectionString = (value) => {
  const text = String(value ?? "");
  try {
    const url = new URL(text);
    url.username = url.username.length === 0 ? "" : "redacted";
    url.password = url.password.length === 0 ? "" : "redacted";
    return url.toString();
  } catch {
    return text.replace(/:\/\/([^:@/]+):([^@/]+)@/, "://redacted:redacted@");
  }
};


function outboundBinding(binding) {
  const allowedHosts = ObjectFreeze(ArrayFrom(binding.allowedHosts ?? [], String));
  const allowed = new Set(allowedHosts);
  return ObjectFreeze({
    allowedHosts,
    async fetch(input, init = {}) {
      const request = input instanceof Request ? input : new Request(input, init);
      const host = new URL(request.url).host;
      if (!allowed.has(host)) throw new TypeError(`outbound host ${host} is not allowed by this binding`);
      return await outboundFetch(request);
    },
  });
}

function awsSigv4Binding(name, binding) {
  const allowedHosts = ObjectFreeze(ArrayFrom(binding.allowedHosts ?? [], String));
  const allowed = new Set(allowedHosts);
  const region = String(binding.region ?? "");
  const service = String(binding.service ?? "");
  return ObjectFreeze({
    allowedHosts,
    region,
    service,
    async fetch(input, init = {}) {
      const request = input instanceof Request ? input : new Request(input, init);
      const host = new URL(request.url).host;
      if (!allowed.has(host)) throw new TypeError(`AWS SigV4 host ${host} is not allowed by this binding`);
      const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
      const response = await core.ops.op_aws_sigv4_fetch({
        binding: name,
        region,
        service,
        allowedHosts,
        request: {
          method: request.method,
          url: request.url,
          headers: ArrayFrom(request.headers.entries()),
          body,
          mtls: null,
        },
      });
      return hostResponse(response);
    },
  });
}

function hyperdrive(binding) {
  const connectionString = redactConnectionString(binding.connectionString ?? "");
  const value = ObjectCreate(Hyperdrive.prototype);
  ObjectDefineProperties(value, {
    connectionString: { value: connectionString, enumerable: true },
    host: { value: connectionString, enumerable: true },
    provider: { value: ObjectFreeze({ kind: "pgcat" }), enumerable: true },
    cachingDisabled: { value: Boolean(binding.cachingDisabled), enumerable: true },
    maxAge: { value: Number(binding.maxAge ?? 0), enumerable: true },
    staleWhileRevalidate: { value: Number(binding.staleWhileRevalidate ?? 0), enumerable: true },
    poolMaxConnections: { value: Number(binding.poolMaxConnections ?? 1), enumerable: true },
  });
  return ObjectFreeze(value);
}

const rateLimiterWindows = new Map();
function rateLimiter(name, limit, periodSecs, provider = ObjectFreeze({ kind: "memory" })) {
  const capacity = Math.max(1, Number(limit) || 1);
  const periodMs = Math.max(1, Number(periodSecs) || 1) * 1000;
  return ObjectFreeze(Object.assign(ObjectCreate(RateLimiter.prototype), {
    provider,
    async limit(options = {}) {
      const key = String(options.key ?? "default");
      const now = Date.now();
      const id = `${name}:${key}`;
      let window = rateLimiterWindows.get(id);
      if (window == null || now >= window.reset) {
        window = { count: 0, reset: now + periodMs };
      }
      window.count += 1;
      rateLimiterWindows.set(id, window);
      const remaining = Math.max(0, capacity - window.count);
      return ObjectFreeze({
        success: window.count <= capacity,
        limit: capacity,
        remaining,
        reset: Math.ceil(window.reset / 1000),
      });
    },
  }));
}

const serviceBinding = (service) => {
  const binding = {
    async fetch(input, init = {}) {
      const request = new Request(input, init);
      const body = request.body === null ? [] : ArrayFrom(new Uint8Array(await request.arrayBuffer()));
      const response = await core.ops.op_service_fetch({
        service,
        request: {
          method: request.method,
          url: request.url,
          headers: ArrayFrom(request.headers.entries()),
          body,
        },
      });
      return hostResponse(response);
    },
  };
  return rpcProxy(binding, async (method, args) => {
    const response = await binding.fetch(rpcRequest(method, args));
    return await readRpcResponse(response);
  });
};

globalThis.DurableObject = DurableObject;
globalThis.DurableObjectNamespace = DurableObjectNamespace;
globalThis.DurableObjectState = DurableObjectState;
globalThis.RpcTarget = RpcTarget;
globalThis.WebSocketRequestResponsePair = WebSocketRequestResponsePair;
globalThis.WorkerEntrypoint = WorkerEntrypoint;

class Workflow {
  constructor(binding, id) {
    this.binding = binding;
    this.id = id;
  }
  async status() {
    return await workflowState(this.binding, this.id) ?? { id: this.id, status: "unknown" };
  }
  async terminate(reason) {
    return await writeWorkflowState(this.binding, this.id, "terminated", reason);
  }
  async restart() {
    return await writeWorkflowState(this.binding, this.id, "running", undefined);
  }
}

const workflowKey = (binding, id) => `${binding}/${id}.json`;
const workflowState = async (binding, id) => {
  const bytes = await storage.get(workflowKey(binding, id), { scope: "workflows" });
  return bytes === undefined ? undefined : JSON.parse(decodeText(bytes));
};
const writeWorkflowState = async (binding, id, status, reason) => {
  const state = { id, status, reason: reason == null ? undefined : String(reason), updatedAt: Date.now() };
  await storage.transaction(async (txn) => {
    await txn.put(workflowKey(binding, id), JSON.stringify(state), { scope: "workflows" });
  });
  return state;
};
const workflowId = () => `workflow-${Date.now()}-${Math.random().toString(16).slice(2)}`;

const workflowBinding = (binding, provider = ObjectFreeze({ kind: "native" })) => ObjectFreeze({
  provider,
  async create(options = {}) {
    const id = options.id == null ? workflowId() : String(options.id);
    await writeWorkflowState(binding, id, "running", undefined);
    return new Workflow(binding, id);
  },
  get(id) {
    return new Workflow(binding, String(id));
  },
});

globalThis.Workflow = Workflow;

const QUEUE_MAX_MESSAGE_BYTES = 128000;
const QUEUE_MAX_BATCH_MESSAGES = 100;
const QUEUE_MAX_BATCH_BYTES = 256000;
const QUEUE_MAX_DELAY_SECONDS = 86400;

const queueBody = (body) => {
  if (body instanceof Uint8Array) return ArrayFrom(body);
  if (body instanceof ArrayBuffer) return ArrayFrom(new Uint8Array(body));
  if (typeof body === "string") return ArrayFrom(encodeStorageBytes(body));
  return ArrayFrom(encodeStorageBytes(JSON.stringify(body)));
};

const queueDelay = (value) => {
  if (value == null) return null;
  const delay = Number(value);
  if (!Number.isFinite(delay) || delay < 0 || delay > QUEUE_MAX_DELAY_SECONDS) {
    throw new RangeError(`Queue delaySeconds must be between 0 and ${QUEUE_MAX_DELAY_SECONDS}`);
  }
  return delay;
};

const queueMessage = (queue, body, options = {}) => {
  const bytes = queueBody(body);
  if (bytes.length > QUEUE_MAX_MESSAGE_BYTES) {
    throw new RangeError(`Queue message body exceeds ${QUEUE_MAX_MESSAGE_BYTES} bytes`);
  }
  return {
    queue,
    body: bytes,
    contentType: options.contentType == null ? null : String(options.contentType),
    delaySeconds: queueDelay(options.delaySeconds),
    dedupId: options.dedupId == null ? null : String(options.dedupId),
  };
};

class Queue {
  constructor(queue, provider = ObjectFreeze({ kind: "memory" })) {
    this.queue = String(queue);
    this.provider = provider;
  }
  async send(body, options = {}) {
    await core.ops.op_queue_send(queueMessage(this.queue, body, options));
  }
  async sendBatch(messages) {
    const batch = ArrayFrom(messages ?? [], (message) => queueMessage(this.queue, message.body, message));
    if (batch.length > QUEUE_MAX_BATCH_MESSAGES) {
      throw new RangeError(`Queue sendBatch accepts at most ${QUEUE_MAX_BATCH_MESSAGES} messages`);
    }
    const bytes = batch.reduce((sum, message) => sum + message.body.length, 0);
    if (bytes > QUEUE_MAX_BATCH_BYTES) {
      throw new RangeError(`Queue sendBatch body total exceeds ${QUEUE_MAX_BATCH_BYTES} bytes`);
    }
    for (const message of batch) {
      await core.ops.op_queue_send(message);
    }
  }
}

globalThis.Queue = Queue;

const queueProducer = (queue, provider = ObjectFreeze({ kind: "memory" })) => ObjectFreeze(new Queue(queue, provider));



globalThis.__perenQueueDispositions = [];
const queueDisposition = (id, outcome, options = {}) => ({
  id: String(id),
  outcome,
  delaySeconds: options?.delaySeconds == null ? null : Number(options.delaySeconds),
});
globalThis.__perenPrepareQueue = (event) => {
  globalThis.__perenQueueDispositions = [];
  const messages = event.messages.map((message) => {
    const id = String(message.id);
    return Object.freeze({
      ...message,
      ack() {
        globalThis.__perenQueueDispositions.push(queueDisposition(id, "ack"));
      },
      retry(options = {}) {
        globalThis.__perenQueueDispositions.push(queueDisposition(id, "retry", options));
      },
    });
  });
  return Object.freeze({
    ...event,
    metrics: Object.freeze(event.metrics ?? { ready: 0, delayed: 0, leased: messages.length, oldestReadyTimestamp: null }),
    messages,
    ackAll() {
      for (const message of messages) globalThis.__perenQueueDispositions.push(queueDisposition(message.id, "ack"));
    },
    retryAll(options = {}) {
      for (const message of messages) globalThis.__perenQueueDispositions.push(queueDisposition(message.id, "retry", options));
    },
  });
};

globalThis.__perenDispatchListener = dispatchListener;

globalThis.__perenWaitUntil = [];
globalThis.__perenResetWaitUntil = () => {
  globalThis.__perenWaitUntil = [];
};
globalThis.__perenRegisterWaitUntil = (promise) => {
  const tracked = PromiseResolve(promise);
  globalThis.__perenWaitUntil.push(tracked);
  return tracked;
};
globalThis.__perenDrainWaitUntil = async () => {
  while (globalThis.__perenWaitUntil.length > 0) {
    const pending = globalThis.__perenWaitUntil;
    globalThis.__perenWaitUntil = [];
    await PromiseAll(pending);
  }
};

globalThis.__perenCreateFetchEvent = (request) => {
  let responsePromise;
  return {
    request,
    respondWith(response) {
      responsePromise = PromiseResolve(response);
    },
    waitUntil(promise) {
      return globalThis.__perenRegisterWaitUntil(promise);
    },
    async __perenResponse() {
      return responsePromise;
    },
  };
};

globalThis.__perenCreateExtendableEvent = (name, fields) => ({
  type: name,
  ...fields,
  waitUntil(promise) {
    return globalThis.__perenRegisterWaitUntil(promise);
  },
});

const workflowStepKey = (instance, sequence) => `${String(instance)}/steps/${sequence}.json`;

globalThis.__perenCreateWorkflowStep = (event) => {
  let sequence = 0;
  const read = async (name, kind) => {
    const stepName = String(name);
    const key = workflowStepKey(event.instance, sequence);
    const existing = await storage.get(key, { scope: "workflowSteps" });
    if (existing === undefined) return { stepName, key };
    const entry = JSON.parse(decodeText(existing));
    const entryKind = entry.kind ?? "step";
    if (entry.name !== stepName || entryKind !== kind) {
      throw new Error(`workflow replay mismatch at step ${sequence}: journal has ${entry.name}, code called ${stepName}`);
    }
    sequence += 1;
    return { entry };
  };
  return ObjectFreeze({
    async do(name, optionsOrCallback, maybeCallback) {
      const callback = typeof optionsOrCallback === "function" ? optionsOrCallback : maybeCallback;
      if (typeof callback !== "function") throw new TypeError("workflow step requires a callback");
      const loaded = await read(name, "step");
      if (loaded.entry !== undefined) return loaded.entry.value;
      const value = await callback();
      await storage.transaction(async (txn) => {
        await txn.put(loaded.key, JSON.stringify({ name: loaded.stepName, kind: "step", value }), { scope: "workflowSteps" });
      });
      sequence += 1;
      return value;
    },
    async sleep(name, delayMs = 0) {
      const loaded = await read(name, "sleep");
      if (loaded.entry !== undefined) return loaded.entry.value;
      const delay = Math.max(0, Number(delayMs) || 0);
      const value = { dueAt: Date.now() + delay, delayMs: delay };
      if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
      await storage.transaction(async (txn) => {
        await txn.put(loaded.key, JSON.stringify({ name: loaded.stepName, kind: "sleep", value }), { scope: "workflowSteps" });
      });
      sequence += 1;
      return value;
    },
  });
};


const cacheRequest = (request) => request instanceof Request ? request : new Request(String(request));
const cacheKey = (request, options = {}) => {
  const req = cacheRequest(request);
  const method = options.ignoreMethod === true ? "GET" : req.method.toUpperCase();
  const url = new URL(req.url);
  if (options.ignoreSearch === true) url.search = "";
  return `${method} ${url.href}`;
};
const validateCachePut = (request, response) => {
  const req = cacheRequest(request);
  if (req.method.toUpperCase() !== "GET") {
    throw new TypeError("Cache.put request method must be GET");
  }
  if (response.status === 206) {
    throw new TypeError("Cache.put does not accept partial responses");
  }
  if (response.headers.has("vary") && response.headers.get("vary").split(",").some((value) => value.trim() === "*")) {
    throw new TypeError("Cache.put does not accept Vary: * responses");
  }
  return req;
};

const CACHE_INDEX_HEADER = "x-peren-cache-index";
const CACHE_INDEX_SUFFIX = "\nperen-cache-variants";

const cacheIndexKey = (key) => `${key}${CACHE_INDEX_SUFFIX}`;
const cacheEntryBody = (entry) => new Uint8Array(entry.body);
const cacheEntryText = (entry) => new TextDecoder().decode(cacheEntryBody(entry));
const cacheIndexEntry = (variants) => ({
  cache: "",
  key: "",
  status: 204,
  headers: [[CACHE_INDEX_HEADER, "1"]],
  body: ArrayFrom(new TextEncoder().encode(JSON.stringify({ variants }))),
});

const cacheVaryNames = (response) => {
  const value = response.headers.get("vary");
  if (value === null) return [];
  return value
    .split(",")
    .map((name) => name.trim().toLowerCase())
    .filter((name, index, names) => name.length > 0 && names.indexOf(name) === index)
    .sort();
};

const cacheVariantKey = (base, request, vary) => {
  const headers = vary.map((name) => [name, request.headers.get(name) ?? ""]);
  return `${base}\nvary:${JSON.stringify(headers)}`;
};

const loadCacheIndex = async (cache, key) => {
  const entry = await core.ops.op_cache_match({ cache, key: cacheIndexKey(key) });
  if (entry === null) return [];
  if (!entry.headers.some(([name, value]) => name.toLowerCase() === CACHE_INDEX_HEADER && value === "1")) return [];
  try {
    const parsed = JSON.parse(cacheEntryText(entry));
    return Array.isArray(parsed.variants) ? parsed.variants : [];
  } catch {
    return [];
  }
};

const saveCacheIndex = async (cache, key, variants) => {
  const entry = cacheIndexEntry(variants);
  entry.cache = cache;
  entry.key = cacheIndexKey(key);
  await core.ops.op_cache_put(entry);
};

const matchingCacheVariant = (request, base, variants) => {
  for (const variant of variants) {
    if (!Array.isArray(variant?.vary)) continue;
    const key = cacheVariantKey(base, request, variant.vary);
    if (key === variant.key) return variant;
  }
  return undefined;
};

let cacheProvider = ObjectFreeze({ kind: "memory" });

class Cache {
  #name;

  constructor(name = "default") {
    const value = String(name);
    if (value.length === 0) throw new TypeError("cache name cannot be empty");
    this.#name = value;
  }

  get provider() {
    return cacheProvider;
  }

  async match(request, options = {}) {
    const req = cacheRequest(request);
    const key = cacheKey(req, options);
    const variants = await loadCacheIndex(this.#name, key);
    let entry = null;
    if (variants.length > 0) {
      const variant = options.ignoreVary === true ? variants[0] : matchingCacheVariant(req, key, variants);
      if (variant !== undefined) {
        entry = await core.ops.op_cache_match({ cache: this.#name, key: variant.key });
      }
    }
    if (entry === null) {
      entry = await core.ops.op_cache_match({ cache: this.#name, key });
    }
    if (entry === null) return undefined;
    return new Response(new Uint8Array(entry.body), { status: entry.status, headers: entry.headers });
  }

  async put(request, response) {
    const res = response instanceof Response ? response : new Response(response);
    const req = validateCachePut(request, res);
    const key = cacheKey(req);
    const vary = cacheVaryNames(res);
    const storedKey = vary.length === 0 ? key : cacheVariantKey(key, req, vary);
    await core.ops.op_cache_put({
      cache: this.#name,
      key: storedKey,
      status: res.status,
      headers: ArrayFrom(res.headers.entries()),
      body: ArrayFrom(new Uint8Array(await res.clone().arrayBuffer())),
    });
    if (vary.length > 0) {
      const variants = (await loadCacheIndex(this.#name, key)).filter((variant) => variant.key !== storedKey);
      variants.push({ vary, key: storedKey });
      await saveCacheIndex(this.#name, key, variants);
    }
  }

  async delete(request, options = {}) {
    const req = cacheRequest(request);
    const key = cacheKey(req, options);
    const variants = await loadCacheIndex(this.#name, key);
    if (variants.length === 0) {
      return await core.ops.op_cache_delete({ cache: this.#name, key });
    }
    if (options.ignoreVary === true) {
      const deleted = await PromiseAll(variants.map((variant) => core.ops.op_cache_delete({ cache: this.#name, key: variant.key })));
      await core.ops.op_cache_delete({ cache: this.#name, key: cacheIndexKey(key) });
      const direct = await core.ops.op_cache_delete({ cache: this.#name, key });
      return direct || deleted.some(Boolean);
    }
    const variant = matchingCacheVariant(req, key, variants);
    if (variant === undefined) return false;
    const deleted = await core.ops.op_cache_delete({ cache: this.#name, key: variant.key });
    await saveCacheIndex(this.#name, key, variants.filter((entry) => entry.key !== variant.key));
    return deleted;
  }
}

class CacheStorage {
  #default;

  constructor() {
    this.#default = new Cache("default");
  }

  get default() {
    return this.#default;
  }

  async open(name) {
    return new Cache(name);
  }
}

globalThis.Cache = Cache;
globalThis.CacheStorage = CacheStorage;
globalThis.caches = new CacheStorage();
const cacheMetadata = (raw) => {
  const value = typeof raw === "string" ? JSON.parse(raw) : raw ?? { kind: "memory" };
  if (value.kind === "bucket") return ObjectFreeze({ kind: "bucket", endpoint: value.endpoint, bucket: value.bucket, prefix: value.prefix ?? "" });
  if (value.kind === "redis") return ObjectFreeze({ kind: "redis" });
  if (value.kind === "kv") return ObjectFreeze({ kind: "kv", namespace: value.namespace });
  return ObjectFreeze({ kind: value.kind ?? "memory" });
};

globalThis.__perenHydrateEnv = (env) => {
  cacheProvider = cacheMetadata(env.__perenCache);
  delete env.__perenCache;
  const bindings = typeof env.__perenBindings === "string" ? JSON.parse(env.__perenBindings) : env.__perenBindings ?? {};
  for (const [name, binding] of Object.entries(bindings)) {
    if (binding.type === "service") {
      env[name] = serviceBinding(binding.service);
    }
    if (binding.type === "kv") {
      env[name] = kvNamespace(binding.scope, ObjectFreeze(binding.provider ?? { kind: "native" }));
    }
    if (binding.type === "d1") {
      env[name] = d1Database(name, ObjectFreeze(binding.provider ?? { kind: "native_sqlite" }));
    }
    if (binding.type === "queue") {
      env[name] = queueProducer(binding.queue, ObjectFreeze(binding.provider ?? { kind: "memory" }));
    }
    if (binding.type === "workflow") {
      env[name] = workflowBinding(name, ObjectFreeze(binding.provider ?? { kind: "native" }));
    }
    if (binding.type === "loader") {
      env[name] = loaderBinding();
    }
    if (binding.type === "container") {
      env[name] = containerBinding(binding);
    }
    if (binding.type === "images") {
      env[name] = imagesBinding(binding);
    }
    if (binding.type === "vectorize") {
      env[name] = vectorizeIndex(binding.index, binding);
    }
    if (binding.type === "rate_limiter") {
      env[name] = rateLimiter(name, binding.limit, binding.periodSecs, ObjectFreeze(binding.provider ?? { kind: "memory" }));
    }
    if (binding.type === "analytics_engine") {
      env[name] = analyticsEngineDataset(binding.dataset, ObjectFreeze(binding.provider ?? { kind: "buffer", dataset: binding.dataset ?? "default" }));
    }
    if (binding.type === "outbound") {
      env[name] = outboundBinding(binding);
    }
    if (binding.type === "aws_sigv4") {
      env[name] = awsSigv4Binding(name, binding);
    }
    if (binding.type === "ai") {
      env[name] = aiBinding(binding);
    }
    if (binding.type === "hyperdrive") {
      env[name] = hyperdrive(binding);
    }
    if (binding.type === "durable_object_namespace") {
      env[name] = durableObjectNamespace(name, binding.className);
    }
    if (binding.type === "dispatcher") {
      env[name] = dispatchNamespace(binding);
    }
    if (binding.type === "mtls_certificate") {
      env[name] = mtlsCertificate(name);
    }
    if (binding.type === "r2") {
      env[name] = r2Bucket(binding.bucket, binding.prefix ?? "", ObjectFreeze(binding.provider ?? { kind: "memory" }));
    }
  }
  delete env.__perenBindings;
  return Object.freeze(env);
};

ObjectDefineProperties(globalThis, {
  Ai: core.propNonEnumerable(Ai),
  Container: core.propNonEnumerable(Container),
  DispatchNamespace: core.propNonEnumerable(DispatchNamespace),
  AnalyticsEngineDataset: core.propNonEnumerable(AnalyticsEngineDataset),
  Hyperdrive: core.propNonEnumerable(Hyperdrive),
  ImagesBinding: core.propNonEnumerable(ImagesBinding),
  Loader: core.propNonEnumerable(Loader),
  RateLimiter: core.propNonEnumerable(RateLimiter),
  VectorizeIndex: core.propNonEnumerable(VectorizeIndex),
  Peren: core.propNonEnumerable(Object.freeze({
    fetch: outboundFetch,
    storage: Object.freeze(storage),
  })),
});
