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

