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
