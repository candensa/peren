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

