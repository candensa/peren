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
