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
