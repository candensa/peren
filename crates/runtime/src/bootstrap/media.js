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
  async get(specifier = undefined) {
    const module = await loaderImport(specifier, true);
    return ObjectFreeze({
      getEntrypoint(exportName = "default") {
        return loaderExport(module, exportName, "entrypoint");
      },
      getDurableObjectClass(className) {
        return loaderClass(module, className);
      },
    });
  },
  async getEntrypoint(specifier = undefined, exportName = "default") {
    const module = await loaderImport(specifier, true);
    return loaderExport(module, exportName, "entrypoint");
  },
  async getDurableObjectClass(className, specifier = undefined) {
    const module = await loaderImport(specifier, true);
    return loaderClass(module, className);
  },
  async import(specifier) {
    return await loaderImport(specifier);
  },
}));

const loaderSpecifier = (specifier, relativeOnly = false) => {
  const base = globalThis.__perenEntryModuleSpecifier;
  if (typeof base !== "string" || base.length === 0) {
    throw new TypeError("Worker entry module is unavailable for dynamic import");
  }
  if (specifier === undefined || specifier === null) return base;
  const value = String(specifier);
  if (value.length === 0) throw new TypeError("module specifier cannot be empty");
  if (/^(?:https?:|node:|npm:|jsr:)/.test(value)) {
    throw new TypeError(`unsupported dynamic module specifier ${value}`);
  }
  if (relativeOnly && !/^(?:\.\/|\.\.\/)/.test(value)) {
    throw new TypeError("loader lookup specifier must be relative");
  }
  return new URL(value, base).href;
};

const loaderImport = async (specifier, relativeOnly = false) => await import(loaderSpecifier(specifier, relativeOnly));

const loaderExport = (module, exportName, kind) => {
  const key = String(exportName);
  const value = module?.[key];
  if (value === undefined) throw new TypeError(`module does not export ${kind} ${key}`);
  if (kind === "entrypoint" && value != null && typeof value.fetch === "function") {
    return ObjectFreeze(Object.assign(ObjectCreate(Object.getPrototypeOf(value)), value, {
      async fetch(input = undefined, init = {}) {
        if (input === undefined) return await value.fetch.call(value);
        const request = new Request(input, init);
        throwIfAborted(request.signal);
        return await abortable(value.fetch.call(value, request, init), request.signal);
      },
    }));
  }
  return value;
};

const loaderClass = (module, className) => {
  const value = loaderExport(module, className, "Durable Object class");
  if (typeof value !== "function") throw new TypeError(`Durable Object export ${String(className)} must be a class or constructor`);
  try {
    Reflect.construct(Object, [], value);
  } catch {
    throw new TypeError(`Durable Object export ${String(className)} must be a class or constructor`);
  }
  return value;
};
