import { describe, expect, it } from "vitest";
import { render } from "./template.js";

const source = render({
  configPath: "/tmp/peren.toml",
  configCwd: "/tmp",
  perenBin: "peren",
  wranglerPaths: [],
  readyTimeoutMs: 1000,
  socketName: "public",
  service: "api",
  cache: { kind: "memory" },
  bindings: {
    OUT: { type: "outbound", allowedHosts: ["api.example.com"] },
    LEGACY: { type: "outbound", allowed_hosts: ["legacy.example.com"] },
    AI: { type: "ai", provider: { kind: "open_ai", url: "https://api.openai.com/v1", authorization: "Bearer secret", apiKey: "secret" } },
    VECTOR: { type: "vectorize", provider: { kind: "qdrant", url: "https://qdrant.example.com", apiKey: "secret", collection: "docs" } },
  },
});

describe("render", () => {
  it("exposes outbound bindings and clear experimental container controls", () => {
    expect(source).toContain('function outbound(name, binding)');
    expect(source).toContain('function container(binding)');
    expect(source).toContain('get(name)');
    expect(source).toContain('Container." + operation + " requires a managed sandbox provider');
    expect(source).toContain('provider: visible');
    expect(source).toContain('async readFile(_path)');
    expect(source).toContain('kind === "outbound"');
    expect(source).toContain('binding?.allowedHosts ?? binding?.allowed_hosts');
    expect(source).toContain('kind === "service"');
    expect(source).toContain('kind === "dispatcher"');
    expect(source).toContain('kind === "workflow"');
    expect(source).not.toContain('not exposed by the test control surface');
  });

  it("exposes a local Cache API with provider metadata", () => {
    expect(source).toContain("export const caches = Object.freeze");
    expect(source).toContain("provider: CACHE_PROVIDER");
    expect(source).toContain("globalThis.caches ??= caches");
    expect(source).toContain("function cacheVaryNames(response)");
    expect(source).toContain("function matchingCacheVariant(request, base, variants)");
    expect(source).toContain("options.ignoreVary");
  });

  it("preserves public provider metadata for first-class bindings", () => {
    expect(source).toContain('provider: provider(name, { kind: "native" })');
    expect(source).toContain('compareAndSet: (key, expected, value, opts)');
    expect(source).toContain('provider: provider(name, { kind: "native_sqlite" })');
    expect(source).toContain('provider: provider(name, { kind: "memory" })');
    expect(source).toContain("customMetadata: Object.freeze(object.customMetadata ?? {})");
    expect(source).toContain('provider: provider(name, { kind: "local" })');
    expect(source).toContain('provider: provider(name, { kind: "http" })');
    expect(source).toContain('provider: Object.freeze({ kind: "pgcat" })');
    expect(source).toContain('provider: provider(name, { kind: "native" })');
    expect(source).toContain('function aiTextOutput(raw)');
    expect(source).toContain('function aiEmbeddingOutput(raw)');
    expect(source).toContain('generateText: async (model, prompt, opts)');
    expect(source).toContain('function publicProvider(provider)');
    expect(source).toContain('visible[key === "url" ? "endpoint" : key] = value;');
    expect(source).toContain('PROVIDER_SECRET_KEYS.has(key)');
  });

  it("routes HTTP Images providers through fetch without exposing credentials", () => {
    expect(source).toContain('function images(name, binding)');
    expect(source).toContain('const visible = publicProvider(provider) ?? Object.freeze({ kind: "local" });');
    expect(source).toContain('if (provider.authorization) headers.authorization = provider.authorization;');
    expect(source).toContain('String(provider.url).replace(/\\/$/, "") + "/transform"');
    expect(source).toContain('body: JSON.stringify({ source, transforms })');
    expect(source).not.toContain('HTTP image provider is only available through worker fetch tests');
  });
});
