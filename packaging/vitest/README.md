# @peren/vitest-plugin

Run Peren worker tests from Vitest against a real `peren test-server` instead of a mocked runtime.

The plugin starts a local Peren test server, loads your worker configuration, and exposes bindings through `cloudflare:test`. It is designed for application tests that need real worker dispatch, queue delivery, KV/cache/object-store bindings, service bindings, and dispatcher namespaces.

## Install

```sh
npm install -D @peren/vitest-plugin vitest vite
```

## Configure Vitest

```ts
// vitest.config.ts
import { defineConfig } from "vitest/config";
import { peren } from "@peren/vitest-plugin";

export default defineConfig({
  plugins: [
    peren({
      config: "peren.toml",
      script: "src/worker.js",
    }),
  ],
  test: {
    environment: "node",
  },
});
```

## Use worker bindings in tests

```ts
import { describe, expect, it } from "vitest";
import { env, SELF } from "cloudflare:test";

describe("worker", () => {
  it("serves requests through Peren", async () => {
    const response = await SELF.fetch("https://example.test/");

    expect(response.status).toBe(200);
  });

  it("uses configured bindings", async () => {
    await env.CACHE.put("hello", "world");

    await expect(env.CACHE.get("hello")).resolves.toBe("world");
  });
});
```

`peren:test` exports the same helpers as `cloudflare:test` for projects that prefer an explicit Peren import path.

## Requirements

- Node.js 20 or newer.
- A `peren` binary available on `PATH`, or pass `binary` in the plugin options.
- A worker script and `peren.toml` with the bindings your tests need.
