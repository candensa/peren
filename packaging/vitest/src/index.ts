/// <reference path="./modules.d.ts" />
import type { Plugin } from "vite";
import { resolveOptions, type PerenTestOptions } from "./options.js";
import { render } from "./template.js";

export type { PerenTestOptions } from "./options.js";
export type * from "./types.js";

const primary = "cloudflare:test";
const fallback = "peren:test";
const prefix = "\0peren-vitest:";

export function perenTest(options: PerenTestOptions): Plugin {
  let modules: Map<string, string> | undefined;

  return {
    name: "@peren/vitest-plugin",
    enforce: "pre",
    configResolved(config) {
      const source = render(resolveOptions(options, config.root));
      modules = new Map([
        [prefix + primary, source],
        [prefix + fallback, source],
      ]);
    },
    resolveId(id) {
      return id === primary || id === fallback ? prefix + id : undefined;
    },
    load(id) {
      if (!id.startsWith(prefix)) return undefined;
      const source = modules?.get(id);
      if (source === undefined) {
        throw new Error(`@peren/vitest-plugin: no virtual module registered for ${id}`);
      }
      return source;
    },
  };
}

export default perenTest;
