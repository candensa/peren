import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { parse } from "smol-toml";

export interface PerenTestOptions {
  config: string;
  perenBin?: string;
  socket?: string;
  wrangler?: string[];
  readyTimeoutMs?: number;
}

export interface ResolvedOptions {
  configPath: string;
  configCwd: string;
  perenBin: string;
  wranglerPaths: string[];
  readyTimeoutMs: number;
  socketName: string;
  service: string;
  bindings: Record<string, Record<string, unknown>>;
  cache: Record<string, unknown>;
}

interface RawSocket {
  name: string;
  service: string;
}

interface RawBinding {
  type?: string;
  kind?: string;
  [key: string]: unknown;
}

interface RawService {
  name: string;
  bindings?: Record<string, RawBinding>;
}

interface RawConfig {
  sockets?: RawSocket[];
  services?: RawService[];
  cache?: Record<string, unknown>;
}

export function resolveOptions(options: PerenTestOptions, root: string): ResolvedOptions {
  const configPath = resolve(root, options.config);
  let raw: string;
  try {
    raw = readFileSync(configPath, "utf8");
  } catch (error) {
    throw new Error(
      `@peren/vitest-plugin: failed to read ${configPath}: ${(error as Error).message}`,
    );
  }

  const parsed = parse(raw) as unknown as RawConfig;
  const sockets = parsed.sockets ?? [];
  if (sockets.length === 0) {
    throw new Error(`@peren/vitest-plugin: ${configPath} declares no [[sockets]] entries`);
  }

  const socketName = options.socket ?? sockets[0]!.name;
  const socket = sockets.find((candidate) => candidate.name === socketName);
  if (!socket) {
    throw new Error(
      `@peren/vitest-plugin: socket ${JSON.stringify(socketName)} was not found in ${configPath}`,
    );
  }

  const services = parsed.services ?? [];
  const service = services.find((candidate) => candidate.name === socket.service);
  if (!service) {
    throw new Error(
      `@peren/vitest-plugin: socket ${JSON.stringify(socketName)} targets missing service ${JSON.stringify(socket.service)}`,
    );
  }

  const bindings: Record<string, Record<string, unknown>> = {};
  for (const [name, binding] of Object.entries(service.bindings ?? {})) {
    bindings[name] = { ...binding, type: binding.type ?? binding.kind ?? "unknown" };
  }

  return {
    configPath,
    configCwd: dirname(configPath),
    perenBin: options.perenBin ?? process.env.PEREN_TEST_SERVER_BIN ?? "peren",
    wranglerPaths: options.wrangler ?? [],
    readyTimeoutMs: options.readyTimeoutMs ?? 20_000,
    socketName,
    service: service.name,
    bindings,
    cache: parsed.cache ?? { kind: "memory" },
  };
}
