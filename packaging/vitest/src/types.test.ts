import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { describe, expect, it } from "vitest";

const root = new URL("..", import.meta.url).pathname;

describe("published declarations", () => {
  it("let consumers import native test helpers without local binding interfaces", () => {
    const dir = mkdtempSync(join(tmpdir(), "peren-vitest-types-"));
    mkdirSync(join(dir, "node_modules", "@peren"), { recursive: true });
    symlinkSync(root, join(dir, "node_modules", "@peren", "vitest-plugin"));
    writeFileSync(
      join(dir, "check.ts"),
      `import { caches, env, SELF, createExecutionContext, runInDurableObject, waitOnExecutionContext } from "cloudflare:test";
import type { AiBinding, AiEmbeddingResult, AiTextResult, AnalyticsEngineDataset, CacheQueryOptions, ContainerBinding, ContainerExecResult, ContainerInstance, ContainerStatus, DispatchNamespaceBinding, DurableObjectNamespace, DurableObjectState, DurableObjectTestStorage, HyperdriveBinding, PerenWebSocket, WebSocketRequestResponsePair, KvCompareResult, KvListOptions, KvListResult, KvNamespace, ProviderMetadata, Queue, QueueBatch, QueueMessage, QueueSendOptions, R2Bucket, R2ListOptions, R2ListResult, R2ObjectBody, RateLimiterBinding, RateLimiterRequest, RateLimiterResult, ServiceBinding, VectorMatch, VectorQueryOptions, VectorQueryResult, VectorRecord, VectorizeIndex, WorkflowBinding } from "cloudflare:test";

async function consume(batch: QueueBatch<string>) {
  const oldest: number | null = batch.metrics.oldestReadyTimestamp;
  void oldest;
  batch.messages[0]?.ack();
  batch.retryAll({ delaySeconds: 5 });
  batch.ackAll();
  await batch.waitUntil(Promise.resolve(batch.queue));
}

async function main() {
  const cache = env.CACHE as KvNamespace;
  const jobs = env.JOBS as Queue;
  const bucket = env.BUCKET as R2Bucket;
  const vectors = env.VECTORS as VectorizeIndex;
  const ai = env.AI as AiBinding;
  const auth = env.AUTH as ServiceBinding;
  const dispatch = env.DISPATCH as DispatchNamespaceBinding;
  const flow = env.FLOW as WorkflowBinding;
  const workflowProvider: string = flow.provider.kind;
  const hyperdrive = env.HYPERDRIVE as HyperdriveBinding;
  const hyperdriveProvider: string = hyperdrive.provider.kind;
  void workflowProvider;
  const app = env.APP as ContainerBinding;
  const limiter = env.LIMITER as RateLimiterBinding;
  const analytics = env.ANALYTICS as AnalyticsEngineDataset;
  const room = env.ROOMS as DurableObjectNamespace;
  const ctx = createExecutionContext();
  ctx.waitUntil(jobs.send({ ok: true }, { dedupId: "job:ok" }));
  const provider: string = cache.provider.kind;
  const metadata: ProviderMetadata = cache.provider;
  void metadata;
  const listOptions: KvListOptions = { prefix: "k", limit: 10 };
  const listResult: KvListResult = await cache.list(listOptions);
  void listResult.keys.length;
  await cache.put("key", provider, { metadata: { source: "test" }, expirationTtl: 60 });
  const record = await cache.getWithMetadata<string, { source: string }>("key");
  const source: string | undefined = record.metadata?.source;
  const version: number | null = record.version;
  const firstCas: KvCompareResult = await cache.compareAndSet("atomic", { missing: true }, "first");
  void firstCas.version;
  await cache.compareAndSet("atomic", { version: version ?? 1 }, "second", { metadata: { source: "cas" } });
  void source;
  await bucket.put("object", "body", { httpMetadata: { contentType: "text/plain" }, customMetadata: { owner: "ada" } });
  const object: R2ObjectBody | null = await bucket.get("object");
  const owner: string | undefined = object?.customMetadata.owner;
  const listRequest: R2ListOptions = { prefix: "obj" };
  const listed: R2ListResult = await bucket.list(listRequest);
  const listedSize: number | undefined = listed.objects[0]?.size;
  await bucket.delete(["object", "missing"]);
  void owner;
  void listedSize;
  const vectorRecord: VectorRecord = { id: "doc", values: [0.1, 0.2], metadata: { source: "guide" } };
  void vectorRecord;
  const vectorRequest: VectorQueryOptions = { topK: 3, filter: { source: "guide" }, returnMetadata: true };
  const vectorResult: VectorQueryResult = await vectors.query([0.1, 0.2], vectorRequest);
  const firstMatch: VectorMatch | undefined = vectorResult.matches[0];
  const firstScore: number | undefined = firstMatch?.score;
  void firstScore;
  const aiProvider: string = ai.provider.kind;
  await ai.run("model", { prompt: aiProvider });
  const generated = await ai.generateText("model", "hello", { maxTokens: 64 });
  const generatedResult: AiTextResult = generated;
  const generatedText: string = generated.text;
  const chat = await ai.chat("model", [{ role: "user", content: "hello" }]);
  const chatText: string = chat.text;
  const embedding = await ai.embed("embedding", "hello");
  const embeddingResult: AiEmbeddingResult = embedding;
  const firstEmbedding: number | undefined = embedding.embeddings[0]?.[0];
  const rawEmbedding: unknown = embedding.raw;
  void generatedResult;
  void embeddingResult;
  void generatedText;
  void chatText;
  void firstEmbedding;
  void rawEmbedding;
  await auth.fetch("https://service.invalid/session");
  await dispatch.get("tenant").fetch("https://dispatch.invalid/request");
  const roomId = room.idFromName("lobby");
  const roomStub = room.get(roomId);
  const hibernation: WebSocketRequestResponsePair = { request: "ping", response: "pong" };
  const durableState = undefined as unknown as DurableObjectState;
  durableState.setWebSocketAutoResponse(hibernation);
  const auto: WebSocketRequestResponsePair | null = durableState.getWebSocketAutoResponse();
  const sockets: readonly PerenWebSocket[] = durableState.getWebSockets("room");
  void auto;
  void sockets;
  const stored = await runInDurableObject(roomStub, async (_instance, storage: DurableObjectTestStorage) => {
    await storage.put("count", "1");
    return await storage.get<string>("count");
  });
  void stored;
  void hyperdriveProvider;
  const containerProvider: string = app.provider.kind;
  const build: ContainerInstance = app.get("build");
  const buildProvider: string = build.provider.kind;
  const status: ContainerStatus = await build.status();
  const containerStatus: string | undefined = status.state;
  const execResult: ContainerExecResult = await build.exec(["echo", "ok"], { cwd: "/workspace" });
  const execCode: number | undefined = execResult.exitCode;
  await build.writeFile("/workspace/readme.txt", "hello");
  const fileBytes: Uint8Array = await build.readFile("/workspace/readme.txt");
  await build.destroy();
  void containerStatus;
  void execCode;
  void fileBytes;
  void containerProvider;
  void buildProvider;
  const rateRequest: RateLimiterRequest = { key: "deploy" };
  const limiterProvider: string = limiter.provider.kind;
  const limit: RateLimiterResult = await limiter.limit(rateRequest);
  const allowed: boolean = limit.success;
  const remaining: number = limit.remaining;
  const analyticsProvider: string = analytics.provider.kind;
  analytics.writeDataPoint({ blobs: ["deploy"], doubles: [1], indexes: ["tenant"] });
  void allowed;
  void remaining;
  void limiterProvider;
  void analyticsProvider;
  const instance = await flow.create({ id: "deploy" });
  await instance.terminate("done");
  await flow.get(instance.id).status();
  const named = await caches.open("pages");
  const cacheProvider: string = named.provider.kind;
  await named.put("https://example.com/page", new Response(cacheProvider));
  const cacheOptions: CacheQueryOptions = { ignoreSearch: true };
  await (await named.match("https://example.com/page?cache=bust", cacheOptions))?.text();
  await (await named.match("https://example.com/page", { ignoreVary: true }))?.text();
  await named.delete(new Request("https://example.com/page?cache=bust", { method: "POST" }), { ignoreMethod: true, ignoreSearch: true });
  await SELF.fetch("/healthz");
  const sendOptions: QueueSendOptions = { dedupId: "job:again", delaySeconds: 1 };
  await jobs.send({ ok: true }, sendOptions);
  const message: QueueMessage = { body: { ok: true }, dedupId: "batch:1" };
  await jobs.sendBatch([message]);
  await consume({ queue: "jobs", metrics: { ready: 1, delayed: 0, leased: 1, oldestReadyTimestamp: Date.now() }, messages: [{ id: "1", body: "hello", attempts: 1, timestamp: Date.now(), ack() {}, retry() {} }], ackAll() {}, retryAll() {}, waitUntil: ctx.waitUntil });
  await waitOnExecutionContext(ctx);
}
void main();
`,
    );
    writeFileSync(
      join(dir, "tsconfig.json"),
      JSON.stringify(
        {
          compilerOptions: {
            target: "ES2022",
            module: "ESNext",
            moduleResolution: "Bundler",
            lib: ["ES2022", "DOM"],
            strict: true,
            types: ["@peren/vitest-plugin"],
          },
          include: ["check.ts"],
        },
        null,
        2,
      ),
    );

    const result = spawnSync("node_modules/.bin/tsc", ["--noEmit", "--project", join(dir, "tsconfig.json")], {
      cwd: root,
      encoding: "utf8",
    });

    expect(result.stderr + result.stdout).toBe("");
    expect(result.status).toBe(0);
  });
});
