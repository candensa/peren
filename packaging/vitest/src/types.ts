export interface ProviderMetadata {
  kind: string;
  [key: string]: unknown;
}

export interface ProviderBacked {
  provider: Readonly<ProviderMetadata>;
}

export interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
  passThroughOnException(): void;
  props: Record<string, unknown>;
}

export interface Fetcher {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectTestStorage {
  readonly id: string;
  get<T = unknown>(key: string): Promise<T | undefined>;
  list<T = unknown>(): Promise<T>;
  put(key: string, value: string | Uint8Array): Promise<unknown>;
  delete(key: string): Promise<boolean>;
}

export interface KvListOptions {
  prefix?: string;
  cursor?: string;
  limit?: number;
}

export interface KvListResult {
  keys: Array<{ name: string; expiration?: number; metadata?: unknown }>;
  list_complete: boolean;
  cursor?: string;
}

export interface KvGetOptions {
  type?: "text" | "json" | "arrayBuffer" | "stream";
}

export interface KvPutOptions {
  metadata?: unknown;
  expiration?: number;
  expirationTtl?: number;
}

export interface KvCompareOptions {
  version?: number;
  missing?: boolean;
}

export interface KvCompareResult {
  ok: boolean;
  version: number | null;
}

export interface KvNamespace extends ProviderBacked {
  get(key: string): Promise<string | null>;
  get<T = unknown>(key: string, options: { type: "json" }): Promise<T | null>;
  get(key: string, options: { type: "arrayBuffer" }): Promise<ArrayBuffer | null>;
  get(key: string, options: { type: "stream" }): Promise<ReadableStream<Uint8Array> | null>;
  getWithMetadata<T = string, M = unknown>(
    key: string,
    options?: KvGetOptions,
  ): Promise<{ value: T | null; metadata: M | null; version: number | null }>;
  put(key: string, value: string | ArrayBuffer | ArrayBufferView | ReadableStream, options?: KvPutOptions): Promise<void>;
  compareAndSet(
    key: string,
    expected: KvCompareOptions,
    value: string | ArrayBuffer | ArrayBufferView | ReadableStream,
    options?: KvPutOptions,
  ): Promise<KvCompareResult>;
  delete(key: string): Promise<void>;
  list(options?: KvListOptions): Promise<KvListResult>;
}

export interface CacheQueryOptions {
  ignoreMethod?: boolean;
  ignoreSearch?: boolean;
  ignoreVary?: boolean;
}

export interface Cache extends ProviderBacked {
  match(request: RequestInfo | URL, options?: CacheQueryOptions): Promise<Response | undefined>;
  put(request: RequestInfo | URL, response: Response): Promise<void>;
  delete(request: RequestInfo | URL, options?: CacheQueryOptions): Promise<boolean>;
}

export interface CacheStorage {
  readonly default: Cache;
  open(name: string): Promise<Cache>;
}

export interface R2ObjectBody {
  key: string;
  size: number;
  httpMetadata: { contentType?: string };
  customMetadata: Readonly<Record<string, string>>;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T = unknown>(): Promise<T>;
}

export interface R2PutOptions {
  httpMetadata?: { contentType?: string };
  customMetadata?: Record<string, string | number | boolean>;
}

export interface R2ObjectEntry {
  key: string;
  size: number;
  customMetadata: Readonly<Record<string, string>>;
}

export interface R2ListResult {
  objects: R2ObjectEntry[];
  cursor?: string;
  truncated: boolean;
}

export interface R2ListOptions {
  prefix?: string;
  cursor?: string;
  limit?: number;
}

export interface R2Bucket extends ProviderBacked {
  get(key: string, options?: Record<string, unknown>): Promise<R2ObjectBody | null>;
  put(key: string, value: string | ArrayBuffer | ArrayBufferView | ReadableStream, options?: R2PutOptions): Promise<unknown>;
  delete(keys: string | string[]): Promise<unknown>;
  list(options?: R2ListOptions): Promise<R2ListResult>;
}

export interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  all<T = unknown>(): Promise<T>;
  first<T = unknown>(): Promise<T | null>;
  run<T = unknown>(): Promise<T>;
  raw<T = unknown>(): Promise<T>;
}

export interface D1Database extends ProviderBacked {
  prepare(sql: string): D1PreparedStatement;
  exec(sql: string): Promise<unknown>;
  batch<T = unknown>(statements: D1PreparedStatement[]): Promise<T>;
}

export interface QueueSendOptions {
  contentType?: string;
  delaySeconds?: number;
  dedupId?: string;
}

export interface QueueMessage {
  body: unknown;
  contentType?: string;
  delaySeconds?: number;
  dedupId?: string;
}

export interface Queue extends ProviderBacked {
  send(body: unknown, options?: QueueSendOptions): Promise<unknown>;
  sendBatch(messages: QueueMessage[]): Promise<unknown>;
}

export interface QueueRetryOptions {
  delaySeconds?: number;
}

export interface QueueBatchMetrics {
  ready: number;
  delayed: number;
  leased: number;
  oldestReadyTimestamp: number | null;
}

export interface QueueBatchMessage<T = unknown> {
  id: string;
  body: T;
  attempts: number;
  timestamp: number;
  ack(): void;
  retry(options?: QueueRetryOptions): void;
}

export interface QueueBatch<T = unknown> {
  queue: string;
  messages: QueueBatchMessage<T>[];
  metrics: QueueBatchMetrics;
  ackAll(): void;
  retryAll(options?: QueueRetryOptions): void;
  waitUntil(promise: Promise<unknown>): void;
}

export interface VectorRecord {
  id: string;
  values: number[];
  metadata?: Record<string, unknown>;
}

export interface VectorQueryOptions {
  topK?: number;
  namespace?: string;
  filter?: Record<string, string | number | boolean | null>;
  returnMetadata?: boolean;
  returnValues?: boolean;
}

export interface VectorMatch {
  id: string;
  score: number;
  values?: number[];
  metadata?: Record<string, unknown> | null;
}

export interface VectorQueryResult {
  matches: VectorMatch[];
  count: number;
}

export interface VectorMutationResult {
  count: number;
}

export interface VectorizeIndex extends ProviderBacked {
  upsert(vectors: VectorRecord[]): Promise<VectorMutationResult>;
  query(vector: number[], options?: VectorQueryOptions): Promise<VectorQueryResult>;
  getByIds(ids: string[]): Promise<VectorRecord[]>;
  deleteByIds(ids: string[]): Promise<VectorMutationResult>;
}

export interface AiTextResult<T = unknown> {
  text: string;
  raw: T;
}

export interface AiEmbeddingResult<T = unknown> {
  embeddings: number[][];
  raw: T;
}

export interface AiBinding extends ProviderBacked {
  run<T = unknown>(model: string, input: unknown, options?: Record<string, unknown>): Promise<T>;
  embed<T = unknown>(model: string, input: string | { text?: unknown; input?: unknown }, options?: Record<string, unknown>): Promise<AiEmbeddingResult<T>>;
  generateText<T = unknown>(model: string, prompt: string | { prompt?: unknown; text?: unknown; input?: unknown }, options?: Record<string, unknown>): Promise<AiTextResult<T>>;
  chat<T = unknown>(model: string, messages: Array<Record<string, unknown>>, options?: Record<string, unknown>): Promise<AiTextResult<T>>;
}

export interface RateLimiterRequest {
  key?: string;
}

export interface RateLimiterResult {
  success: boolean;
  limit: number;
  remaining: number;
  reset: number;
}

export interface RateLimiterBinding extends ProviderBacked {
  limit(request?: RateLimiterRequest): Promise<RateLimiterResult>;
}

export interface AnalyticsEngineDataset extends ProviderBacked {
  writeDataPoint(point?: { blobs?: unknown[]; doubles?: number[]; indexes?: unknown[] }): void;
}


export interface PerenWebSocket extends WebSocket {
  serializeAttachment(value: unknown): Promise<void>;
  deserializeAttachment<T = unknown>(): Promise<T | undefined>;
  deleteAttachment(): Promise<void>;
}

export interface WebSocketRequestResponsePair {
  readonly request: string;
  readonly response: string;
}

export interface WebSocketRequestResponsePairConstructor {
  new (request: string, response: string): WebSocketRequestResponsePair;
}

export interface DurableObjectState {
  readonly storage: DurableObjectTestStorage;
  readonly facets: unknown;
  acceptWebSocket(socket: PerenWebSocket, tags?: Iterable<string>): PerenWebSocket;
  getWebSockets(tag?: string): readonly PerenWebSocket[];
  setWebSocketAutoResponse(pair: WebSocketRequestResponsePair | null): void;
  getWebSocketAutoResponse(): WebSocketRequestResponsePair | null;
  getWebSocketAutoResponseTimestamp(): number | null;
}

export interface DurableObjectStateConstructor {
  new (): DurableObjectState;
}

export interface DurableObjectId {
  toString(): string;
}

export interface DurableObjectStub extends Fetcher {}

export interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  idFromString(id: string): DurableObjectId;
  newUniqueId(): DurableObjectId;
  get(id: DurableObjectId): DurableObjectStub;
}

export interface ServiceBinding extends Fetcher {}

export interface DispatchNamespaceBinding {
  readonly namespace: string;
  readonly scripts: readonly string[];
  get(script: string): Fetcher;
}

export interface WorkflowInstance {
  readonly id: string;
  status(): Promise<{ id: string; status: string; reason?: string; updatedAt?: number }>;
  terminate(reason?: string): Promise<{ id: string; status: string; reason?: string; updatedAt: number }>;
  restart(): Promise<{ id: string; status: string; reason?: string; updatedAt: number }>;
}

export interface WorkflowBinding extends ProviderBacked {
  create(options?: { id?: string }): Promise<WorkflowInstance>;
  get(id: string): WorkflowInstance;
}

export interface HyperdriveBinding extends ProviderBacked {
  connectionString: string;
  host: string;
  cachingDisabled: boolean;
  maxAge: number;
  staleWhileRevalidate: number;
  poolMaxConnections: number;
}

export interface ContainerExecResult {
  exitCode?: number;
  stdout?: string;
  stderr?: string;
}

export interface ContainerStatus {
  state?: string;
  [key: string]: unknown;
}

export interface ContainerInstance extends Fetcher, ProviderBacked {
  id: string;
  image: string;
  port: number;
  memoryMb: unknown;
  cpuMillis: unknown;
  idleSleepSecs: number;
  allowNetworkEgress: boolean;
  experimental: true;
  status(): Promise<ContainerStatus>;
  start(): Promise<ContainerStatus>;
  stop(): Promise<ContainerStatus>;
  destroy(): Promise<ContainerStatus>;
  exec(command: string | string[], options?: Record<string, unknown>): Promise<ContainerExecResult>;
  writeFile(path: string, contents: string | Uint8Array | ArrayBuffer): Promise<boolean>;
  readFile(path: string): Promise<Uint8Array>;
}

export interface ContainerBinding extends ContainerInstance {
  get(name: string): ContainerInstance;
}

export interface OutboundBinding extends Fetcher {
  allowedHosts: readonly string[];
}

export interface LoaderBinding {
  import<T = unknown>(specifier: string): Promise<T>;
}

export interface ImagesBinding extends ProviderBacked {
  input(source: unknown): {
    transform(options?: Record<string, unknown>): unknown;
    output(): Promise<Response>;
  };
}

export type TestBinding =
  | Cache
  | KvNamespace
  | R2Bucket
  | D1Database
  | Queue
  | VectorizeIndex
  | AiBinding
  | RateLimiterBinding
  | AnalyticsEngineDataset
  | DurableObjectNamespace
  | ServiceBinding
  | DispatchNamespaceBinding
  | WorkflowBinding
  | HyperdriveBinding
  | ContainerBinding
  | OutboundBinding
  | LoaderBinding
  | ImagesBinding;

export type TestEnv = Record<string, TestBinding>;
