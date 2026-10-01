pub(super) const CLOUDFLARE_WORKERS_SOURCE: &str = r"
export const DurableObject = globalThis.DurableObject;
export const DurableObjectState = globalThis.DurableObjectState;
export const WorkerEntrypoint = globalThis.WorkerEntrypoint;
export const RpcTarget = globalThis.RpcTarget;
export const WebSocketRequestResponsePair = globalThis.WebSocketRequestResponsePair;
";
