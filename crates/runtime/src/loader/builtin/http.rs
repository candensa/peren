pub(super) const NODE_HTTP_SOURCE: &str = r#"
import { EventEmitter } from "node:events";
import { Buffer } from "node:buffer";
function refuse(name) { return function () { throw new Error(`http.${name}() is not available inside a Worker isolate`); }; }
function normalize(input, options = {}) {
  const url = input instanceof URL ? new URL(input.href) : typeof input === "string" ? new URL(input) : new URL(`${input.protocol ?? "http:"}//${input.hostname ?? input.host ?? "localhost"}${input.port ? `:${input.port}` : ""}${input.path ?? input.pathname ?? "/"}`);
  const headers = new Headers(options.headers ?? input.headers ?? {});
  return { url, method: String(options.method ?? input.method ?? "GET"), headers };
}
export class IncomingMessage extends EventEmitter {
  constructor(response) { super(); this.statusCode = response.status; this.statusMessage = response.statusText; this.headers = Object.fromEntries(response.headers.entries()); this.url = response.url; this.readable = true; }
}
export class ClientRequest extends EventEmitter {
  constructor(input, options, callback) { super(); const normalized = normalize(input, options ?? {}); this.url = normalized.url; this.method = normalized.method; this.headers = normalized.headers; this.body = []; if (typeof callback === "function") this.once("response", callback); }
  setHeader(name, value) { this.headers.set(name, String(value)); }
  getHeader(name) { return this.headers.get(name); }
  removeHeader(name) { this.headers.delete(name); }
  write(chunk) { if (chunk !== undefined) this.body.push(chunk); return true; }
  async end(chunk) {
    if (chunk !== undefined) this.write(chunk);
    try {
      const body = this.body.length === 0 ? undefined : new Blob(this.body);
      const response = await fetch(this.url, { method: this.method, headers: this.headers, body });
      const message = new IncomingMessage(response);
      this.emit("response", message);
      const bytes = Buffer.from(await response.arrayBuffer());
      if (bytes.length > 0) message.emit("data", bytes);
      message.emit("end");
      this.emit("close");
    } catch (error) { this.emit("error", error); }
  }
  abort() { this.emit("abort"); this.emit("close"); }
  destroy(error) { if (error) this.emit("error", error); this.emit("close"); }
}
export function request(input, options, callback) { if (typeof options === "function") { callback = options; options = {}; } return new ClientRequest(input, options, callback); }
export function get(input, options, callback) { const req = request(input, options, callback); req.end(); return req; }
export const createServer = refuse("createServer");
export const Server = function Server() { throw new Error("http.Server is not available inside a Worker isolate"); };
export const Agent = class Agent {};
export const globalAgent = new Agent();
export default { request, get, createServer, ClientRequest, IncomingMessage, Server, Agent, globalAgent };
"#;
