pub(super) const NODE_STREAM_SOURCE: &str = r#"
function unsupported(name) { return function () { throw new Error(`stream.${name} is not supported inside a Worker isolate; use Web Streams instead`); }; }
export const Readable = unsupported("Readable");
export const Writable = unsupported("Writable");
export const Duplex = unsupported("Duplex");
export const Transform = unsupported("Transform");
export const PassThrough = unsupported("PassThrough");
export const Stream = unsupported("Stream");
export const pipeline = unsupported("pipeline");
export const finished = unsupported("finished");
export const compose = unsupported("compose");
export const promises = Object.freeze({ pipeline, finished });
export const web = Object.freeze({
  ReadableStream: globalThis.ReadableStream,
  WritableStream: globalThis.WritableStream,
  TransformStream: globalThis.TransformStream,
  ByteLengthQueuingStrategy: globalThis.ByteLengthQueuingStrategy,
  CountQueuingStrategy: globalThis.CountQueuingStrategy,
});
export const ReadableStream = globalThis.ReadableStream;
export const WritableStream = globalThis.WritableStream;
export const TransformStream = globalThis.TransformStream;
export const ByteLengthQueuingStrategy = globalThis.ByteLengthQueuingStrategy;
export const CountQueuingStrategy = globalThis.CountQueuingStrategy;
export default { Readable, Writable, Duplex, Transform, PassThrough, Stream, pipeline, finished, compose, promises, web, ReadableStream, WritableStream, TransformStream, ByteLengthQueuingStrategy, CountQueuingStrategy };
"#;

pub(super) const NODE_STREAM_WEB_SOURCE: &str = r"
export const ReadableStream = globalThis.ReadableStream;
export const WritableStream = globalThis.WritableStream;
export const TransformStream = globalThis.TransformStream;
export const ByteLengthQueuingStrategy = globalThis.ByteLengthQueuingStrategy;
export const CountQueuingStrategy = globalThis.CountQueuingStrategy;
export default { ReadableStream, WritableStream, TransformStream, ByteLengthQueuingStrategy, CountQueuingStrategy };
";

pub(super) const NODE_STREAM_CONSUMERS_SOURCE: &str = r#"
import { Buffer } from "node:buffer";

async function chunks(input) {
  if (input === undefined || input === null) throw new TypeError("stream is required");
  if (input instanceof Response) return chunks(input.body);
  if (typeof Blob !== "undefined" && input instanceof Blob) return [new Uint8Array(await input.arrayBuffer())];
  if (typeof input === "string" || input instanceof ArrayBuffer || ArrayBuffer.isView(input)) return [Buffer.from(input)];
  if (typeof input.getReader === "function") {
    const reader = input.getReader();
    const out = [];
    try {
      while (true) {
        const entry = await reader.read();
        if (entry.done) break;
        out.push(Buffer.from(entry.value));
      }
    } finally {
      reader.releaseLock?.();
    }
    return out;
  }
  if (input.readable && typeof input.readable.getReader === "function") return chunks(input.readable);
  if (typeof input[Symbol.asyncIterator] === "function") {
    const out = [];
    for await (const chunk of input) out.push(Buffer.from(chunk));
    return out;
  }
  throw new TypeError("stream must be a ReadableStream, async iterable, Blob, Response, or byte-like value");
}

export async function buffer(stream) { return Buffer.concat(await chunks(stream)); }
export async function arrayBuffer(stream) {
  const bytes = await buffer(stream);
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}
export async function text(stream) { return new TextDecoder().decode(await buffer(stream)); }
export async function json(stream) { return JSON.parse(await text(stream)); }
export async function blob(stream, options = {}) {
  if (typeof Blob === "undefined") throw new Error("Blob is not available inside this Worker isolate");
  return new Blob([await arrayBuffer(stream)], options);
}
export default { arrayBuffer, blob, buffer, json, text };
"#;

pub(super) const NODE_STREAM_PROMISES_SOURCE: &str = r#"
async function writeToDestination(source, destination) {
  if (destination == null) return source;
  if (typeof destination === "function") return destination(source);
  if (destination instanceof TransformStream) return source.pipeThrough(destination);
  if (destination instanceof WritableStream) { await source.pipeTo(destination); return undefined; }
  if (destination?.writable instanceof WritableStream) { await source.pipeTo(destination.writable); return destination; }
  throw new TypeError("stream/promises.pipeline only supports Web Streams inside a Worker isolate");
}
function toReadable(value) {
  if (value instanceof ReadableStream) return value;
  if (value instanceof Response) return value.body;
  if (value instanceof Blob) return value.stream();
  if (typeof value === "string" || value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return new Response(value).body;
  }
  if (value?.readable instanceof ReadableStream) return value.readable;
  throw new TypeError("stream/promises.pipeline source must be a Web ReadableStream or byte-like value inside a Worker isolate");
}
export async function pipeline(source, ...stages) {
  if (stages.length === 0) return toReadable(source);
  let current = toReadable(source);
  for (const stage of stages) current = await writeToDestination(current, stage);
  return current;
}
export async function finished(stream) {
  if (stream instanceof ReadableStream) {
    const reader = stream.getReader();
    try { while (!(await reader.read()).done) {} } finally { reader.releaseLock?.(); }
    return;
  }
  if (stream instanceof WritableStream) { await stream.getWriter().closed; return; }
  if (stream?.readable instanceof ReadableStream) return finished(stream.readable);
  throw new TypeError("stream/promises.finished only supports Web Streams inside a Worker isolate");
}
export default { pipeline, finished };
"#;
