use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_console_and_stream_web_reexports_are_worker_safe() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import consoleModule, { Console, log } from "node:console";
            import stream from "node:stream";
            import web, { ByteLengthQueuingStrategy, CountQueuingStrategy, ReadableStream, TransformStream, WritableStream } from "node:stream/web";
            export default {
              async fetch() {
                log('ignored');
                const consoleInstance = new Console();
                consoleInstance.info('ignored');
                const readable = new ReadableStream({
                  start(controller) {
                    controller.enqueue(new TextEncoder().encode('a'));
                    controller.enqueue(new TextEncoder().encode('b'));
                    controller.close();
                  }
                });
                const transformed = readable.pipeThrough(new TransformStream({
                  transform(chunk, controller) { controller.enqueue(chunk); }
                }));
                const text = await new Response(transformed).text();
                return new Response(JSON.stringify({
                  defaultLog: consoleModule.log === log,
                  consoleInstance: consoleInstance instanceof Console,
                  readable: ReadableStream === globalThis.ReadableStream && web.ReadableStream === ReadableStream && stream.ReadableStream === ReadableStream,
                  writable: WritableStream === globalThis.WritableStream && stream.web.WritableStream === WritableStream,
                  transform: TransformStream === globalThis.TransformStream,
                  byteStrategy: new ByteLengthQueuingStrategy({ highWaterMark: 4 }).size(new Uint8Array([1, 2, 3])),
                  countStrategy: new CountQueuingStrategy({ highWaterMark: 2 }).size("chunk"),
                  defaultByteStrategy: web.ByteLengthQueuingStrategy === ByteLengthQueuingStrategy,
                  classicRefused: (() => { try { stream.Readable(); return false; } catch (error) { return error.message.includes('Web Streams'); } })(),
                  text,
                }));
              }
            };
        "#,
            ),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/node-stream".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "defaultLog": true,
            "consoleInstance": true,
            "readable": true,
            "writable": true,
            "transform": true,
            "byteStrategy": 3,
            "countStrategy": 1,
            "defaultByteStrategy": true,
            "classicRefused": true,
            "text": "ab",
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn node_stream_consumers_read_web_streams_and_byte_like_inputs() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import consumers, { arrayBuffer, blob, buffer, json, text } from "node:stream/consumers";
            import promises, { finished, pipeline } from "node:stream/promises";
            import zlib, { gzip, gunzip } from "node:zlib";
            export default {
              async fetch() {
                const stream = new ReadableStream({
                  start(controller) {
                    controller.enqueue(new TextEncoder().encode('{"ok":'));
                    controller.enqueue(new TextEncoder().encode('true}'));
                    controller.close();
                  }
                });
                const parsed = await json(stream);
                const bytes = await buffer(new ReadableStream({
                  start(controller) {
                    controller.enqueue(new Uint8Array([97, 98]));
                    controller.close();
                  }
                }));
                const raw = await arrayBuffer(new Uint8Array([1, 2, 3]));
                const madeBlob = await blob('hello', { type: 'text/plain' });
                const piped = await pipeline(new Response('pipe').body, new TransformStream({
                  transform(chunk, controller) { controller.enqueue(chunk); }
                }));
                await finished(piped);
                const pipedText = await text(await pipeline('done'));
                const compressed = await gzip('hello');
                const inflated = await gunzip(compressed);
                const callbackInflated = await new Promise((resolve, reject) => gzip('cb', (error, value) => error ? reject(error) : gunzip(value, (inner, output) => inner ? reject(inner) : resolve(output.toString()))));
                let syncRefused = false;
                try { zlib.gzipSync('x'); } catch (error) { syncRefused = error.message.includes('async helper'); }
                return new Response(JSON.stringify({
                  defaultText: typeof consumers.text,
                  parsed,
                  text: await text(new Response('text')),
                  bytes: bytes.toString(),
                  raw: Array.from(new Uint8Array(raw)),
                  blob: { type: madeBlob.type, text: await madeBlob.text() },
                  pipeline: typeof promises.pipeline,
                  pipedText,
                  zlib: { inflated: inflated.toString(), callbackInflated, syncRefused, defaultGzip: typeof zlib.gzip },
                }));
              }
            };
        "#,
            ),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/node-stream-consumers".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "defaultText": "function",
            "parsed": { "ok": true },
            "text": "text",
            "bytes": "ab",
            "raw": [1, 2, 3],
            "blob": { "type": "text/plain", "text": "hello" },
            "pipeline": "function",
            "pipedText": "done",
            "zlib": { "inflated": "hello", "callbackInflated": "cb", "syncRefused": true, "defaultGzip": "function" },
        })
    );
}
