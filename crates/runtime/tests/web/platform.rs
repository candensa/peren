use super::*;

#[tokio::test(flavor = "current_thread")]
async fn urlpattern_global_matches_worker_routes() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          fetch() {
            const route = new URLPattern({ protocol: 'https', hostname: 'worker.invalid', pathname: '/users/:id' });
            const insensitive = new URLPattern('/files/:name', 'https://worker.invalid', { ignoreCase: true });
            const match = route.exec('https://worker.invalid/users/42?tab=profile');
            return new Response(JSON.stringify({
              kind: typeof URLPattern,
              route: route.test('https://worker.invalid/users/42'),
              miss: route.test('https://other.invalid/users/42'),
              id: match.pathname.groups.id,
              input: match.pathname.input,
              insensitive: insensitive.test('https://worker.invalid/FILES/readme'),
              protocol: route.protocol,
              hostname: route.hostname,
              pathname: route.pathname,
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/urlpattern".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(2048, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "kind": "function",
            "route": true,
            "miss": false,
            "id": "42",
            "input": "/users/42",
            "insensitive": true,
            "protocol": "https",
            "hostname": "worker.invalid",
            "pathname": "/users/:id",
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn compression_stream_globals_round_trip_gzip() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          async fetch() {
            const source = new ReadableStream({
              start(controller) {
                controller.enqueue(new TextEncoder().encode('text'));
                controller.close();
              }
            });
            const compressed = source.pipeThrough(new CompressionStream('gzip'));
            const bytes = new Uint8Array(await new Response(compressed).arrayBuffer());
            const restored = await new Response(
              new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))
            ).text();
            return new Response(JSON.stringify({
              compression: typeof CompressionStream,
              decompression: typeof DecompressionStream,
              smallerThanPlainHeader: bytes[0] === 31 && bytes[1] === 139,
              restored,
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/compression".into(),
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
            "compression": "function",
            "decompression": "function",
            "smallerThanPlainHeader": true,
            "restored": "text",
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stream_queuing_strategy_globals_measure_chunks() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          fetch() {
            const byteStrategy = new ByteLengthQueuingStrategy({ highWaterMark: 8 });
            const countStrategy = new CountQueuingStrategy({ highWaterMark: 3 });
            return new Response(JSON.stringify({
              byteKind: typeof ByteLengthQueuingStrategy,
              countKind: typeof CountQueuingStrategy,
              byteWater: byteStrategy.highWaterMark,
              countWater: countStrategy.highWaterMark,
              bytes: byteStrategy.size(new Uint8Array([1, 2, 3, 4])),
              count: countStrategy.size({}),
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/strategy".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(2048, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "byteKind": "function",
            "countKind": "function",
            "byteWater": 8,
            "countWater": 3,
            "bytes": 4,
            "count": 1,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn text_stream_globals_encode_and_decode_incrementally() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          async fetch() {
            const textReader = new ReadableStream({
              start(controller) {
                controller.enqueue(new Uint8Array([0xf0, 0x9f]));
                controller.enqueue(new Uint8Array([0x8c, 0x8d]));
                controller.close();
              }
            }).pipeThrough(new TextDecoderStream()).getReader();
            let decoded = '';
            while (true) {
              const chunk = await textReader.read();
              if (chunk.done) break;
              decoded += chunk.value;
            }
            const encoded = await new Response(new ReadableStream({
              start(controller) {
                controller.enqueue('pe');
                controller.enqueue('ren');
                controller.close();
              }
            }).pipeThrough(new TextEncoderStream())).arrayBuffer();
            return new Response(JSON.stringify({
              decoder: typeof TextDecoderStream,
              encoder: typeof TextEncoderStream,
              decoded,
              encoded: Array.from(new Uint8Array(encoded)),
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/text-streams".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(2048, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "decoder": "function",
            "encoder": "function",
            "decoded": "🌍",
            "encoded": [112, 101, 114, 101, 110],
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn formdata_global_round_trips_fetch_bodies() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        export default {
          async fetch() {
            const form = new FormData();
            form.append('name', 'text');
            form.append('tag', 'one');
            form.append('tag', 'two');
            form.set('tag', 'final');
            form.append('file', new File(['hello'], 'hello.txt', { type: 'text/plain' }));
            form.delete('missing');
            const request = new Request('https://worker.invalid/upload', { method: 'POST', body: form });
            const parsed = await request.formData();
            const file = parsed.get('file');
            return new Response(JSON.stringify({
              kind: typeof FormData,
              hasName: form.has('name'),
              name: form.get('name'),
              tags: form.getAll('tag'),
              keys: Array.from(form.keys()),
              entries: Array.from(form).map(([key, value]) => [key, typeof value === 'string' ? value : `${value.name}:${value.type}`]),
              contentType: request.headers.get('content-type').startsWith('multipart/form-data; boundary='),
              parsed: { name: parsed.get('name'), tag: parsed.get('tag'), file: `${file.name}:${file.type}:${await file.text()}` },
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/formdata".into(),
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
            "kind": "function",
            "hasName": true,
            "name": "text",
            "tags": ["final"],
            "keys": ["name", "tag", "file"],
            "entries": [["name", "text"], ["tag", "final"], ["file", "hello.txt:text/plain"]],
            "contentType": true,
            "parsed": { "name": "text", "tag": "final", "file": "hello.txt:text/plain:hello" },
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn filereader_global_reads_blob_forms_and_events() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r"
        const read = (method, blob) => new Promise((resolve, reject) => {
          const reader = new FileReader();
          const events = [];
          reader.onload = (event) => events.push(`${event.type}:${event instanceof ProgressEvent}:${event.loaded}`);
          reader.onloadend = () => resolve({ result: reader.result, state: reader.readyState, events });
          reader.onerror = () => reject(reader.error);
          reader[method](blob);
        });
        export default {
          async fetch() {
            const text = await read('readAsText', new Blob(['text'], { type: 'text/plain' }));
            const data = await read('readAsDataURL', new Blob(['ok'], { type: 'text/plain' }));
            const bytes = await read('readAsArrayBuffer', new Blob([new Uint8Array([1, 2, 3])]));
            return new Response(JSON.stringify({
              kind: typeof FileReader,
              constants: [FileReader.EMPTY, FileReader.LOADING, FileReader.DONE],
              text: { result: text.result, state: text.state, events: text.events },
              data: data.result,
              bytes: Array.from(new Uint8Array(bytes.result)),
            }));
          }
        };
        ",
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/filereader".into(),
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
            "kind": "function",
            "constants": [0, 1, 2],
            "text": { "result": "text", "state": 2, "events": ["load:true:4"] },
            "data": "data:text/plain;base64,b2s=",
            "bytes": [1, 2, 3],
        })
    );
}
