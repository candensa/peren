use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn blob_and_file_globals_follow_file_api_basics() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            r#"
        export default {
          async fetch() {
            const blob = new Blob(["te", new Uint8Array([120, 116])], { type: "text/plain" });
            const sliced = blob.slice(1, 3, "text/custom");
            const file = new File([blob], "text.txt", { type: "text/plain", lastModified: 42 });
            const streamText = await new Response(blob.stream()).text();
            return new Response(JSON.stringify({
              blob: { size: blob.size, type: blob.type, text: await blob.text(), bytes: Array.from(new Uint8Array(await blob.arrayBuffer())) },
              slice: { size: sliced.size, type: sliced.type, text: await sliced.text() },
              file: { name: file.name, lastModified: file.lastModified, type: file.type, text: await file.text(), isBlob: file instanceof Blob },
              streamText,
            }));
          }
        };
        "#,
        ),
        limits(),
    )
    .await
    .unwrap();
    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/blob".into(),
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
            "blob": { "size": 4, "type": "text/plain", "text": "text", "bytes": [116, 101, 120, 116] },
            "slice": { "size": 2, "type": "text/custom", "text": "ex" },
            "file": { "name": "text.txt", "lastModified": 42, "type": "text/plain", "text": "text", "isBlob": true },
            "streamText": "text",
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn base64_globals_encode_and_decode_latin1_strings() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { fetch() {
              return new Response(`${btoa('hello')}:${atob('aGVsbG8=')}:${btoa('£')}`);
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/base64".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"aGVsbG8=:hello:ow==");
}

#[tokio::test(flavor = "current_thread")]
async fn structured_clone_deep_copies_objects_and_binary_data() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { fetch() {
              const original = { nested: { value: 1 }, bytes: new Uint8Array([1, 2, 3]) };
              const copied = structuredClone(original);
              original.nested.value = 9;
              original.bytes[0] = 9;
              return new Response(`${copied.nested.value}:${copied.bytes[0]}:${copied.bytes.length}`);
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/clone".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"1:1:3");
}

#[tokio::test(flavor = "current_thread")]
async fn performance_now_and_time_origin_are_available() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { fetch() {
              return new Response(String(typeof performance.now === 'function' && Number.isFinite(performance.timeOrigin) && performance.now() >= 0));
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/performance".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"true");
}

#[tokio::test(flavor = "current_thread")]
async fn web_timers_run_and_clear_inside_dispatch() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default { async fetch() {
              let value = 0;
              const cancelled = setTimeout(() => { value = 99; }, 0);
              clearTimeout(cancelled);
              await new Promise((resolve) => setTimeout(resolve, 0));
              const interval = setInterval(() => { value += 1; }, 0);
              await new Promise((resolve) => setTimeout(resolve, 5));
              clearInterval(interval);
              return new Response(String(value > 0 && value < 99));
            } };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/timers".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"true");
}
