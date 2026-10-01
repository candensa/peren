use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_buffer_module_covers_common_encoding_and_mutation() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import buffer, { Buffer } from "node:buffer";
            export default {
              fetch() {
                const hello = Buffer.from("hello", "utf8");
                const hex = hello.toString("hex");
                const b64 = hello.toString("base64");
                const fromHex = Buffer.from(hex, "hex").toString();
                const fromB64 = Buffer.from(b64, "base64").toString();
                const concat = Buffer.concat([Buffer.from("foo"), Buffer.from("bar")]).toString();
                const filled = Buffer.alloc(4).fill("ab").toString();
                const written = Buffer.alloc(5);
                written.write("word");
                return new Response(JSON.stringify({
                  defaultHasBuffer: buffer.Buffer === Buffer,
                  isBuffer: Buffer.isBuffer(hello),
                  len: Buffer.byteLength("hello"),
                  hex,
                  b64,
                  fromHex,
                  fromB64,
                  concat,
                  filled,
                  written: written.toString(),
                  compare: Buffer.compare(Buffer.from("a"), Buffer.from("b")),
                  json: hello.toJSON(),
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
                url: "https://worker.invalid/buffer".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "defaultHasBuffer": true,
            "isBuffer": true,
            "len": 5,
            "hex": "68656c6c6f",
            "b64": "aGVsbG8=",
            "fromHex": "hello",
            "fromB64": "hello",
            "concat": "foobar",
            "filled": "abab",
            "written": "word\u{0000}",
            "compare": -1,
            "json": { "type": "Buffer", "data": [104, 101, 108, 108, 111] },
        })
    );
}
