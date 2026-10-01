use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn pure_node_modules_cover_assert_query_os_perf_and_decoder() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import assert, { AssertionError, deepStrictEqual, strictEqual, throws } from "node:assert";
            import querystring from "node:querystring";
            import os from "node:os";
            import punycode from "node:punycode";
            import { performance } from "node:perf_hooks";
            import { StringDecoder } from "node:string_decoder";
            export default {
              fetch() {
                strictEqual(assert.strictEqual, strictEqual);
                deepStrictEqual({ a: [1, 2] }, { a: [1, 2] });
                throws(() => strictEqual(1, 2), AssertionError);
                const parsed = querystring.parse("a=1&a=2&bad=%E0%A4%A");
                const encoded = querystring.stringify({ a: ["1", "2"], space: "a b" });
                const ascii = punycode.toASCII("mañana.com");
                const unicode = punycode.toUnicode(ascii);
                const points = punycode.ucs2.decode("🌍");
                const punyEncoded = punycode.encode("hello");
                const decoder = new StringDecoder("utf8");
                const first = decoder.write(new Uint8Array([0xf0, 0x9f]));
                const second = decoder.end(new Uint8Array([0x8c, 0x8d]));
                return new Response(JSON.stringify({
                  parsed,
                  encoded,
                  platform: os.platform(),
                  arch: os.arch(),
                  tmp: os.tmpdir(),
                  perf: typeof performance.now() === "number" && performance.timeOrigin > 0,
                  punycode: { ascii, unicode, points, roundtrip: punycode.decode(punyEncoded), version: typeof punycode.version },
                  decoded: first + second,
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
                url: "https://worker.invalid/node-pure".into(),
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
            "parsed": { "a": ["1", "2"], "bad": "%E0%A4%A" },
            "encoded": "a=1&a=2&space=a%20b",
            "platform": "linux",
            "arch": "x64",
            "tmp": "/tmp",
            "perf": true,
            "punycode": {
                "ascii": "xn--maana-pta.com",
                "unicode": "xn--maana-pta.com",
                "points": [127_757],
                "roundtrip": "hello",
                "version": "string"
            },
            "decoded": "🌍",
        })
    );
}
