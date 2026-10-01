use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_url_module_covers_whatwg_legacy_and_file_helpers() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import url, { URL, parse, format, resolve, fileURLToPath, pathToFileURL, urlToHttpOptions, domainToASCII } from "node:url";
            export default {
              fetch() {
                const parsed = parse("https://user@example.com:8443/a/b?x=1&x=2#top", true);
                const file = pathToFileURL("/tmp/a b.txt");
                const options = urlToHttpOptions(new URL("https://user:pass@example.com:8443/path?q=1"));
                return new Response(JSON.stringify({
                  defaultHasURL: url.URL === URL,
                  parsed: { protocol: parsed.protocol, auth: parsed.auth, host: parsed.host, pathname: parsed.pathname, query: parsed.query, hash: parsed.hash },
                  formatted: format({ protocol: "https:", hostname: "example.com", pathname: "/ok", query: { a: "1" } }),
                  resolved: resolve("https://example.com/a/b", "../c"),
                  filePath: fileURLToPath(file),
                  fileHref: file.href,
                  options: { protocol: options.protocol, hostname: options.hostname, port: options.port, path: options.path, auth: options.auth },
                  ascii: domainToASCII("täst.example"),
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
                url: "https://worker.invalid/node-url".into(),
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
            "defaultHasURL": true,
            "parsed": {
                "protocol": "https:",
                "auth": "user",
                "host": "example.com:8443",
                "pathname": "/a/b",
                "query": { "x": ["1", "2"] },
                "hash": "#top"
            },
            "formatted": "https://example.com/ok?a=1",
            "resolved": "https://example.com/c",
            "filePath": "/tmp/a b.txt",
            "fileHref": "file:///tmp/a%20b.txt",
            "options": { "protocol": "https:", "hostname": "example.com", "port": 8443, "path": "/path?q=1", "auth": "user:pass" },
            "ascii": "xn--tst-qla.example",
        })
    );
}
