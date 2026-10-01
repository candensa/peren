use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn cache_api_matches_puts_deletes_and_opens_named_caches() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default {
              async fetch() {
                const request = new Request('https://cache.invalid/item');
                const before = await caches.default.match(request);
                await caches.default.put(request, new Response('cached', { status: 203, headers: { 'x-cache': 'text' } }));
                const hit = await caches.default.match(request);
                const named = await caches.open('named');
                await named.put('https://cache.invalid/item', new Response('named'));
                const namedHit = await named.match('https://cache.invalid/item');
                const queryHit = await named.match('https://cache.invalid/item?version=2', { ignoreSearch: true });
                const postHit = await named.match(new Request('https://cache.invalid/item?version=2', { method: 'POST' }), { ignoreMethod: true, ignoreSearch: true });
                const putFailures = [];
                for (const pair of [
                  [new Request('https://cache.invalid/post', { method: 'POST' }), new Response('x')],
                  [new Request('https://cache.invalid/range'), new Response('x', { status: 206 })],
                  [new Request('https://cache.invalid/vary'), new Response('x', { headers: { vary: '*' } })],
                ]) {
                  try {
                    await caches.default.put(pair[0], pair[1]);
                    putFailures.push('accepted');
                  } catch (error) {
                    putFailures.push(error instanceof TypeError);
                  }
                }
                const deleted = await caches.default.delete(request);
                const after = await caches.default.match(request);
                return new Response(JSON.stringify({
                  storage: caches instanceof CacheStorage,
                  defaultCache: caches.default instanceof Cache,
                  namedCache: named instanceof Cache,
                  constructorName: Object.getPrototypeOf(named).constructor.name,
                  before: before === undefined,
                  status: hit.status,
                  header: hit.headers.get('x-cache'),
                  body: await hit.text(),
                  named: await namedHit.text(),
                  query: await queryHit.text(),
                  post: await postHit.text(),
                  putFailures,
                  deleted,
                  after: after === undefined,
                }));
              }
            };",
        ),
        limits(),
        WorkerEnvironment::empty(),
        Capabilities {
            storage: Arc::new(Host::default()),
            fetch: None,
            queue: None,
            r2: None,
            service: None,
            durable: None,
            cache: Some(Arc::new(CacheMap::default())),
            kv: None,
            ai: None,
        },
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/cache".into(),
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
            "storage": true,
            "defaultCache": true,
            "namedCache": true,
            "constructorName": "Cache",
            "before": true,
            "status": 203,
            "header": "text",
            "body": "cached",
            "named": "named",
            "query": "named",
            "post": "named",
            "putFailures": [true, true, true],
            "deleted": true,
            "after": true,
        })
    );
}

#[tokio::test]
async fn cache_api_matches_vary_request_headers() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "
            export default {
              async fetch() {
                const english = new Request('https://cache.invalid/vary', { headers: { 'accept-language': 'en' } });
                const french = new Request('https://cache.invalid/vary', { headers: { 'accept-language': 'fr' } });
                await caches.default.put(english, new Response('hello', { headers: { vary: 'accept-language' } }));
                await caches.default.put(french, new Response('bonjour', { headers: { vary: 'accept-language' } }));
                const englishHit = await caches.default.match(english);
                const frenchHit = await caches.default.match(french);
                const missingLanguage = await caches.default.match('https://cache.invalid/vary');
                const ignoredVary = await caches.default.match('https://cache.invalid/vary', { ignoreVary: true });
                return Response.json({
                  english: await englishHit.text(),
                  french: await frenchHit.text(),
                  missingLanguage: missingLanguage === undefined,
                  ignoredVary: await ignoredVary.text(),
                });
              }
            };",
        ),
        limits(),
        WorkerEnvironment::empty(),
        Capabilities {
            storage: Arc::new(Host::default()),
            fetch: None,
            queue: None,
            r2: None,
            service: None,
            durable: None,
            cache: Some(Arc::new(CacheMap::default())),
            kv: None,
            ai: None,
        },
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/cache".into(),
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
            "english": "hello",
            "french": "bonjour",
            "missingLanguage": true,
            "ignoredVary": "hello",
        })
    );
}
