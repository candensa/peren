use super::*;

#[derive(Default)]
struct RecordingFetchHost {
    requests: Mutex<Vec<HttpRequest>>,
}

#[async_trait::async_trait]
impl OutboundFetchHost for RecordingFetchHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        self.requests.lock().await.push(request.clone());
        let body = if request.url.ends_with("/points/search") {
            serde_json::json!({
                "result": [{
                    "id": "intro",
                    "score": 0.99,
                    "payload": { "title": "Intro" },
                    "vector": [1, 0, 0]
                }]
            })
        } else {
            serde_json::json!({ "result": { "operation_id": 1, "status": "completed" } })
        };
        Ok(HttpResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body: serde_json::to_vec(&body).unwrap(),
            upgrade: false,
            websocket_id: None,
        })
    }
}

fn named_vector_provider_env() -> WorkerEnvironment {
    WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        serde_json::json!({
            "PINECONE": {
                "type": "vectorize",
                "index": "docs",
                "route": {
                    "kind": "pinecone",
                    "url": "https://pinecone.invalid",
                    "namespace": "tenant-a",
                    "apiKey": "pinecone-secret"
                }
            },
            "WEAVIATE": {
                "type": "vectorize",
                "index": "articles",
                "route": {
                    "kind": "weaviate",
                    "url": "https://weaviate.invalid",
                    "className": "Article",
                    "apiKey": "weaviate-secret"
                }
            }
        })
        .to_string(),
    )]))
}

fn named_vector_provider_worker() -> &'static str {
    "export default { async fetch(_request, env) {
      await env.PINECONE.upsert([{ id: 'doc-1', values: [0.1, 0.2], metadata: { source: 'guide' } }]);
      await env.PINECONE.query([0.1, 0.2], { topK: 5, returnMetadata: true, returnValues: true, filter: { source: 'guide' } });
      await env.WEAVIATE.upsert([{ id: 'article-1', values: [0.3, 0.4], metadata: { title: 'Intro' } }]);
      await env.WEAVIATE.query([0.3, 0.4], { topK: 3, filter: { title: 'Intro', published: true } });
      return Response.json({ ok: true });
    } };"
}

fn assert_pinecone_vector_requests(upsert: &HttpRequest, query: &HttpRequest) {
    assert_eq!(upsert.url, "https://pinecone.invalid/vectors/upsert");
    assert!(
        upsert
            .headers
            .contains(&("api-key".to_string(), "pinecone-secret".to_string()))
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&upsert.body).unwrap(),
        serde_json::json!({
            "vectors": [{ "id": "doc-1", "values": [0.1, 0.2], "metadata": { "source": "guide" } }],
            "namespace": "tenant-a"
        })
    );
    assert_eq!(query.url, "https://pinecone.invalid/query");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&query.body).unwrap(),
        serde_json::json!({
            "vector": [0.1, 0.2],
            "topK": 5,
            "namespace": "tenant-a",
            "includeMetadata": true,
            "includeValues": true,
            "filter": { "source": "guide" }
        })
    );
}

fn assert_weaviate_vector_requests(upsert: &HttpRequest, query: &HttpRequest) {
    assert_eq!(upsert.url, "https://weaviate.invalid/v1/batch/objects");
    assert!(
        upsert
            .headers
            .contains(&("api-key".to_string(), "weaviate-secret".to_string()))
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&upsert.body).unwrap(),
        serde_json::json!({
            "objects": [{
                "class": "Article",
                "id": "article-1",
                "vector": [0.3, 0.4],
                "properties": { "title": "Intro" }
            }]
        })
    );
    assert_eq!(query.url, "https://weaviate.invalid/v1/graphql");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&query.body).unwrap(),
        serde_json::json!({
            "query": "query PerenVectorSearch($vector:[Float!]!,$limit:Int!,$where:WhereInput){ Get { Article(nearVector:{vector:$vector}, limit:$limit, where:$where){ _additional { id distance vector } } } }",
            "variables": {
                "vector": [0.3, 0.4],
                "limit": 3,
                "where": {
                    "operator": "And",
                    "operands": [
                        { "path": ["title"], "operator": "Equal", "valueText": "Intro" },
                        { "path": ["published"], "operator": "Equal", "valueBoolean": true }
                    ]
                }
            }
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn named_vector_providers_route_native_request_shapes() {
    let fetch = Arc::new(RecordingFetchHost::default());
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(named_vector_provider_worker()),
        limits(),
        named_vector_provider_env(),
        Capabilities {
            storage: Arc::new(Host),
            fetch: Some(fetch.clone()),
            queue: None,
            r2: None,
            service: None,
            durable: None,
            cache: None,
            kv: None,
            ai: None,
        },
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(worker_request(), InvocationLimits::new(1024, 10))
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    let requests = fetch.requests.lock().await;
    assert_eq!(requests.len(), 4);
    assert_pinecone_vector_requests(&requests[0], &requests[1]);
    assert_weaviate_vector_requests(&requests[2], &requests[3]);
}

#[tokio::test(flavor = "current_thread")]
async fn qdrant_vector_route_uses_native_provider_shape() {
    let fetch = Arc::new(RecordingFetchHost::default());
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default { async fetch(_request, env) {
              const upsert = await env.VECTORS.upsert([{ id: 'intro', values: [1, 0, 0], metadata: { title: 'Intro' } }]);
              const query = await env.VECTORS.query([1, 0, 0], { topK: 2, returnMetadata: true, returnValues: true, filter: { title: 'Intro' } });
              return Response.json({ provider: env.VECTORS.provider, upsert, query });
            } };",
        ),
        limits(),
        WorkerEnvironment::new(BTreeMap::from([(
            "__perenBindings".to_string(),
            serde_json::json!({
                "VECTORS": {
                    "type": "vectorize",
                    "index": "docs",
                    "route": {
                        "kind": "qdrant",
                        "url": "https://qdrant.invalid",
                        "collection": "docs",
                        "apiKey": "qdrant-secret"
                    }
                }
            })
            .to_string(),
        )])),
        Capabilities {
            storage: Arc::new(Host),
            fetch: Some(fetch.clone()),
            queue: None,
            r2: None,
            service: None,
            durable: None,
            cache: None,
            kv: None,
            ai: None,
        },
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(worker_request(), InvocationLimits::new(2048, 10))
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "provider": { "kind": "qdrant", "endpoint": "https://qdrant.invalid", "collection": "docs" },
            "upsert": { "count": 1 },
            "query": { "matches": [{ "id": "intro", "score": 0.99, "metadata": { "title": "Intro" }, "values": [1, 0, 0] }], "count": 1 }
        })
    );

    let requests = fetch.requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].url,
        "https://qdrant.invalid/collections/docs/points?wait=true"
    );
    assert!(
        requests[0]
            .headers
            .contains(&("api-key".into(), "qdrant-secret".into()))
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&requests[0].body).unwrap(),
        serde_json::json!({
            "points": [{ "id": "intro", "vector": [1, 0, 0], "payload": { "title": "Intro" } }]
        })
    );
    assert_eq!(
        requests[1].url,
        "https://qdrant.invalid/collections/docs/points/search"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&requests[1].body).unwrap(),
        serde_json::json!({
            "vector": [1, 0, 0],
            "limit": 2,
            "with_payload": true,
            "with_vector": true,
            "filter": { "must": [{ "key": "title", "match": { "value": "Intro" } }] }
        })
    );
}
