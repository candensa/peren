use super::super::support::*;

struct VectorFetchHost;

#[async_trait::async_trait]
impl OutboundFetchHost for VectorFetchHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        let body = if request.url.contains("pinecone.invalid/query") {
            br#"{"matches":[{"id":"pine","score":0.91,"values":[1,0],"metadata":{"title":"Pine"}}]}"#.to_vec()
        } else if request.url.contains("pinecone.invalid/vectors/fetch") {
            br#"{"vectors":{"pine":{"id":"pine","values":[1,0],"metadata":{"title":"Pine"}}}}"#
                .to_vec()
        } else if request.url.contains("weaviate.invalid/v1/graphql")
            && String::from_utf8_lossy(&request.body).contains("PerenVectorSearch")
        {
            br#"{"data":{"Get":{"Article":[{"title":"Weaviate","_additional":{"id":"weaviate","distance":0.2,"vector":[0,1]}}]}}}"#.to_vec()
        } else if request.url.contains("weaviate.invalid/v1/graphql") {
            br#"{"data":{"Get":{"Article":[{"title":"Weaviate","_additional":{"id":"weaviate","vector":[0,1]}}]}}}"#.to_vec()
        } else {
            br"{}".to_vec()
        };
        Ok(HttpResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body,
            upgrade: false,
            websocket_id: None,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn local_vector_binding_persists_records_in_cell_storage() {
    let environment = WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        r#"{"VECTORS":{"type":"vectorize","index":"docs","route":{"kind":"local","index":"docs"}}}"#
            .to_string(),
    )]));
    let host = Arc::new(SqlHost::new());
    let mut writer = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                await env.VECTORS.upsert([
                  { id: 'intro', values: [1, 0, 0], metadata: { title: 'Intro', section: 'guide' }, namespace: 'docs' },
                  { id: 'other', values: [0, 1, 0], metadata: { title: 'Other', section: 'api' }, namespace: 'docs' }
                ]);
                return new Response('stored');
            } };",
        ),
        limits(),
        environment.clone(),
        host.clone(),
    )
    .await
    .unwrap();

    let stored = writer
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://example.com/write".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();
    assert_eq!(stored.body, b"stored");

    let mut reader = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                const query = await env.VECTORS.query([1, 0, 0], { topK: 1, returnMetadata: true, returnValues: true, filter: { section: 'guide' } });
                const filtered = await env.VECTORS.query([1, 0, 0], { topK: 5, returnMetadata: true, filter: { section: 'missing' } });
                const records = await env.VECTORS.getByIds(['intro']);
                await env.VECTORS.deleteByIds(['intro']);
                const after = await env.VECTORS.getByIds(['intro']);
                return Response.json({ query, filtered, records, after });
            } };",
        ),
        limits(),
        environment,
        host,
    )
    .await
    .unwrap();

    let response = reader
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://example.com/read".into(),
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
            "query": {
                "matches": [{
                    "id": "intro",
                    "score": 1,
                    "values": [1, 0, 0],
                    "metadata": { "title": "Intro", "section": "guide" }
                }],
                "count": 1
            },
            "filtered": { "matches": [], "count": 0 },
            "records": [{
                "id": "intro",
                "values": [1, 0, 0],
                "metadata": { "title": "Intro", "section": "guide" },
                "namespace": "docs"
            }],
            "after": []
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_vector_bindings_normalize_external_results() {
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            "export default { async fetch(_request, env) {
                const pineQuery = await env.PINE.query([1, 0], { topK: 1, returnMetadata: true, returnValues: true });
                const pineRecords = await env.PINE.getByIds(['pine']);
                const pineDelete = await env.PINE.deleteByIds(['pine']);
                const weaviateQuery = await env.WEAVIATE.query([0, 1], { topK: 1, returnMetadata: true, returnValues: true });
                const weaviateRecords = await env.WEAVIATE.getByIds(['weaviate']);
                const weaviateDelete = await env.WEAVIATE.deleteByIds(['weaviate']);
                return Response.json({ pineQuery, pineRecords, pineDelete, weaviateQuery, weaviateRecords, weaviateDelete });
            } };",
        ),
        limits(),
        WorkerEnvironment::new(BTreeMap::from([(
            "__perenBindings".to_string(),
            r#"{
              "PINE":{"type":"vectorize","index":"docs","route":{"kind":"pinecone","url":"https://pinecone.invalid","index":"docs","apiKey":"secret","namespace":"docs"}},
              "WEAVIATE":{"type":"vectorize","index":"articles","route":{"kind":"weaviate","url":"https://weaviate.invalid","className":"Article","apiKey":"secret"}}
            }"#
            .to_string(),
        )])),
        Capabilities {
            storage: Arc::new(Host::default()),
            fetch: Some(Arc::new(VectorFetchHost)),
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
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://example.com/vectors".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(8192, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "pineQuery": {
                "matches": [{ "id": "pine", "score": 0.91, "values": [1, 0], "metadata": { "title": "Pine" } }],
                "count": 1
            },
            "pineRecords": [{ "id": "pine", "values": [1, 0], "metadata": { "title": "Pine" } }],
            "pineDelete": { "count": 1 },
            "weaviateQuery": {
                "matches": [{ "id": "weaviate", "score": 0.8, "values": [0, 1], "metadata": { "title": "Weaviate" } }],
                "count": 1
            },
            "weaviateRecords": [{ "id": "weaviate", "values": [0, 1], "metadata": { "title": "Weaviate" } }],
            "weaviateDelete": { "count": 1 }
        })
    );
}
