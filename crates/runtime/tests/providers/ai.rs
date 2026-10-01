use super::*;

#[derive(Default)]
struct AiRecordingFetchHost {
    requests: Mutex<Vec<HttpRequest>>,
}

#[async_trait::async_trait]
impl OutboundFetchHost for AiRecordingFetchHost {
    async fn fetch(&self, request: HttpRequest) -> Result<HttpResponse, HostError> {
        self.requests.lock().await.push(request.clone());
        let body = if request.url.ends_with("/embeddings") {
            serde_json::json!({ "data": [{ "embedding": [0.1, 0.2, 0.3] }] })
        } else if request.url.ends_with("/responses") {
            serde_json::json!({ "output_text": "openai text" })
        } else if request.url.ends_with("/messages") {
            serde_json::json!({ "content": [{ "type": "text", "text": "anthropic text" }] })
        } else if request.url.contains(":generateContent") {
            serde_json::json!({ "candidates": [{ "content": { "parts": [{ "text": "gemini text" }] } }] })
        } else {
            serde_json::json!({ "result": { "response": "workers text" } })
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
fn named_ai_provider_env() -> WorkerEnvironment {
    WorkerEnvironment::new(BTreeMap::from([(
        "__perenBindings".to_string(),
        serde_json::json!({
            "OPENAI": {
                "type": "ai",
                "endpoint": "https://api.openai.invalid/v1",
                "route": {
                    "kind": "open_ai",
                    "url": "https://api.openai.invalid/v1",
                    "authorization": "Bearer openai-secret"
                }
            },
            "CLAUDE": {
                "type": "ai",
                "endpoint": "https://api.anthropic.invalid/v1",
                "route": {
                    "kind": "anthropic",
                    "url": "https://api.anthropic.invalid/v1",
                    "xApiKey": "anthropic-secret",
                    "anthropicVersion": "2023-06-01"
                }
            },
            "GEMINI": {
                "type": "ai",
                "endpoint": "https://generativelanguage.googleapis.invalid/v1beta",
                "route": {
                    "kind": "gemini",
                    "url": "https://generativelanguage.googleapis.invalid/v1beta",
                    "apiKey": "gemini-secret"
                }
            },
            "WORKERS_AI": {
                "type": "ai",
                "endpoint": "https://api.cloudflare.invalid/client/v4/accounts/account-secret/ai/run",
                "route": {
                    "kind": "workers_ai",
                    "url": "https://api.cloudflare.invalid/client/v4/accounts/account-secret/ai/run",
                    "authorization": "Bearer workers-secret"
                }
            }
        })
        .to_string(),
    )]))
}

fn named_ai_provider_worker() -> &'static str {
    "export default { async fetch(_request, env) {
      const visible = [
        env.OPENAI.provider.authorization,
        env.CLAUDE.provider.xApiKey,
        env.CLAUDE.provider.anthropicVersion,
        env.GEMINI.provider.apiKey,
        env.WORKERS_AI.provider.authorization,
        env.WORKERS_AI.provider.endpoint,
      ];
      const embedding = await env.OPENAI.embed('text-embedding-3-small', 'hello');
      const openai = await env.OPENAI.generateText('gpt-4.1-mini', 'hello', { maxTokens: 64 });
      const claude = await env.CLAUDE.chat('claude-3-5-sonnet', [{ role: 'user', content: 'hello' }], { maxTokens: 64 });
      const gemini = await env.GEMINI.generateText('gemini-1.5-flash', 'hello', { maxTokens: 64, temperature: 0.1 });
      const workers = await env.WORKERS_AI.generateText('@cf/meta/llama-3.1-8b-instruct', 'hello');
      return Response.json({
        visible,
        embedding: embedding.embeddings[0],
        embeddingRaw: embedding.raw.data[0].embedding,
        text: [openai.text, claude.text, gemini.text, workers.text],
        raw: [openai.raw.output_text, claude.raw.content[0].text, gemini.raw.candidates[0].content.parts[0].text, workers.raw.result.response],
      });
    } };"
}

fn assert_openai_request(request: &HttpRequest) {
    assert_eq!(request.url, "https://api.openai.invalid/v1/responses");
    assert!(request.headers.contains(&(
        "authorization".to_string(),
        "Bearer openai-secret".to_string()
    )));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({ "model": "gpt-4.1-mini", "input": "hello", "max_output_tokens": 64 })
    );
}

fn assert_openai_embedding_request(request: &HttpRequest) {
    assert_eq!(request.url, "https://api.openai.invalid/v1/embeddings");
    assert!(request.headers.contains(&(
        "authorization".to_string(),
        "Bearer openai-secret".to_string()
    )));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({ "model": "text-embedding-3-small", "input": "hello" })
    );
}

fn assert_anthropic_request(request: &HttpRequest) {
    assert_eq!(request.url, "https://api.anthropic.invalid/v1/messages");
    assert!(
        request
            .headers
            .contains(&("x-api-key".to_string(), "anthropic-secret".to_string()))
    );
    assert!(
        request
            .headers
            .contains(&("anthropic-version".to_string(), "2023-06-01".to_string()))
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "model": "claude-3-5-sonnet",
            "messages": [{ "role": "user", "content": "hello" }],
            "max_tokens": 64
        })
    );
}

fn assert_gemini_request(request: &HttpRequest) {
    assert_eq!(
        request.url,
        "https://generativelanguage.googleapis.invalid/v1beta/models/gemini-1.5-flash:generateContent?key=gemini-secret"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "contents": [{ "role": "user", "parts": [{ "text": "hello" }] }],
            "generationConfig": { "maxOutputTokens": 64, "temperature": 0.1 }
        })
    );
}

fn assert_workers_ai_request(request: &HttpRequest) {
    assert_eq!(
        request.url,
        "https://api.cloudflare.invalid/client/v4/accounts/account-secret/ai/run/%40cf%2Fmeta%2Fllama-3.1-8b-instruct"
    );
    assert!(request.headers.contains(&(
        "authorization".to_string(),
        "Bearer workers-secret".to_string()
    )));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({ "prompt": "hello" })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn named_ai_provider_routes_shape_requests_without_exposing_credentials() {
    let fetch = Arc::new(AiRecordingFetchHost::default());
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(named_ai_provider_worker()),
        limits(),
        named_ai_provider_env(),
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
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "visible": [null, null, null, null, null, "https://api.cloudflare.invalid/client/v4/accounts/redacted/ai/run"],
            "embedding": [0.1, 0.2, 0.3],
            "embeddingRaw": [0.1, 0.2, 0.3],
            "text": ["openai text", "anthropic text", "gemini text", "workers text"],
            "raw": ["openai text", "anthropic text", "gemini text", "workers text"]
        })
    );

    let requests = fetch.requests.lock().await;
    assert_eq!(requests.len(), 5);
    assert_openai_embedding_request(&requests[0]);
    assert_openai_request(&requests[1]);
    assert_anthropic_request(&requests[2]);
    assert_gemini_request(&requests[3]);
    assert_workers_ai_request(&requests[4]);
}
