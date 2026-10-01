use super::*;

#[derive(Default)]
struct KvRecordingHost {
    entries: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl KvRecordingHost {
    fn key(namespace: &str, key: &str) -> String {
        format!("{namespace}/{key}")
    }
}

#[async_trait::async_trait]
impl peren_runtime::KvHost for KvRecordingHost {
    async fn get(&self, request: peren_runtime::KvGet) -> Result<Option<Vec<u8>>, HostError> {
        Ok(self
            .entries
            .lock()
            .await
            .get(&Self::key(&request.namespace, &request.key))
            .cloned())
    }

    async fn put(&self, request: peren_runtime::KvPut) -> Result<(), HostError> {
        self.entries
            .lock()
            .await
            .insert(Self::key(&request.namespace, &request.key), request.value);
        Ok(())
    }

    async fn delete(&self, request: peren_runtime::KvGet) -> Result<bool, HostError> {
        Ok(self
            .entries
            .lock()
            .await
            .remove(&Self::key(&request.namespace, &request.key))
            .is_some())
    }

    async fn list(
        &self,
        request: peren_runtime::KvList,
    ) -> Result<peren_runtime::ListPage, HostError> {
        let namespace = format!("{}/", request.namespace);
        let prefix = request.prefix.unwrap_or_default();
        let cursor = request.cursor.unwrap_or_default();
        let mut keys = self
            .entries
            .lock()
            .await
            .keys()
            .filter_map(|key| key.strip_prefix(&namespace))
            .filter(|key| key.starts_with(&prefix) && *key > cursor.as_str())
            .map(|name| peren_runtime::ListEntry {
                name: name.to_string(),
            })
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(peren_runtime::ListPage {
            keys,
            cursor: None,
            list_complete: true,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn provider_kv_uses_host_operations_and_worker_shape() {
    let kv = Arc::new(KvRecordingHost::default());
    let mut env = BTreeMap::new();
    env.insert(
        "__perenBindings".to_string(),
        serde_json::json!({
            "CACHE": {
                "type": "kv",
                "scope": "sessions",
                "provider": { "kind": "redis" }
            }
        })
        .to_string(),
    );
    let mut runtime = WorkerRuntime::load_with_capabilities(
        bundle(
            r#"
            export default {
              async fetch(_request, env) {
                await env.CACHE.put("user:1", JSON.stringify({ name: "Ada" }), { metadata: { role: "user" } });
                await env.CACHE.put("admin:1", "Grace");
                await env.CACHE.put("short", "gone", { expirationTtl: -1, metadata: { stale: true } });
                const value = await env.CACHE.get("user:1", { type: "json" });
                const record = await env.CACHE.getWithMetadata("user:1", { type: "json" });
                const expired = await env.CACHE.getWithMetadata("short");
                const page = await env.CACHE.list({ prefix: "user:" });
                const deleted = await env.CACHE.delete("user:1");
                const missing = await env.CACHE.get("user:1");
                return Response.json({
                  value,
                  record,
                  expired,
                  keys: page.keys.map((key) => key.name),
                  complete: page.list_complete,
                  missing,
                  provider: env.CACHE.provider.kind,
                });
              }
            };
            "#,
        ),
        limits(),
        WorkerEnvironment::new(env),
        Capabilities {
            storage: Arc::new(Host),
            fetch: None,
            queue: None,
            r2: None,
            service: None,
            durable: None,
            cache: None,
            kv: Some(kv),
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
            "value": { "name": "Ada" },
            "record": { "value": { "name": "Ada" }, "metadata": { "role": "user" }, "version": null },
            "expired": { "value": null, "metadata": null, "version": null },
            "keys": ["user:1"],
            "complete": true,
            "missing": null,
            "provider": "redis"
        })
    );
}
