mod binding;
mod route;

pub(super) use binding::{environments, has_binding};
pub(super) use route::{d1_routes, kv_routes, r2_routes};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Providers, fixture::DataEnv};
    use peren_config::{Binding, D1Backend, FleetConfig, ValidatedConfig};

    #[test]
    fn turso_d1_binding_hydrates_env_and_route_metadata() {
        let worker = worker("export default { fetch() { return new Response('ok'); } };");
        let env_state = DataEnv::temp("node-data")
            .with("TURSO_URL", "libsql://example.invalid")
            .with("TURSO_TOKEN", "secret-token");
        let mut config = config(&worker);
        config.raw.services[0].bindings.insert(
            "DB".into(),
            Binding::D1Database {
                database_name: "db".into(),
                unique_key: "db-key".into(),
                backend: D1Backend::Turso {
                    url_env: "TURSO_URL".into(),
                    token_env: "TURSO_TOKEN".into(),
                    replica_path: Some("d1/db.sqlite".into()),
                },
            },
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let providers = runtime
            .block_on(Providers::build(&config, &env_state))
            .unwrap();
        let env = binding::environment(
            peren_primitives::NodeId::from_uuid(config.raw.node.id),
            &config.raw.services[0],
            &config.raw.dispatch_namespaces,
            &config.raw.cache,
            &providers,
            &serde_json::json!({ "kind": "memory" }),
        )
        .unwrap();
        let routes = d1_routes(&config.raw.services, &providers, env_state.path()).unwrap();

        assert_eq!(
            env.get("__perenBindings"),
            Some(r#"{"DB":{"type":"d1","provider":{"kind":"turso","replica":true}}}"#)
        );
        let route = &routes["api"]["DB"];
        assert_eq!(route.url.as_ref(), "libsql://example.invalid");
        assert_eq!(route.token.as_ref(), "secret-token");
        assert_eq!(route.replica, Some(env_state.path().join("d1/db.sqlite")));
        cleanup(&worker, &env_state);
    }

    fn worker(source: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("peren-node-worker-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("worker.js");
        std::fs::write(&path, source).unwrap();
        path
    }

    fn config(worker: &std::path::Path) -> ValidatedConfig {
        FleetConfig::from_toml(&format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:0"
service = "api"
"#,
            worker.display()
        ))
        .unwrap()
        .validate()
        .unwrap()
    }

    fn cleanup(worker: &std::path::Path, environment: &DataEnv) {
        std::fs::remove_dir_all(worker.parent().unwrap()).unwrap();
        std::fs::remove_dir_all(environment.path()).ok();
    }
}
