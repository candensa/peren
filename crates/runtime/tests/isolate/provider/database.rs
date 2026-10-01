use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn native_d1_binding_runs_sql_statements() {
    let mut values = BTreeMap::new();
    values.insert(
        "__perenBindings".to_string(),
        r#"{"DB":{"type":"d1"}}"#.to_string(),
    );
    let host = Arc::new(SqlHost::new());
    let mut runtime = WorkerRuntime::load_with_environment(
        bundle(
            "export default { async fetch(_request, env) {
                await env.DB.prepare('CREATE TABLE records(id INTEGER PRIMARY KEY, label TEXT)').run();
                await env.DB.prepare('INSERT INTO records(label) VALUES (?)').bind('alpha').run();
                const row = await env.DB.prepare('SELECT id,label FROM records WHERE label=?').bind('alpha').first();
                return new Response(`${row.id}:${row.label}`);
            } };",
        ),
        limits(),
        WorkerEnvironment::new(values),
        host.clone(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://example.com/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"1:alpha");
    assert_eq!(
        host.databases.lock().await.as_slice(),
        [
            Some("DB".to_string()),
            Some("DB".to_string()),
            Some("DB".to_string())
        ]
    );
}
