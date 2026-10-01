use super::super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn supported_runtime_globals_are_exposed_to_worker_code() {
    let names = CAPABILITIES
        .iter()
        .filter(|capability| {
            capability.kind == CapabilityKind::Global
                && capability.status == CapabilityStatus::Supported
        })
        .map(|capability| capability.name)
        .collect::<Vec<_>>();
    let names = serde_json::to_string(&names).unwrap();
    let mut runtime = WorkerRuntime::load(
        bundle(&format!(
            "const names = {names};
             export default {{
               fetch() {{
                 const missing = names.filter((name) => typeof globalThis[name] === 'undefined');
                 return new Response(JSON.stringify(missing));
               }}
             }};"
        )),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    let missing = serde_json::from_slice::<Vec<String>>(&response.body).unwrap();

    assert!(missing.is_empty(), "missing supported globals: {missing:?}");
}
