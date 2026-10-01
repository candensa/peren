use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn dispatches_web_requests_and_collects_web_responses() {
    let mut runtime = WorkerRuntime::load(bundle(
        "export default { async fetch(request) { const body = await request.text(); return new Response(`${request.method} ${new URL(request.url).pathname}: ${body}`, { status: 201, headers: { 'x-worker': 'text' } }); } };",
    ), limits())
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "POST".into(),
                url: "https://worker.invalid/tasks".into(),
                headers: vec![("content-type".into(), "text/plain".into())],
                body: b"accepted".to_vec(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 201);
    assert_eq!(response.body, b"POST /tasks: accepted");
    assert!(
        response
            .headers
            .contains(&("x-worker".into(), "text".into()))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn fetch_wait_until_drains_background_work_before_completion() {
    let host = Arc::new(Host::default());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async fetch(_request, _env, ctx) {
                ctx.waitUntil(Peren.storage.transaction(async (storage) => {
                  await storage.put('background', new Uint8Array([42]));
                }));
                return new Response('accepted');
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/background".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.body, b"accepted");
    assert_eq!(runtime.committed_revision(), StorageRevision::new(7));
}
