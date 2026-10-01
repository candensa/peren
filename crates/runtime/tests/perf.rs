use std::{collections::BTreeMap, time::Duration};

use peren_runtime::{
    HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle,
    WorkerRuntime,
};

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance benchmark; run through make perf or cargo test --test perf -- --ignored"]
async fn http_dispatch_stays_within_the_release_budget() {
    let bundle = WorkerBundle::new(
        ModuleName::parse("main.js").unwrap(),
        BTreeMap::from([(
            ModuleName::parse("main.js").unwrap(),
            Module::new(
                ModuleKind::JavaScript,
                r"
                export default {
                  async fetch(request) {
                    const body = await request.text();
                    const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(body));
                    return new Response(String(new Uint8Array(digest)[0]), { headers: { 'x-perf': 'runtime' } });
                  }
                };
                "
                .as_bytes(),
            )
            .unwrap(),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(
        bundle,
        IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(10)),
    )
    .await
    .unwrap();
    let request = HttpRequest {
        method: "POST".into(),
        url: "https://worker.invalid/perf".into(),
        headers: Vec::new(),
        body: b"peren performance benchmark".to_vec(),
        mtls: None,
    };

    let warmup = 10;
    for _ in 0..warmup {
        runtime
            .dispatch_http(request.clone(), InvocationLimits::new(1024, 10))
            .await
            .unwrap();
    }

    let iterations = 100;
    let started = std::time::Instant::now();
    for _ in 0..iterations {
        let response = runtime
            .dispatch_http(request.clone(), InvocationLimits::new(1024, 10))
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert!(
            response
                .headers
                .iter()
                .any(|header| header == &("x-perf".into(), "runtime".into()))
        );
    }
    let elapsed = started.elapsed();
    let average = elapsed / iterations;
    println!("runtime http dispatch: {iterations} iterations in {elapsed:?}; average {average:?}");

    assert!(
        average < Duration::from_millis(25),
        "runtime HTTP dispatch average {average:?} exceeded release sanity budget"
    );
}
