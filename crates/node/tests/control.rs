#[path = "support/config.rs"]
mod config;
#[path = "support/env.rs"]
mod env;

use env::DataEnv;
use peren_node::Process;
use peren_testkit::{
    http::{get, request},
    worker::TestWorker,
};

#[tokio::test]
async fn peer_control_api_certifies_drain_state_before_storage_removal() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let config = config::worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let public = process.listeners()["public"];
    let peer = process.listeners()["peer"];

    let public_control = get(public, "/control/v1/node").await;
    assert!(
        public_control.starts_with("HTTP/1.1 404"),
        "{public_control}"
    );

    let before = get(peer, "/control/v1/node").await;
    assert!(before.starts_with("HTTP/1.1 200"), "{before}");
    let before = json_body(&before);
    assert_eq!(before["admission"], "serving");
    assert_eq!(before["ready"], true);
    assert_eq!(before["disk_removal_safe"], false);
    assert!(
        before["incarnation"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let premature = post(peer, "/control/v1/node/retire", "").await;
    assert!(premature.starts_with("HTTP/1.1 409"), "{premature}");
    let premature = json_body(&premature);
    assert_eq!(premature["disk_removal_safe"], false);
    assert_eq!(premature["retired"], false);

    let drained = post(peer, "/control/v1/node/drain", "").await;
    assert!(drained.starts_with("HTTP/1.1 200"), "{drained}");
    let drained = json_body(&drained);
    assert_eq!(drained["admission"], "draining");
    assert_eq!(drained["ready"], false);
    assert_eq!(drained["active"], 0);
    assert_eq!(drained["disk_removal_safe"], true);
    assert_eq!(drained["retired"], false);
    assert_eq!(drained["incarnation"], before["incarnation"]);

    let retired = post(peer, "/control/v1/node/retire", "").await;
    assert!(retired.starts_with("HTTP/1.1 200"), "{retired}");
    let retired = json_body(&retired);
    assert_eq!(retired["admission"], "control_only");
    assert_eq!(retired["disk_removal_safe"], true);
    assert_eq!(retired["retired"], true);
    assert_eq!(retired["incarnation"], before["incarnation"]);

    let refused = get(public, "/after-drain").await;
    assert!(refused.starts_with("HTTP/1.1 503"), "{refused}");

    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn peer_control_api_exposes_deployment_engine_without_cli_shelling() {
    let worker = TestWorker::from_source(
        "export default { async fetch() { return new Response('deployable'); } };",
    );
    let environment = DataEnv::new();
    let config = config::worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let public = process.listeners()["public"];
    let peer = process.listeners()["peer"];

    let hidden = get(public, "/control/v1/deployments").await;
    assert!(hidden.starts_with("HTTP/1.1 404"), "{hidden}");

    let recorded = post(peer, "/control/v1/deployments", r#"{"percent":25}"#).await;
    assert!(recorded.starts_with("HTTP/1.1 200"), "{recorded}");
    let recorded = json_body(&recorded);
    let digest = recorded["generations"][0]["digest"].as_str().unwrap();
    assert_eq!(recorded["generations"][0]["service"], "api");
    assert_eq!(recorded["generations"][0]["percent"], 25);
    assert_eq!(recorded["generations"][0]["active"], true);

    let listed = get(peer, "/control/v1/deployments?service=api").await;
    assert!(listed.starts_with("HTTP/1.1 200"), "{listed}");
    let listed = json_body(&listed);
    assert_eq!(listed["generations"][0]["digest"], digest);

    let healthy = get(peer, "/control/v1/deployments/health?service=api").await;
    assert!(healthy.starts_with("HTTP/1.1 200"), "{healthy}");
    let healthy = json_body(&healthy);
    assert_eq!(healthy["services"][0]["digest"], digest);
    assert_eq!(healthy["services"][0]["percent"], 25);

    let verified = get(peer, "/control/v1/deployments/verify?service=api").await;
    assert!(verified.starts_with("HTTP/1.1 200"), "{verified}");
    let verified = json_body(&verified);
    assert_eq!(verified["generations"][0]["generation"]["digest"], digest);

    process.shutdown().await.unwrap();
}

async fn post(address: std::net::SocketAddr, path: &str, body: &str) -> String {
    request(
        address,
        &format!(
            "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

fn json_body(response: &str) -> serde_json::Value {
    let body = response.split("\r\n\r\n").nth(1).unwrap_or(response);
    serde_json::from_str(body.trim())
        .unwrap_or_else(|error| panic!("invalid json body: {error}: {response}"))
}
