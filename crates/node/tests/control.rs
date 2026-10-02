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
use std::time::{SystemTime, UNIX_EPOCH};

const NODE: &str = "00000000-0000-0000-0000-000000000001";

#[tokio::test]
async fn peer_control_api_certifies_drain_state_before_storage_removal() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let config = config::signed_control_worker(worker.path());
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

    let unsigned = post(peer, "/control/v1/node/drain", "").await;
    assert!(unsigned.starts_with("HTTP/1.1 401"), "{unsigned}");

    let premature = signed_post(&environment, peer, "/control/v1/node/retire", "").await;
    assert!(premature.starts_with("HTTP/1.1 409"), "{premature}");
    let premature = json_body(&premature);
    assert_eq!(premature["disk_removal_safe"], false);
    assert_eq!(premature["retired"], false);

    let drained = signed_post(&environment, peer, "/control/v1/node/drain", "").await;
    assert!(drained.starts_with("HTTP/1.1 200"), "{drained}");
    let drained = json_body(&drained);
    assert_eq!(drained["admission"], "draining");
    assert_eq!(drained["ready"], false);
    assert_eq!(drained["active"], 0);
    assert_eq!(drained["disk_removal_safe"], true);
    assert_eq!(drained["retired"], false);
    assert_eq!(drained["incarnation"], before["incarnation"]);

    let retired = signed_post(&environment, peer, "/control/v1/node/retire", "").await;
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
async fn peer_control_mutations_remain_unsigned_by_default_for_compatibility() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let config = config::worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let peer = process.listeners()["peer"];

    let drained = post(peer, "/control/v1/node/drain", "").await;
    assert!(drained.starts_with("HTTP/1.1 200"), "{drained}");

    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn peer_control_mutations_reject_replay_and_tampering() {
    let worker =
        TestWorker::from_source("export default { async fetch() { return new Response('ok'); } };");
    let environment = DataEnv::new();
    let config = config::signed_control_worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let peer = process.listeners()["peer"];

    let nonce = unique_nonce();
    let first =
        signed_post_with_nonce(&environment, peer, "/control/v1/node/control", "", &nonce).await;
    assert!(first.starts_with("HTTP/1.1 200"), "{first}");

    let replay =
        signed_post_with_nonce(&environment, peer, "/control/v1/node/control", "", &nonce).await;
    assert!(replay.starts_with("HTTP/1.1 401"), "{replay}");

    let tampered = signed_post_with_signed_body(
        &environment,
        peer,
        "/control/v1/deployments",
        r#"{"percent":25}"#,
        r#"{"percent":26}"#,
    )
    .await;
    assert!(tampered.starts_with("HTTP/1.1 401"), "{tampered}");

    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn peer_control_api_exposes_deployment_engine_without_cli_shelling() {
    let worker = TestWorker::from_source(
        "export default { async fetch() { return new Response('deployable'); } };",
    );
    let environment = DataEnv::new();
    let config = config::signed_control_worker(worker.path());
    let process = Process::start(config, &environment).await.unwrap();
    let public = process.listeners()["public"];
    let peer = process.listeners()["peer"];

    let hidden = get(public, "/control/v1/deployments").await;
    assert!(hidden.starts_with("HTTP/1.1 404"), "{hidden}");
    let hidden_mutation = signed_post(
        &environment,
        public,
        "/control/v1/deployments",
        r#"{"percent":25}"#,
    )
    .await;
    assert!(
        hidden_mutation.starts_with("HTTP/1.1 404"),
        "{hidden_mutation}"
    );

    let recorded = signed_post(
        &environment,
        peer,
        "/control/v1/deployments",
        r#"{"percent":25}"#,
    )
    .await;
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

async fn signed_post(
    environment: &DataEnv,
    address: std::net::SocketAddr,
    path: &str,
    body: &str,
) -> String {
    let nonce = unique_nonce();
    signed_post_with_nonce(environment, address, path, body, &nonce).await
}

async fn signed_post_with_nonce(
    environment: &DataEnv,
    address: std::net::SocketAddr,
    path: &str,
    body: &str,
    nonce: &str,
) -> String {
    signed_post_request(environment, address, path, body, body, nonce).await
}

async fn signed_post_with_signed_body(
    environment: &DataEnv,
    address: std::net::SocketAddr,
    path: &str,
    signed_body: &str,
    sent_body: &str,
) -> String {
    let nonce = unique_nonce();
    signed_post_request(environment, address, path, signed_body, sent_body, &nonce).await
}

async fn signed_post_request(
    environment: &DataEnv,
    address: std::net::SocketAddr,
    path: &str,
    signed_body: &str,
    sent_body: &str,
    nonce: &str,
) -> String {
    let timestamp_ms = now_ms();
    let signed = peren_security::sign_control_request(
        &environment.path().join("credentials"),
        &peren_security::ControlRequest {
            target_node: NODE,
            method: "POST",
            target: path,
            body: signed_body.as_bytes(),
            timestamp_ms,
            nonce,
        },
    )
    .unwrap();
    request(
        address,
        &format!(
            "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\nx-peren-target-node: {NODE}\r\nx-peren-request-timestamp-ms: {timestamp_ms}\r\nx-peren-nonce: {nonce}\r\nx-peren-signature-version: {}\r\nx-peren-signature: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{sent_body}",
            signed.version,
            signed.signature,
            sent_body.len()
        ),
    )
    .await
}

fn unique_nonce() -> String {
    format!("nonce-{}", uuid::Uuid::new_v4())
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn json_body(response: &str) -> serde_json::Value {
    let body = response.split("\r\n\r\n").nth(1).unwrap_or(response);
    serde_json::from_str(body.trim())
        .unwrap_or_else(|error| panic!("invalid json body: {error}: {response}"))
}
