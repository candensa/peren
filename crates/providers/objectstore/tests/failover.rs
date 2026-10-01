use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use peren_cell::{CellState, WorkerCell};
use peren_fleet::{RecoveryObservation, RecoveryPolicy};
use peren_primitives::{CellId, NodeId, OwnershipEpoch};
use peren_provider_object_store::{
    AzureCredentials, AzureOptions, BucketStore, S3Credentials, S3Options,
};
use peren_replication::ReplicaRepository;
use peren_runtime::{
    HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle,
};
use uuid::Uuid;
use wait_timeout::ChildExt;

const NODE_A: &str = "11111111-1111-4111-8111-111111111111";
const NODE_B: &str = "22222222-2222-4222-8222-222222222222";
const WITNESS_A: &str = "33333333-3333-4333-8333-333333333333";
const WITNESS_B: &str = "44444444-4444-4444-8444-444444444444";

fn store() -> BucketStore {
    match std::env::var("PEREN_PROOF_PROVIDER").as_deref() {
        Ok("azure") => BucketStore::azure(
            AzureOptions {
                account: required_env("PEREN_AZURE_ACCOUNT"),
                container: required_env("PEREN_AZURE_CONTAINER"),
                endpoint: Some(required_env("PEREN_AZURE_ENDPOINT")),
                emulator: std::env::var("PEREN_AZURE_EMULATOR")
                    .is_ok_and(|value| value == "1" || value == "true"),
            },
            AzureCredentials::new(required_env("PEREN_AZURE_ACCESS_KEY")),
        )
        .unwrap(),
        _ => BucketStore::s3(
            S3Options {
                bucket: required_env("PEREN_S3_BUCKET"),
                region: "us-east-1".to_owned(),
                endpoint: Some(required_env("PEREN_S3_ENDPOINT")),
                allow_http: true,
                virtual_hosted: false,
            },
            S3Credentials::new(
                required_env("PEREN_S3_ACCESS_KEY"),
                required_env("PEREN_S3_SECRET_KEY"),
                None,
            ),
        )
        .unwrap(),
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set for the object-store failover proof"))
}

fn node(value: &str) -> NodeId {
    NodeId::from_uuid(Uuid::parse_str(value).unwrap())
}

fn cell() -> CellId {
    let id = Uuid::parse_str(&std::env::var("PEREN_PROOF_CELL").unwrap()).unwrap();
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(id.as_bytes());
    bytes[16..].copy_from_slice(id.as_bytes());
    CellId::from_bytes(bytes)
}

fn data_path() -> PathBuf {
    PathBuf::from(std::env::var_os("PEREN_PROOF_DATA").unwrap()).join("cell.sqlite")
}

fn bundle() -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(
                ModuleKind::JavaScript,
                b"export default { async fetch(request) {
                  if (new URL(request.url).pathname === '/increment') {
                    await Peren.storage.transaction(async (storage) => {
                      const current = await storage.get('counter');
                      const next = current === undefined ? 1 : current[0] + 1;
                      await storage.put('counter', new Uint8Array([next]));
                    });
                  }
                  const value = await Peren.storage.get('counter');
                  return new Response(String(value?.[0] ?? 0));
                } };"
                    .as_slice(),
            )
            .unwrap(),
        )]),
    )
    .unwrap()
}

fn request(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("https://worker.invalid{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

fn isolate() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

fn invocation() -> InvocationLimits {
    InvocationLimits::new(1024, 10)
}

#[tokio::test]
#[ignore = "spawned by the cross-process S3 failover test"]
async fn process_node() {
    match std::env::var("PEREN_PROOF_ROLE").unwrap().as_str() {
        "a" => run_a().await,
        "b" => run_b().await,
        role => panic!("unknown proof role: {role}"),
    }
}

async fn run_a() {
    let store = store();
    let lease = store.acquire(node(NODE_A), cell()).await.unwrap();
    let mut resident = WorkerCell::activate(
        &data_path(),
        lease,
        store,
        bundle(),
        isolate(),
        peren_runtime::WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    let written = resident
        .dispatch_http(request("/increment"), invocation())
        .await
        .unwrap();
    assert_eq!(written.body, b"1");
    println!("READY");
    std::io::stdout().flush().unwrap();

    let mut signal = String::new();
    std::io::stdin().read_line(&mut signal).unwrap();
    assert_eq!(signal.trim(), "resume");
    assert!(
        resident
            .dispatch_http(request("/increment"), invocation())
            .await
            .is_err()
    );
    assert_eq!(resident.state(), CellState::Fenced);
    assert!(resident.release().await.is_err());
}

async fn run_b() {
    let store = store();
    let evidence = RecoveryPolicy::new(NonZeroUsize::new(2).unwrap(), 100)
        .authorize(&RecoveryObservation::new(
            node(NODE_A),
            OwnershipEpoch::new(1),
            1_000,
            1_100,
            BTreeSet::from([node(WITNESS_A), node(WITNESS_B)]),
        ))
        .unwrap();
    let lease = store.recover(node(NODE_B), cell(), evidence).await.unwrap();
    let replica = store.restore(cell()).await.unwrap().unwrap();
    let path = data_path();
    std::fs::write(&path, replica.database).unwrap();
    if !replica.wal.is_empty() {
        std::fs::write(format!("{}-wal", path.display()), replica.wal).unwrap();
    }
    let mut resident = WorkerCell::activate(
        &path,
        lease,
        store,
        bundle(),
        isolate(),
        peren_runtime::WorkerEnvironment::empty(),
    )
    .await
    .unwrap();
    assert_eq!(
        resident
            .dispatch_http(request("/read"), invocation())
            .await
            .unwrap()
            .body,
        b"1"
    );
    resident.release().await.unwrap();
}

#[test]
#[ignore = "requires an S3-compatible service configured through PEREN_S3_* variables"]
fn s3_cross_process_failover_fences_the_old_owner() {
    for name in [
        "PEREN_S3_ENDPOINT",
        "PEREN_S3_BUCKET",
        "PEREN_S3_ACCESS_KEY",
        "PEREN_S3_SECRET_KEY",
    ] {
        required_env(name);
    }

    run_cross_process_failover("s3");
}

#[test]
#[ignore = "requires Azure Blob or Azurite configured through PEREN_AZURE_* variables"]
fn azure_cross_process_failover_fences_the_old_owner() {
    for name in [
        "PEREN_AZURE_ACCOUNT",
        "PEREN_AZURE_CONTAINER",
        "PEREN_AZURE_ENDPOINT",
        "PEREN_AZURE_ACCESS_KEY",
    ] {
        required_env(name);
    }

    run_cross_process_failover("azure");
}

fn run_cross_process_failover(provider: &str) {
    let root = std::env::temp_dir().join(Uuid::new_v4().to_string());
    let a_data = root.join("a");
    let b_data = root.join("b");
    std::fs::create_dir_all(&a_data).unwrap();
    std::fs::create_dir_all(&b_data).unwrap();
    let executable = std::env::current_exe().unwrap();
    let proof_cell = Uuid::new_v4().to_string();

    let mut a = process_with_provider(&executable, provider, "a", &a_data, &proof_cell)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = BufReader::new(a.stdout.take().unwrap());
    let (ready_tx, ready_rx) = mpsc::channel();
    let output_thread = std::thread::spawn(move || {
        let mut ready = false;
        for line in output.lines() {
            if !ready && line.is_ok_and(|line| line.trim() == "READY") {
                ready = true;
                ready_tx.send(true).ok();
            }
        }
        if !ready {
            ready_tx.send(false).ok();
        }
    });
    if !matches!(ready_rx.recv_timeout(Duration::from_secs(20)), Ok(true)) {
        a.kill().ok();
        a.wait().ok();
        panic!("node A did not become ready");
    }

    let mut b = process_with_provider(&executable, provider, "b", &b_data, &proof_cell)
        .spawn()
        .unwrap();
    wait_success(&mut b, "node B");
    writeln!(a.stdin.take().unwrap(), "resume").unwrap();
    wait_success(&mut a, "node A");
    output_thread.join().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn process_with_provider(
    executable: &Path,
    provider: &str,
    role: &str,
    data: &Path,
    cell: &str,
) -> Command {
    let mut command = Command::new(executable);
    command
        .args(["--ignored", "--exact", "process_node", "--nocapture"])
        .env("PEREN_PROOF_PROVIDER", provider)
        .env("PEREN_PROOF_ROLE", role)
        .env("PEREN_PROOF_DATA", data)
        .env("PEREN_PROOF_CELL", cell);
    command
}

fn wait_success(child: &mut std::process::Child, name: &str) {
    if let Some(status) = child.wait_timeout(Duration::from_secs(20)).unwrap() {
        assert!(status.success(), "{name} failed with {status}");
    } else {
        child.kill().unwrap();
        child.wait().unwrap();
        panic!("{name} exceeded the process deadline");
    }
}
