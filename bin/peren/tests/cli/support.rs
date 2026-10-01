use std::{
    fs,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub(crate) fn peren(args: &[&str]) -> std::process::Output {
    command(args).output().expect("peren process starts")
}

pub(crate) fn command(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_peren"));
    command.args(args);
    command
}

pub(crate) fn temp(prefix: &str) -> std::path::PathBuf {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{}-{time}-{sequence}", std::process::id()))
}

pub(crate) fn config() -> std::path::PathBuf {
    let directory = temp("peren-cli");
    fs::create_dir_all(&directory).unwrap();
    let worker = directory.join("worker.js");
    fs::write(
        &worker,
        "export default { async fetch() { return new Response('ok'); } };",
    )
    .unwrap();
    let map = directory.join("worker.js.map");
    fs::write(
        &map,
        r#"{"version":3,"sources":["worker.ts"],"mappings":""}"#,
    )
    .unwrap();
    let config = directory.join("fleet.toml");
    fs::write(
        &config,
        format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
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
[services.source_maps]
"worker.js.map" = "{}"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#,
            worker.display(),
            map.display()
        ),
    )
    .unwrap();
    config
}

pub(crate) fn ready_server() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 512];
        let _ = std::io::Read::read(&mut stream, &mut request);
        std::io::Write::write_all(
            &mut stream,
            b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        )
        .unwrap();
    });
    address
}

pub(crate) fn config_with_secret() -> std::path::PathBuf {
    let config = config();
    let text = fs::read_to_string(&config).unwrap();
    fs::write(
        &config,
        text.replace(
            "[[services]]",
            "[secrets_store]\nTOKEN = \"TOKEN_ENV\"\n[[services]]",
        ),
    )
    .unwrap();
    config
}

pub(crate) fn config_with_tenant() -> std::path::PathBuf {
    let config = config();
    let text = fs::read_to_string(&config).unwrap();
    fs::write(
        &config,
        text.replace(
            "[[services]]\nname = \"api\"",
            "[[tenants]]\nid = \"acme\"\ncell_quota = 10\n[[services]]\nname = \"api\"",
        )
        .replace(
            "compatibility_date = \"2026-01-01\"",
            "compatibility_date = \"2026-01-01\"\ntenant_id = \"acme\"",
        ),
    )
    .unwrap();
    config
}

pub(crate) fn config_with_workflow() -> std::path::PathBuf {
    let config = config();
    let text = fs::read_to_string(&config).unwrap();
    fs::write(
        &config,
        format!(
            "{text}\n[services.bindings.FLOW]\ntype = \"workflow\"\nclass_name = \"Flow\"\nunique_key = \"flow\"\n"
        ),
    )
    .unwrap();
    config
}

pub(crate) fn deploy(config: &std::path::Path, data: &std::path::Path, args: &[&str]) -> String {
    let mut command = command(args);
    command.env("PEREN_DATA_DIR", data);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(config.exists());
    stdout
}

pub(crate) fn deploy_drift(config: &std::path::Path, data: &std::path::Path) {
    let output = command(&["deploy", "health", config.to_str().unwrap()])
        .env("PEREN_DATA_DIR", data)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("artifact digest drifted")
    );
}

pub(crate) fn write_worker(directory: &std::path::Path, response: &str) {
    fs::write(
        directory.join("worker.js"),
        format!("export default {{ async fetch() {{ return new Response('{response}'); }} }};"),
    )
    .unwrap();
}
