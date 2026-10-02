#[cfg(unix)]
mod unix {
    use std::{
        fs,
        io::{BufRead, BufReader, Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        process::{Command, Stdio},
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use wait_timeout::ChildExt;

    fn unused_addresses() -> (SocketAddr, SocketAddr) {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        (first.local_addr().unwrap(), second.local_addr().unwrap())
    }

    #[test]
    fn serve_handles_termination_and_exits_cleanly() {
        let (peer, public) = unused_addresses();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("peren-process-{nonce}"));
        fs::create_dir_all(&directory).unwrap();
        let worker = directory.join("worker.js");
        fs::write(
            &worker,
            "export default { async fetch() { return new Response('ok'); } };",
        )
        .unwrap();
        let config = directory.join("fleet.toml");
        fs::write(
            &config,
            format!(
                r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "{peer}"
listen = "{peer}"
[bucket]
kind = "memory"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[shutdown]
evacuation_deadline_secs = 2
[[services]]
name = "api"
worker_bundle_path = "{}"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "{public}"
service = "api"
"#,
                worker.display()
            ),
        )
        .unwrap();

        let mut child = Command::new(env!("CARGO_BIN_EXE_peren"))
            .args(["serve", config.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let started = Instant::now();
        while TcpStream::connect(public).is_err() {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "server did not become ready"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = child.wait_timeout(Duration::from_secs(3)).unwrap();
        if status.is_none() {
            child.kill().unwrap();
        }
        let status = status.unwrap_or_else(|| child.wait().unwrap());
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(status.success(), "{stderr}");
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        assert!(stdout.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn dev_prints_local_listener_addresses_and_serves_worker() {
        let (peer, public) = unused_addresses();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("peren-dev-{nonce}"));
        fs::create_dir_all(&directory).unwrap();
        let worker = directory.join("worker.js");
        fs::write(
            &worker,
            "export default { async fetch() { return new Response('dev-ok', { status: 203 }); } };",
        )
        .unwrap();
        let config = directory.join("fleet.toml");
        fs::write(
            &config,
            format!(
                r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "{peer}"
listen = "{peer}"
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
listen = "{public}"
service = "api"
"#,
                worker.display()
            ),
        )
        .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_peren"))
            .args(["dev", config.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut public = None;
        let started = Instant::now();
        while public.is_none() {
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "dev did not print listener addresses"
            );
            let mut line = String::new();
            output.read_line(&mut line).unwrap();
            if let Some(address) = line.strip_prefix("public: http://") {
                public = Some(address.trim().to_string());
            }
        }
        let public = public.unwrap();
        let mut stream = TcpStream::connect(&public).unwrap();
        stream
            .write_all(b"GET /room HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 203"), "{response}");
        assert!(response.ends_with("dev-ok"), "{response}");
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = child
            .wait_timeout(Duration::from_secs(3))
            .unwrap()
            .expect("dev exits after SIGTERM");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(status.success(), "{stderr}");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn test_server_emits_one_parseable_readiness_record() {
        let (peer, public) = unused_addresses();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("peren-test-server-{nonce}"));
        fs::create_dir_all(&directory).unwrap();
        let worker = directory.join("worker.js");
        fs::write(
            &worker,
            "export default { async fetch() { return new Response('worker-ok', { status: 202 }); } };",
        )
        .unwrap();
        let config = directory.join("fleet.toml");
        fs::write(
            &config,
            format!(
                r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "{peer}"
listen = "{peer}"
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
listen = "{public}"
service = "api"
"#,
                worker.display()
            ),
        )
        .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_peren"))
            .args(["test-server", config.to_str().unwrap()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let readiness: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(readiness["ready"], true);
        let sockets = readiness["sockets"].as_object().unwrap();
        assert_eq!(sockets.len(), 2);
        for address in sockets.values() {
            let address = address.as_str().unwrap();
            let mut stream = TcpStream::connect(address).unwrap();
            stream
                .write_all(b"GET /readyz HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(response.starts_with("HTTP/1.1 200"));
        }
        assert!(
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let status = child
            .wait_timeout(Duration::from_secs(3))
            .unwrap()
            .expect("test-server exits after SIGTERM");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert!(status.success(), "{stderr}");
        fs::remove_dir_all(directory).unwrap();
    }
}
