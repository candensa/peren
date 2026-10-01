use peren_config::{BucketKind, ConfigError, FleetConfig, ValidatedConfig};
use thiserror::Error;

pub fn prepare_test_server(mut config: FleetConfig) -> Result<ValidatedConfig, DevelopmentError> {
    if !config.seed_peers.is_empty() {
        return Err(DevelopmentError::Peers);
    }
    if config.deploy.is_some() {
        return Err(DevelopmentError::Deployment);
    }

    config.bucket.kind = BucketKind::Memory;
    config.node.advertise_addr = loopback(&config.node.advertise_addr, "peer")?;
    config.node.listen.clone_from(&config.node.advertise_addr);
    for socket in &mut config.sockets {
        socket.listen = loopback(&socket.listen, &socket.name)?;
    }
    if let Some(console) = &mut config.console {
        console.listen = loopback(&console.listen, "console")?;
    }
    config.validate().map_err(DevelopmentError::Config)
}

fn loopback(value: &str, listener: &str) -> Result<String, DevelopmentError> {
    let address = value
        .parse::<std::net::SocketAddr>()
        .map_err(|_| DevelopmentError::Address(listener.to_string()))?;
    if !address.ip().is_loopback() {
        return Err(DevelopmentError::Address(listener.to_string()));
    }
    Ok(std::net::SocketAddr::new(address.ip(), 0).to_string())
}

#[derive(Debug, Error)]
pub enum DevelopmentError {
    #[error("test-server requires a single-node configuration")]
    Peers,
    #[error("test-server does not accept deployment mode")]
    Deployment,
    #[error("test-server listener {0:?} must use a loopback IP address")]
    Address(String),
    #[error(transparent)]
    Config(#[from] ConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLEET: &str = r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:7000"
listen = "127.0.0.1:7000"
[bucket]
kind = "file"
path = "data"
[mtls]
ca_cert_path = "ca.pem"
leaf_cert_path = "leaf.pem"
leaf_key_path = "key.pem"
[[services]]
name = "api"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"
[[sockets]]
name = "public"
listen = "127.0.0.1:8080"
service = "api"
"#;

    #[test]
    fn isolates_test_server_from_durable_and_public_resources() {
        let prepared = prepare_test_server(FleetConfig::from_toml(FLEET).unwrap()).unwrap();

        assert_eq!(prepared.raw.bucket.kind, BucketKind::Memory);
        assert_eq!(prepared.peer_listen.port(), 0);
        assert_eq!(prepared.sockets["public"].port(), 0);
    }

    #[test]
    fn rejects_non_loopback_listeners() {
        let mut config = FleetConfig::from_toml(FLEET).unwrap();
        config.sockets[0].listen = "0.0.0.0:8080".to_string();

        assert!(matches!(
            prepare_test_server(config),
            Err(DevelopmentError::Address(name)) if name == "public"
        ));
    }
}
