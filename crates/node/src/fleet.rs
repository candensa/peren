use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use peren_primitives::NodeId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{Environment, process};

#[derive(Debug)]
pub struct Join {
    pub key_dir: PathBuf,
    pub token: String,
}

#[derive(Debug)]
pub struct Drain {
    pub node: NodeId,
    pub reason: Option<String>,
}

#[derive(Debug)]
pub struct Remove {
    pub node: NodeId,
    pub force: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    pub node: NodeId,
    pub state: State,
    pub reason: Option<String>,
    pub peer_addr: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    Active,
    Draining,
    Removed,
}

pub fn join(environment: &impl Environment, request: &Join) -> Result<Report, Error> {
    let claims = peren_security::verify_node(&request.key_dir, &request.token)?;
    let node = Uuid::parse_str(&claims.node).map_err(|_| Error::TokenNode(claims.node.clone()))?;
    let mut registry = Registry::load(root(environment))?;
    let record = Record {
        node,
        state: StateRecord::Active,
        reason: None,
        peer_addr: Some(claims.peer_addr),
        changed_at_ms: now()?,
    };
    registry.upsert(record.clone());
    registry.save()?;
    Ok(report(record))
}

pub fn drain(environment: &impl Environment, request: Drain) -> Result<Report, Error> {
    let mut registry = Registry::load(root(environment))?;
    let record = Record {
        node: request.node.as_uuid(),
        state: StateRecord::Draining,
        reason: request.reason,
        peer_addr: prior_peer(&registry, request.node),
        changed_at_ms: now()?,
    };
    registry.upsert(record.clone());
    registry.save()?;
    Ok(report(record))
}

pub fn remove(environment: &impl Environment, request: &Remove) -> Result<Report, Error> {
    let mut registry = Registry::load(root(environment))?;
    let prior = registry.find(request.node);
    if !request.force
        && !matches!(
            prior.map(|record| record.state),
            Some(StateRecord::Draining)
        )
    {
        return Err(Error::NotDrained(request.node));
    }
    let record = Record {
        node: request.node.as_uuid(),
        state: StateRecord::Removed,
        reason: prior.and_then(|record| record.reason.clone()),
        peer_addr: prior.and_then(|record| record.peer_addr.clone()),
        changed_at_ms: now()?,
    };
    registry.upsert(record.clone());
    registry.save()?;
    Ok(report(record))
}

fn report(record: Record) -> Report {
    Report {
        node: NodeId::from_uuid(record.node),
        state: match record.state {
            StateRecord::Active => State::Active,
            StateRecord::Draining => State::Draining,
            StateRecord::Removed => State::Removed,
        },
        reason: record.reason,
        peer_addr: record.peer_addr,
    }
}

fn prior_peer(registry: &Registry, node: NodeId) -> Option<String> {
    registry
        .find(node)
        .and_then(|record| record.peer_addr.clone())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Record {
    node: Uuid,
    state: StateRecord,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    peer_addr: Option<String>,
    changed_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StateRecord {
    Active,
    Draining,
    Removed,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Registry {
    #[serde(default)]
    nodes: Vec<Record>,
    #[serde(skip)]
    root: PathBuf,
}

impl Registry {
    fn load(root: PathBuf) -> Result<Self, Error> {
        let path = root.join("nodes.json");
        match fs::read(&path) {
            Ok(bytes) => {
                let mut registry = serde_json::from_slice::<Self>(&bytes)?;
                registry.root = root;
                Ok(registry)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                nodes: Vec::new(),
                root,
            }),
            Err(source) => Err(Error::Read { path, source }),
        }
    }

    fn find(&self, node: NodeId) -> Option<&Record> {
        self.nodes
            .iter()
            .find(|record| record.node == node.as_uuid())
    }

    fn upsert(&mut self, record: Record) {
        if let Some(existing) = self.nodes.iter_mut().find(|item| item.node == record.node) {
            *existing = record;
        } else {
            self.nodes.push(record);
        }
        self.nodes.sort_by_key(|record| record.node);
    }

    fn save(&self) -> Result<(), Error> {
        fs::create_dir_all(&self.root).map_err(|source| Error::Create {
            path: self.root.clone(),
            source,
        })?;
        let path = self.root.join("nodes.json");
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&path, bytes).map_err(|source| Error::Write { path, source })
    }
}

fn root(environment: &impl Environment) -> PathBuf {
    environment
        .get("PEREN_DATA_DIR")
        .map_or_else(process::default_data, PathBuf::from)
        .join("fleet")
}

fn now() -> Result<i64, Error> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Time)?;
    i64::try_from(duration.as_millis()).map_err(|_| Error::Time)
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("node {0:?} must be drained before removal; pass --force to override")]
    NotDrained(NodeId),
    #[error("node identity token contains invalid node id {0:?}")]
    TokenNode(String),
    #[error("system clock cannot produce a valid fleet timestamp")]
    Time,
    #[error("failed to read fleet registry {path:?}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create fleet registry directory {path:?}")]
    Create {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write fleet registry {path:?}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Security(#[from] peren_security::CredentialError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::DataEnv;

    #[test]
    fn join_verifies_signed_identity_and_tracks_membership() {
        let root = std::env::temp_dir().join(format!("peren-fleet-{}", Uuid::new_v4()));
        let env = DataEnv::new(root.join("data"));
        let keys = root.join("keys");
        let node = NodeId::from_uuid(Uuid::new_v4());
        let token = peren_security::mint_node(
            &keys,
            peren_security::NodeMint {
                cluster: "prod".into(),
                node: node.as_uuid().to_string(),
                peer_addr: "127.0.0.1:7000".into(),
            },
        )
        .unwrap();

        let joined = join(
            &env,
            &Join {
                key_dir: keys,
                token,
            },
        )
        .unwrap();

        assert_eq!(joined.node, node);
        assert_eq!(joined.state, State::Active);
        assert_eq!(joined.peer_addr.as_deref(), Some("127.0.0.1:7000"));
        let registry = Registry::load(env.path().join("fleet")).unwrap();
        assert_eq!(registry.find(node).unwrap().state, StateRecord::Active);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remove_requires_prior_drain_unless_forced() {
        let root = std::env::temp_dir().join(format!("peren-fleet-{}", Uuid::new_v4()));
        let env = DataEnv::new(root.join("data"));
        let node = NodeId::from_uuid(Uuid::new_v4());

        assert!(matches!(
            remove(&env, &Remove { node, force: false }),
            Err(Error::NotDrained(blocked)) if blocked == node
        ));

        let drained = drain(
            &env,
            Drain {
                node,
                reason: Some("maintenance".into()),
            },
        )
        .unwrap();
        assert_eq!(drained.state, State::Draining);
        assert_eq!(drained.reason.as_deref(), Some("maintenance"));

        let removed = remove(&env, &Remove { node, force: false }).unwrap();
        assert_eq!(removed.state, State::Removed);
        assert_eq!(removed.reason.as_deref(), Some("maintenance"));

        fs::remove_dir_all(root).unwrap();
    }
}
