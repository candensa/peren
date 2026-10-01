use std::path::PathBuf;

use axum::{
    body::Body,
    http::{Response, StatusCode},
    response::IntoResponse,
};
use peren_config::QueueBroker;
use peren_runtime::BundleError;
use thiserror::Error;

use crate::{ProviderError, SupervisorError};

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("listener operation failed: {0}")]
    Listen(#[source] std::io::Error),
    #[error("worker module {path:?} could not be read")]
    BundleFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("inherited listener {0:?} is not configured")]
    UnknownInheritedListener(String),
    #[error(transparent)]
    Supervisor(#[from] SupervisorError),
    #[error("socket {0:?} has no service mapping")]
    SocketService(String),
    #[error("service {0:?} has no loaded Worker bundle")]
    ServiceBundle(String),
    #[error(
        "secret environment variable {0:?} was resolved during provider startup but is no longer available"
    )]
    SecretVariable(String),
    #[error(
        "secret store entry {0:?} was resolved during provider startup but is no longer available"
    )]
    SecretStore(String),
    #[error("R2 bucket {bucket:?} requires {field}")]
    MissingR2Credential { bucket: String, field: &'static str },
    #[error("queue broker {0:?} is missing required configuration or is unavailable")]
    QueueBroker(QueueBroker),
    #[error(transparent)]
    QueueAdapter(#[from] peren_queues::QueueError),
    #[error("worker module path {0:?} is not a valid bundle-relative path")]
    ModulePath(PathBuf),
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error("worker environment could not be serialized")]
    Environment(#[source] serde_json::Error),
    #[error("configured limit {0} does not fit this platform")]
    Limit(&'static str),
    #[error(transparent)]
    WebSocketSession(#[from] crate::websocket::RegistryError),
    #[error("request body exceeds the configured limit")]
    BodyLimit,
    #[error("durable object namespace {0:?} was not found on the test service")]
    DurableObjectNamespace(String),
    #[error("durable object class {0:?} does not map to a configured service")]
    DurableObjectClass(String),
    #[error("binding method is not supported by the test surface")]
    TestBindingMethod,
    #[error("failed to create test dispatch runtime")]
    Runtime(#[source] std::io::Error),
    #[error("test dispatch task failed: {0}")]
    Task(String),
    #[error("asset path escapes the configured directory")]
    AssetPath,
    #[error("failed to read asset {path:?}")]
    AssetRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("data directory operation failed")]
    Data(#[source] std::io::Error),
    #[error(transparent)]
    Storage(#[from] peren_storage::StorageError),
    #[error(transparent)]
    Turso(#[from] peren_provider_turso::TursoError),
    #[error(transparent)]
    ObjectStore(#[from] peren_provider_object_store::BuildError),
    #[error("request header is not valid UTF-8")]
    Header(#[from] axum::http::header::ToStrError),
    #[error(transparent)]
    HeaderName(#[from] axum::http::header::InvalidHeaderName),
    #[error(transparent)]
    HeaderValue(#[from] axum::http::header::InvalidHeaderValue),
    #[error("response could not be built")]
    Response(#[source] axum::http::Error),
    #[error(transparent)]
    Node(#[from] crate::NodeError),
    #[error(transparent)]
    Tail(#[from] crate::TailError),
}

impl IntoResponse for ProcessError {
    fn into_response(self) -> Response<Body> {
        let status = match self {
            Self::BodyLimit => StatusCode::PAYLOAD_TOO_LARGE,
            Self::AssetPath => StatusCode::BAD_REQUEST,
            Self::Header(_) | Self::HeaderName(_) | Self::HeaderValue(_) | Self::Response(_) => {
                StatusCode::BAD_GATEWAY
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = self.to_string().into_response();
        *response.status_mut() = status;
        response
    }
}
