use std::{collections::BTreeMap, sync::Mutex};

use axum::{
    body::Body,
    http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode, header},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use peren_primitives::CellId;
use peren_runtime::HttpResponse;
use sha1::Sha1;
use sha2::Digest;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Session {
    pub(crate) id: String,
    pub(crate) service: String,
    pub(crate) path: String,
    pub(crate) cell: CellId,
}

#[derive(Default)]
pub(crate) struct Registry {
    sessions: Mutex<BTreeMap<String, Session>>,
}

impl Registry {
    pub(crate) fn record(&self, session: Session) -> Result<bool, RegistryError> {
        let mut sessions = self.sessions.lock().map_err(|_| RegistryError::Poisoned)?;
        Ok(sessions.insert(session.id.clone(), session).is_none())
    }

    pub(crate) fn get(&self, id: &str) -> Result<Option<Session>, RegistryError> {
        let sessions = self.sessions.lock().map_err(|_| RegistryError::Poisoned)?;
        Ok(sessions.get(id).cloned())
    }

    pub(crate) fn remove(&self, id: &str) -> Result<Option<Session>, RegistryError> {
        let mut sessions = self.sessions.lock().map_err(|_| RegistryError::Poisoned)?;
        Ok(sessions.remove(id))
    }

    pub(crate) fn len(&self) -> Result<usize, RegistryError> {
        let sessions = self.sessions.lock().map_err(|_| RegistryError::Poisoned)?;
        Ok(sessions.len())
    }
}

pub(crate) fn is_upgrade(headers: &HeaderMap) -> bool {
    let has_upgrade_token = headers
        .get(header::CONNECTION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        });
    let is_websocket = headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    has_upgrade_token && is_websocket
}

pub(crate) fn response(
    request: &HeaderMap,
    response: HttpResponse,
) -> Result<Response<Body>, RegistryError> {
    let key = request
        .get("sec-websocket-key")
        .and_then(|value| value.to_str().ok())
        .ok_or(RegistryError::MissingKey)?;
    let mut digest = Sha1::new();
    digest.update(key.as_bytes());
    digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = STANDARD.encode(digest.finalize());
    let mut builder = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header("sec-websocket-accept", accept);
    if let Some(session) = response.websocket_id {
        builder = builder.header("x-peren-websocket-session", session);
    }
    let requested_protocols = requested_protocols(request);
    for (name, value) in response.headers {
        let name = HeaderName::try_from(name)?;
        if matches!(
            name.as_str(),
            "connection" | "upgrade" | "sec-websocket-accept"
        ) {
            continue;
        }
        if name.as_str() == "sec-websocket-protocol" {
            let selected = value.trim();
            if !requested_protocols
                .iter()
                .any(|protocol| protocol == selected)
            {
                return Err(RegistryError::UnrequestedProtocol(selected.into()));
            }
            builder = builder.header(name, HeaderValue::try_from(selected)?);
            continue;
        }
        builder = builder.header(name, HeaderValue::try_from(value)?);
    }
    builder.body(Body::empty()).map_err(RegistryError::Response)
}

fn requested_protocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get("sec-websocket-protocol")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("WebSocket session registry is unavailable")]
    Poisoned,
    #[error("WebSocket upgrade request is missing Sec-WebSocket-Key")]
    MissingKey,
    #[error("Worker did not accept WebSocket upgrade")]
    Rejected,
    #[error("Worker WebSocket response did not include a session id")]
    MissingSession,
    #[error("WebSocket response could not be built")]
    Response(#[source] axum::http::Error),
    #[error("WebSocket response header is invalid")]
    Header(#[from] axum::http::header::InvalidHeaderValue),
    #[error("Worker selected unrequested WebSocket subprotocol {0:?}")]
    UnrequestedProtocol(String),
    #[error("WebSocket response header name is invalid")]
    HeaderName(#[from] axum::http::header::InvalidHeaderName),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(value: u8) -> CellId {
        CellId::from_bytes([value; 32])
    }

    #[test]
    fn parses_requested_subprotocols() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "sec-websocket-protocol",
            HeaderValue::from_static("chat.v2, peren.rpc"),
        );

        assert_eq!(
            requested_protocols(&headers),
            vec!["chat.v2".to_string(), "peren.rpc".to_string()]
        );
    }

    #[test]
    fn upgrade_detection_accepts_comma_separated_connection_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, Upgrade"),
        );
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));

        assert!(is_upgrade(&headers));
    }

    #[test]
    fn response_rejects_unrequested_subprotocol() {
        let mut request = HeaderMap::new();
        request.insert(
            "sec-websocket-key",
            HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ=="),
        );
        request.insert(
            "sec-websocket-protocol",
            HeaderValue::from_static("chat.v1"),
        );
        let response = HttpResponse {
            status: 101,
            headers: vec![("sec-websocket-protocol".into(), "peren.rpc".into())],
            body: Vec::new(),
            upgrade: true,
            websocket_id: Some("socket-1".into()),
        };

        assert!(matches!(
            super::response(&request, response),
            Err(RegistryError::UnrequestedProtocol(protocol)) if protocol == "peren.rpc"
        ));
    }

    #[test]
    fn registry_records_session_metadata_once() {
        let registry = Registry::default();
        let session = Session {
            id: "socket-1".into(),
            service: "api".into(),
            path: "/chat".into(),
            cell: cell(7),
        };

        assert!(registry.record(session.clone()).unwrap());
        assert!(!registry.record(session.clone()).unwrap());
        assert_eq!(registry.len().unwrap(), 1);
        assert_eq!(registry.get("socket-1").unwrap(), Some(session));
    }
}
