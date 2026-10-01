use std::path::{Path, PathBuf};

use axum::{
    body::Body,
    http::{Method, Response, StatusCode},
};
use peren_config::RunWorkerFirst;

use crate::process::ProcessError;

#[derive(Clone)]
pub(super) struct AssetConfig {
    directory: PathBuf,
    run: Option<RunWorkerFirst>,
}

impl AssetConfig {
    pub(super) fn new(directory: PathBuf, run: Option<RunWorkerFirst>) -> Self {
        Self { directory, run }
    }

    pub(super) fn run_worker_first(&self, path: &str) -> bool {
        match &self.run {
            Some(RunWorkerFirst::Always(value)) => *value,
            Some(RunWorkerFirst::Patterns(patterns)) => {
                patterns.iter().any(|pattern| asset_match(pattern, path))
            }
            None => false,
        }
    }
}

pub(super) async fn serve(
    assets: Option<&AssetConfig>,
    method: &Method,
    path: &str,
) -> Result<Option<Response<Body>>, ProcessError> {
    let Some(assets) = assets else {
        return Ok(None);
    };
    if !matches!(*method, Method::GET | Method::HEAD) {
        return Ok(None);
    }
    let Some(relative) = asset_path(path) else {
        return Err(ProcessError::AssetPath);
    };
    let path = assets.directory.join(relative);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::IsADirectory => return Ok(None),
        Err(source) => return Err(ProcessError::AssetRead { path, source }),
    };
    let body = if *method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(bytes)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type(&path))
        .body(body)
        .map(Some)
        .map_err(ProcessError::Response)
}

fn asset_path(path: &str) -> Option<PathBuf> {
    let trimmed = path.trim_start_matches('/');
    let name = if trimmed.is_empty() {
        "index.html"
    } else {
        trimmed
    };
    let mut out = PathBuf::new();
    for segment in name.split('/') {
        if segment.is_empty() || matches!(segment, "." | "..") || segment.contains('\\') {
            return None;
        }
        out.push(segment);
    }
    Some(out)
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|value| value.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn asset_match(pattern: &str, path: &str) -> bool {
    pattern == path
        || pattern == "*"
        || pattern
            .strip_suffix('*')
            .is_some_and(|prefix| path.starts_with(prefix))
}
