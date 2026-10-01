use peren_runtime::{
    HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName, WorkerBundle,
    WorkerRuntime,
};
use std::{collections::BTreeMap, time::Duration};

pub(crate) fn limits() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}

pub(crate) fn bundle(source: &str) -> WorkerBundle {
    let entry = ModuleName::parse("main.js").unwrap();
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(
            entry,
            Module::new(ModuleKind::JavaScript, source.as_bytes()).unwrap(),
        )]),
    )
    .unwrap()
}

pub(crate) fn request(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("https://worker.invalid{path}"),
        headers: Vec::new(),
        body: Vec::new(),
        mtls: None,
    }
}

#[path = "web/crypto.rs"]
mod crypto;
#[path = "web/platform.rs"]
mod platform;
