pub use peren_runtime::{
    BundleError, HttpRequest, InvocationLimits, IsolateLimits, Module, ModuleKind, ModuleName,
    WorkerBundle, WorkerRuntime,
};
pub use std::{collections::BTreeMap, time::Duration};

pub(super) fn name(value: &str) -> ModuleName {
    ModuleName::parse(value).unwrap()
}

pub(super) fn javascript(source: &str) -> Module {
    Module::new(ModuleKind::JavaScript, source.as_bytes()).unwrap()
}

pub(super) fn limits() -> IsolateLimits {
    IsolateLimits::new(128 * 1024 * 1024, Duration::from_secs(5))
}
