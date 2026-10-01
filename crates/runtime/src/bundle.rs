use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, btree_map},
    fmt,
    sync::{Arc, OnceLock},
};
use thiserror::Error;

const MAX_MODULES: usize = 1_024;
const MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const BUILTIN_CLOUDFLARE_WORKERS: &str = "cloudflare:workers";
pub(crate) const BUILTIN_NODE_BUFFER: &str = "node:buffer";
pub(crate) const BUILTIN_NODE_CONSOLE: &str = "node:console";
pub(crate) const BUILTIN_NODE_CRYPTO: &str = "node:crypto";
pub(crate) const BUILTIN_NODE_ASSERT: &str = "node:assert";
pub(crate) const BUILTIN_NODE_OS: &str = "node:os";
pub(crate) const BUILTIN_NODE_PERF_HOOKS: &str = "node:perf_hooks";
pub(crate) const BUILTIN_NODE_QUERYSTRING: &str = "node:querystring";
pub(crate) const BUILTIN_NODE_STRING_DECODER: &str = "node:string_decoder";
pub(crate) const BUILTIN_NODE_STREAM_WEB: &str = "node:stream/web";
pub(crate) const BUILTIN_NODE_EVENTS: &str = "node:events";
pub(crate) const BUILTIN_NODE_MODULE: &str = "node:module";
pub(crate) const BUILTIN_NODE_PATH: &str = "node:path";
pub(crate) const BUILTIN_NODE_PROCESS: &str = "node:process";
pub(crate) const BUILTIN_NODE_TIMERS: &str = "node:timers";
pub(crate) const BUILTIN_NODE_TIMERS_PROMISES: &str = "node:timers/promises";
pub(crate) const BUILTIN_NODE_UTIL: &str = "node:util";
pub(crate) const BUILTIN_NODE_URL: &str = "node:url";
pub(crate) const BUILTIN_NODE_ASYNC_HOOKS: &str = "node:async_hooks";
pub(crate) const BUILTIN_NODE_DIAGNOSTICS_CHANNEL: &str = "node:diagnostics_channel";
pub(crate) const BUILTIN_NODE_DNS: &str = "node:dns";
pub(crate) const BUILTIN_NODE_DNS_PROMISES: &str = "node:dns/promises";
pub(crate) const BUILTIN_NODE_FS: &str = "node:fs";
pub(crate) const BUILTIN_NODE_FS_PROMISES: &str = "node:fs/promises";
pub(crate) const BUILTIN_NODE_HTTP: &str = "node:http";
pub(crate) const BUILTIN_NODE_HTTPS: &str = "node:https";
pub(crate) const BUILTIN_NODE_NET: &str = "node:net";
pub(crate) const BUILTIN_NODE_PUNYCODE: &str = "node:punycode";
pub(crate) const BUILTIN_NODE_STREAM: &str = "node:stream";
pub(crate) const BUILTIN_NODE_STREAM_CONSUMERS: &str = "node:stream/consumers";
pub(crate) const BUILTIN_NODE_STREAM_PROMISES: &str = "node:stream/promises";
pub(crate) const BUILTIN_NODE_TEST: &str = "node:test";
pub(crate) const BUILTIN_NODE_TLS: &str = "node:tls";
pub(crate) const BUILTIN_NODE_ZLIB: &str = "node:zlib";
pub const PYTHON_PACKAGE_LOCK_MODULE: &str = "__peren_python_packages.json";

pub(crate) fn is_builtin(specifier: &str) -> bool {
    matches!(
        specifier,
        BUILTIN_CLOUDFLARE_WORKERS
            | BUILTIN_NODE_ASSERT
            | BUILTIN_NODE_BUFFER
            | BUILTIN_NODE_CONSOLE
            | BUILTIN_NODE_CRYPTO
            | BUILTIN_NODE_EVENTS
            | BUILTIN_NODE_MODULE
            | BUILTIN_NODE_OS
            | BUILTIN_NODE_PERF_HOOKS
            | BUILTIN_NODE_QUERYSTRING
            | BUILTIN_NODE_PATH
            | BUILTIN_NODE_PROCESS
            | BUILTIN_NODE_STRING_DECODER
            | BUILTIN_NODE_STREAM_WEB
            | BUILTIN_NODE_TIMERS
            | BUILTIN_NODE_TIMERS_PROMISES
            | BUILTIN_NODE_UTIL
            | BUILTIN_NODE_URL
            | BUILTIN_NODE_ASYNC_HOOKS
            | BUILTIN_NODE_DIAGNOSTICS_CHANNEL
            | BUILTIN_NODE_DNS
            | BUILTIN_NODE_DNS_PROMISES
            | BUILTIN_NODE_FS
            | BUILTIN_NODE_FS_PROMISES
            | BUILTIN_NODE_HTTP
            | BUILTIN_NODE_HTTPS
            | BUILTIN_NODE_NET
            | BUILTIN_NODE_PUNYCODE
            | BUILTIN_NODE_STREAM
            | BUILTIN_NODE_STREAM_CONSUMERS
            | BUILTIN_NODE_STREAM_PROMISES
            | BUILTIN_NODE_TEST
            | BUILTIN_NODE_TLS
            | BUILTIN_NODE_ZLIB
    )
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ModuleName(Arc<str>);

impl ModuleName {
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self, BundleError> {
        let value = value.into();
        let valid = if is_builtin(value.as_ref()) {
            true
        } else {
            !value.is_empty()
                && !value.starts_with('/')
                && !value.starts_with("./")
                && !value.contains(['\\', ':'])
                && value
                    .split('/')
                    .all(|component| !component.is_empty() && !matches!(component, "." | ".."))
        };
        if !valid {
            return Err(BundleError::ModuleName(value.to_string()));
        }
        Ok(Self(value))
    }

    pub fn resolve(&self, specifier: &str) -> Result<Self, BundleError> {
        if is_builtin(specifier) {
            return Self::parse(specifier);
        }
        if !(specifier.starts_with("./") || specifier.starts_with("../")) {
            return Err(BundleError::ImportSpecifier(specifier.to_string()));
        }
        if specifier.contains(['\\', '?', '#', '\0']) {
            return Err(BundleError::ImportSpecifier(specifier.to_string()));
        }

        let mut components: Vec<&str> = self.as_ref().split('/').collect();
        components.pop();
        for component in specifier.split('/') {
            match component {
                "" | "." => {}
                ".." => {
                    if components.pop().is_none() {
                        return Err(BundleError::ImportEscape(specifier.to_string()));
                    }
                }
                value => components.push(value),
            }
        }
        Self::parse(components.join("/"))
    }
}

impl AsRef<str> for ModuleName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModuleName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModuleKind {
    JavaScript,
    CommonJs,
    Wasm,
    Python,
}

#[derive(Clone, Debug)]
pub struct Module {
    kind: ModuleKind,
    source: Arc<[u8]>,
}

impl Module {
    pub fn new(kind: ModuleKind, source: impl Into<Arc<[u8]>>) -> Result<Self, BundleError> {
        let source = source.into();
        if source.len() > MAX_MODULE_BYTES {
            return Err(BundleError::ModuleTooLarge(source.len()));
        }
        Ok(Self { kind, source })
    }

    #[must_use]
    pub const fn kind(&self) -> ModuleKind {
        self.kind
    }

    #[must_use]
    pub fn source(&self) -> &[u8] {
        &self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BundleDigest([u8; 32]);

impl BundleDigest {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for BundleDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", hex::encode(self.0))
    }
}

#[derive(Clone, Debug)]
pub struct WorkerBundle {
    entry: ModuleName,
    modules: BTreeMap<ModuleName, Module>,
    digest: BundleDigest,
}

impl WorkerBundle {
    pub fn new(
        entry: ModuleName,
        modules: BTreeMap<ModuleName, Module>,
    ) -> Result<Self, BundleError> {
        if modules.len() > MAX_MODULES {
            return Err(BundleError::TooManyModules(modules.len()));
        }
        let bytes = modules.values().map(|module| module.source.len()).sum();
        if bytes > MAX_BUNDLE_BYTES {
            return Err(BundleError::BundleTooLarge(bytes));
        }
        let Some(entry_module) = modules.get(&entry) else {
            return Err(BundleError::MissingEntry(entry.to_string()));
        };
        if entry_module.kind == ModuleKind::Wasm {
            return Err(BundleError::WasmEntry(entry.to_string()));
        }
        let digest = digest(&entry, &modules);
        Ok(Self {
            entry,
            modules,
            digest,
        })
    }

    #[must_use]
    pub fn entry(&self) -> &ModuleName {
        &self.entry
    }

    #[must_use]
    pub const fn digest(&self) -> BundleDigest {
        self.digest
    }

    #[must_use]
    pub fn module(&self, name: &ModuleName) -> Option<&Module> {
        self.modules.get(name)
    }

    pub fn modules(&self) -> btree_map::Iter<'_, ModuleName, Module> {
        self.modules.iter()
    }

    pub fn resolve(
        &self,
        referrer: &ModuleName,
        specifier: &str,
    ) -> Result<(&ModuleName, &Module), BundleError> {
        if is_builtin(specifier) {
            let module = builtin_module(specifier);
            return Ok((&module.0, &module.1));
        }
        let name = referrer.resolve(specifier)?;
        self.modules
            .get_key_value(&name)
            .ok_or_else(|| BundleError::MissingImport(name.to_string()))
    }
}

fn builtin_module(specifier: &str) -> &'static (ModuleName, Module) {
    static CLOUDFLARE_WORKERS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_ASSERT: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_BUFFER: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_CONSOLE: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_CRYPTO: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_EVENTS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_MODULE: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_OS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_PERF_HOOKS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_QUERYSTRING: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_PATH: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_PROCESS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_STRING_DECODER: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_STREAM_WEB: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_TIMERS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_TIMERS_PROMISES: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_UTIL: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_URL: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_ASYNC_HOOKS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_DIAGNOSTICS_CHANNEL: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_DNS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_DNS_PROMISES: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_FS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_FS_PROMISES: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_HTTP: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_HTTPS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_NET: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_PUNYCODE: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_STREAM: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_STREAM_CONSUMERS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_STREAM_PROMISES: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_TEST: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_TLS: OnceLock<(ModuleName, Module)> = OnceLock::new();
    static NODE_ZLIB: OnceLock<(ModuleName, Module)> = OnceLock::new();

    let slot = match specifier {
        BUILTIN_CLOUDFLARE_WORKERS => &CLOUDFLARE_WORKERS,
        BUILTIN_NODE_ASSERT => &NODE_ASSERT,
        BUILTIN_NODE_BUFFER => &NODE_BUFFER,
        BUILTIN_NODE_CONSOLE => &NODE_CONSOLE,
        BUILTIN_NODE_CRYPTO => &NODE_CRYPTO,
        BUILTIN_NODE_EVENTS => &NODE_EVENTS,
        BUILTIN_NODE_MODULE => &NODE_MODULE,
        BUILTIN_NODE_OS => &NODE_OS,
        BUILTIN_NODE_PERF_HOOKS => &NODE_PERF_HOOKS,
        BUILTIN_NODE_QUERYSTRING => &NODE_QUERYSTRING,
        BUILTIN_NODE_PATH => &NODE_PATH,
        BUILTIN_NODE_PROCESS => &NODE_PROCESS,
        BUILTIN_NODE_STRING_DECODER => &NODE_STRING_DECODER,
        BUILTIN_NODE_STREAM_WEB => &NODE_STREAM_WEB,
        BUILTIN_NODE_TIMERS => &NODE_TIMERS,
        BUILTIN_NODE_TIMERS_PROMISES => &NODE_TIMERS_PROMISES,
        BUILTIN_NODE_UTIL => &NODE_UTIL,
        BUILTIN_NODE_URL => &NODE_URL,
        BUILTIN_NODE_ASYNC_HOOKS => &NODE_ASYNC_HOOKS,
        BUILTIN_NODE_DIAGNOSTICS_CHANNEL => &NODE_DIAGNOSTICS_CHANNEL,
        BUILTIN_NODE_DNS => &NODE_DNS,
        BUILTIN_NODE_DNS_PROMISES => &NODE_DNS_PROMISES,
        BUILTIN_NODE_FS => &NODE_FS,
        BUILTIN_NODE_FS_PROMISES => &NODE_FS_PROMISES,
        BUILTIN_NODE_HTTP => &NODE_HTTP,
        BUILTIN_NODE_HTTPS => &NODE_HTTPS,
        BUILTIN_NODE_NET => &NODE_NET,
        BUILTIN_NODE_PUNYCODE => &NODE_PUNYCODE,
        BUILTIN_NODE_STREAM => &NODE_STREAM,
        BUILTIN_NODE_STREAM_CONSUMERS => &NODE_STREAM_CONSUMERS,
        BUILTIN_NODE_STREAM_PROMISES => &NODE_STREAM_PROMISES,
        BUILTIN_NODE_TEST => &NODE_TEST,
        BUILTIN_NODE_TLS => &NODE_TLS,
        BUILTIN_NODE_ZLIB => &NODE_ZLIB,
        _ => unreachable!("unknown built-in module: {specifier}"),
    };
    slot.get_or_init(|| {
        (
            ModuleName::parse(specifier).expect("built-in module name is valid"),
            Module::new(ModuleKind::JavaScript, b"".as_slice())
                .expect("built-in module source is valid"),
        )
    })
}

fn digest(entry: &ModuleName, modules: &BTreeMap<ModuleName, Module>) -> BundleDigest {
    let mut digest = Sha256::new();
    digest.update(b"peren-worker-bundle-v1\0");
    hash_field(&mut digest, entry.as_ref().as_bytes());
    for (name, module) in modules {
        hash_field(&mut digest, name.as_ref().as_bytes());
        digest.update([match module.kind {
            ModuleKind::JavaScript => 0,
            ModuleKind::CommonJs => 1,
            ModuleKind::Wasm => 2,
            ModuleKind::Python => 3,
        }]);
        hash_field(&mut digest, &module.source);
    }
    BundleDigest(digest.finalize().into())
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .expect("bundle limits fit in u64")
            .to_be_bytes(),
    );
    digest.update(value);
}

#[derive(Debug, Error)]
pub enum BundleError {
    #[error("module name {0:?} must be a normalized relative bundle path")]
    ModuleName(String),
    #[error("module import {0:?} must be relative to the bundle")]
    ImportSpecifier(String),
    #[error("module import {0:?} escapes the bundle root")]
    ImportEscape(String),
    #[error("resolved module {0:?} is absent from the bundle")]
    MissingImport(String),
    #[error("module is {0} bytes; the limit is {MAX_MODULE_BYTES}")]
    ModuleTooLarge(usize),
    #[error("bundle has {0} modules; the limit is {MAX_MODULES}")]
    TooManyModules(usize),
    #[error("bundle is {0} bytes; the limit is {MAX_BUNDLE_BYTES}")]
    BundleTooLarge(usize),
    #[error("entry module {0:?} is absent from the bundle")]
    MissingEntry(String),
    #[error("entry module {0:?} must be JavaScript or CommonJS")]
    WasmEntry(String),
}
