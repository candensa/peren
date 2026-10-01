use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use peren_config::Service;
use peren_runtime::{Module, ModuleKind, ModuleName, WorkerBundle};

use crate::process::ProcessError;

pub(crate) fn load(service: &Service) -> Result<WorkerBundle, ProcessError> {
    let root = root(service);
    let entry = name(&root, &service.worker_bundle_path)?;
    let mut files = vec![(entry.clone(), service.worker_bundle_path.clone())];
    for (configured, path) in &service.additional_modules {
        let module = if configured.is_empty() {
            name(&root, path)?
        } else {
            ModuleName::parse(configured.as_str())?
        };
        if files.iter().any(|(name, _)| name == &module) {
            continue;
        }
        files.push((module, path.clone()));
    }
    let mut index = 0;
    while index < files.len() {
        let (module, path) = files[index].clone();
        if matches!(kind(&path), ModuleKind::JavaScript | ModuleKind::CommonJs)
            && let Ok(source) = std::str::from_utf8(&read(&path)?)
        {
            for specifier in relative_specifiers(source) {
                let imported = module.resolve(specifier).map_err(ProcessError::Bundle)?;
                if files.iter().any(|(name, _)| name == &imported) {
                    continue;
                }
                let file = import_path(&path, specifier);
                if file.is_file() {
                    files.push((imported, file));
                }
            }
        }
        index += 1;
    }
    let mut modules = BTreeMap::new();
    for (module, path) in &files {
        modules.insert(module.clone(), Module::new(kind(path), read(path)?)?);
    }
    WorkerBundle::new(entry, modules).map_err(ProcessError::Bundle)
}

pub(crate) fn load_path(path: &Path) -> Result<WorkerBundle, ProcessError> {
    let root = path.parent().unwrap_or_else(|| Path::new(""));
    let entry = name(root, path)?;
    WorkerBundle::new(
        entry.clone(),
        BTreeMap::from([(entry, Module::new(kind(path), read(path)?)?)]),
    )
    .map_err(ProcessError::Bundle)
}

fn read(path: &Path) -> Result<Vec<u8>, ProcessError> {
    std::fs::read(path).map_err(|source| ProcessError::BundleFile {
        path: path.to_path_buf(),
        source,
    })
}

fn root(service: &Service) -> PathBuf {
    let mut root = service
        .worker_bundle_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_path_buf();
    for path in service.additional_modules.values() {
        let directory = path.parent().unwrap_or_else(|| Path::new(""));
        while !directory.starts_with(&root) {
            if !root.pop() {
                return PathBuf::new();
            }
        }
    }
    root
}

fn name(root: &Path, path: &Path) -> Result<ModuleName, ProcessError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| ProcessError::ModulePath(path.to_path_buf()))?;
    let value = relative
        .to_str()
        .ok_or_else(|| ProcessError::ModulePath(path.to_path_buf()))?;
    ModuleName::parse(value).map_err(ProcessError::Bundle)
}

fn import_path(from: &Path, specifier: &str) -> PathBuf {
    let mut path = from.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
    for component in specifier.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                path.pop();
            }
            value => path.push(value),
        }
    }
    path
}

fn relative_specifiers(source: &str) -> Vec<&str> {
    let mut found = Vec::new();
    let mut index = 0;
    while index < source.len() {
        let Some(rest) = source.get(index..) else {
            index += 1;
            continue;
        };
        let after = if let Some(value) = rest.strip_prefix("import") {
            value
        } else if let Some(value) = rest.strip_prefix("export") {
            value
        } else if let Some(value) = rest.strip_prefix("require") {
            value
        } else {
            index += 1;
            continue;
        };
        if index > 0 {
            let previous = source.as_bytes()[index - 1];
            if previous.is_ascii_alphanumeric() || previous == b'_' {
                index += 1;
                continue;
            }
        }
        let next = after.as_bytes().first().copied().unwrap_or(b' ');
        if next.is_ascii_alphanumeric() || next == b'_' {
            index += 1;
            continue;
        }
        if let Some(specifier) = first_specifier(after)
            && (specifier.starts_with("./") || specifier.starts_with("../"))
        {
            found.push(specifier);
        }
        index += 1;
    }
    found
}

fn first_specifier(source: &str) -> Option<&str> {
    let bytes = source.as_bytes();
    let mut index = 0;
    while index < bytes.len() && index < 240 {
        match bytes[index] {
            b'"' | b'\'' | b'`' => {
                let quote = bytes[index];
                let start = index + 1;
                let end = source[start..]
                    .bytes()
                    .position(|byte| byte == quote)
                    .map(|offset| start + offset)?;
                return Some(&source[start..end]);
            }
            b'\n' | b';' => return None,
            _ => index += 1,
        }
    }
    None
}

fn kind(path: &Path) -> ModuleKind {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("cjs") => ModuleKind::CommonJs,
        Some("wasm") => ModuleKind::Wasm,
        _ => ModuleKind::JavaScript,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn load_preserves_relative_module_names() {
        let root = std::env::temp_dir().join(format!("peren-bundle-{}", Uuid::new_v4()));
        let modules = root.join("modules");
        std::fs::create_dir_all(&modules).unwrap();
        let entry = root.join("worker.js");
        let helper = modules.join("helper.cjs");
        std::fs::write(&entry, "import './modules/helper.cjs'; export default {};").unwrap();
        std::fs::write(&helper, "module.exports = {};").unwrap();
        let config = format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
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
[services.additional_modules]
"" = "{}"
"#,
            entry.display(),
            helper.display()
        );
        let config = toml::from_str::<peren_config::FleetConfig>(&config)
            .unwrap()
            .validate()
            .unwrap();

        let bundle = load(&config.raw.services[0]).unwrap();

        assert!(
            bundle
                .module(&ModuleName::parse("worker.js").unwrap())
                .is_some()
        );
        assert!(
            bundle
                .module(&ModuleName::parse("modules/helper.cjs").unwrap())
                .is_some()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_follows_relative_imports() {
        let root = std::env::temp_dir().join(format!("peren-bundle-import-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let entry = root.join("worker.js");
        let wasm = root.join("answer.wasm");
        std::fs::write(&entry, "import compiled from './answer.wasm'; export default { fetch() { return new Response('ok'); } };").unwrap();
        std::fs::write(&wasm, [0, 97, 115, 109]).unwrap();
        let config = format!(
            r#"
[node]
node_id = "00000000-0000-0000-0000-000000000001"
advertise_addr = "127.0.0.1:0"
listen = "127.0.0.1:0"
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
"#,
            entry.display()
        );
        let config = toml::from_str::<peren_config::FleetConfig>(&config)
            .unwrap()
            .validate()
            .unwrap();

        let bundle = load(&config.raw.services[0]).unwrap();

        assert!(
            bundle
                .module(&ModuleName::parse("answer.wasm").unwrap())
                .is_some()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
