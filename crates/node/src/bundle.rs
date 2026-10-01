use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use peren_config::{Service, ServiceRuntime};
use peren_runtime::{
    Module, ModuleKind, ModuleName, PYTHON_PACKAGE_LOCK_MODULE, PythonPackageLock, WorkerBundle,
};

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
        let module_kind = kind(&path);
        if let Ok(source) = std::str::from_utf8(&read(&path)?) {
            if matches!(module_kind, ModuleKind::JavaScript | ModuleKind::CommonJs) {
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
            if module_kind == ModuleKind::Python {
                for (imported, file) in python_imports(root.as_path(), &path, source)? {
                    if files.iter().any(|(name, _)| name == &imported) {
                        continue;
                    }
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
    if service.runtime == ServiceRuntime::Python
        && let Some(path) = &service.python_packages_lock_path
    {
        let source = read(path)?;
        PythonPackageLock::parse(&source).map_err(ProcessError::PythonPackageLock)?;
        modules.insert(
            ModuleName::parse(PYTHON_PACKAGE_LOCK_MODULE)?,
            Module::new(ModuleKind::JavaScript, source)?,
        );
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

fn python_imports(
    root: &Path,
    from: &Path,
    source: &str,
) -> Result<Vec<(ModuleName, PathBuf)>, ProcessError> {
    let mut found = Vec::new();
    for line in source.lines() {
        let line = line.trim_start();
        let candidates = if let Some(rest) = line.strip_prefix("from ") {
            let Some((module, names)) = rest.split_once(" import ") else {
                continue;
            };
            from_python_import_candidates(module.trim(), names)
        } else if let Some(rest) = line.strip_prefix("import ") {
            rest.split(',')
                .filter_map(|part| part.split_whitespace().next())
                .map(str::to_string)
                .collect::<Vec<_>>()
        } else {
            continue;
        };
        for candidate in &candidates {
            if candidate.is_empty() || candidate.contains('*') {
                continue;
            }
            if candidate.starts_with('.') {
                if let Some((module, file)) = relative_python_import(root, from, candidate)? {
                    found.push((module, file));
                }
            } else if let Some((module, file)) = absolute_python_import(root, candidate)? {
                found.push((module, file));
            }
            for (module, file) in python_package_inits(root, from, candidate)? {
                found.push((module, file));
            }
        }
    }
    Ok(found)
}

fn from_python_import_candidates(module: &str, names: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    if !module.is_empty() {
        candidates.push(module.to_string());
    }
    for name in names.split(',') {
        let Some(name) = name.split_whitespace().next() else {
            continue;
        };
        if name.is_empty() || name == "*" {
            continue;
        }
        if module.is_empty() || module.chars().all(|value| value == '.') {
            candidates.push(format!("{module}{name}"));
        } else {
            candidates.push(format!("{module}.{name}"));
        }
    }
    candidates
}

fn relative_python_import(
    root: &Path,
    from: &Path,
    specifier: &str,
) -> Result<Option<(ModuleName, PathBuf)>, ProcessError> {
    let dots = specifier.chars().take_while(|value| *value == '.').count();
    let rest = &specifier[dots..];
    let mut base = from.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
    for _ in 1..dots {
        base.pop();
    }
    if !rest.is_empty() {
        for component in rest.split('.') {
            base.push(component);
        }
    }
    python_module_at(root, &base)
}

fn absolute_python_import(
    root: &Path,
    specifier: &str,
) -> Result<Option<(ModuleName, PathBuf)>, ProcessError> {
    let mut path = root.to_path_buf();
    for component in specifier.split('.') {
        path.push(component);
    }
    python_module_at(root, &path)
}

fn python_package_inits(
    root: &Path,
    from: &Path,
    specifier: &str,
) -> Result<Vec<(ModuleName, PathBuf)>, ProcessError> {
    let mut found = Vec::new();
    let mut base = if specifier.starts_with('.') {
        let dots = specifier.chars().take_while(|value| *value == '.').count();
        let rest = &specifier[dots..];
        let mut base = from.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        for _ in 1..dots {
            base.pop();
        }
        if rest.is_empty() {
            return Ok(found);
        }
        (base, rest)
    } else {
        (root.to_path_buf(), specifier)
    };
    let components: Vec<_> = base.1.split('.').collect();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        base.0.push(component);
        let init = base.0.join("__init__.py");
        if init.is_file() {
            found.push((name(root, &init)?, init));
        }
    }
    Ok(found)
}

fn python_module_at(
    root: &Path,
    base: &Path,
) -> Result<Option<(ModuleName, PathBuf)>, ProcessError> {
    let file = base.with_extension("py");
    if file.is_file() {
        return Ok(Some((name(root, &file)?, file)));
    }
    let init = base.join("__init__.py");
    if init.is_file() {
        return Ok(Some((name(root, &init)?, init)));
    }
    Ok(None)
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
        Some("py") => ModuleKind::Python,
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

    #[test]
    fn load_follows_local_python_imports() {
        let root = std::env::temp_dir().join(format!("peren-python-import-{}", Uuid::new_v4()));
        std::fs::create_dir_all(root.join("pkg")).unwrap();
        let entry = root.join("worker.py");
        let hello = root.join("hello.py");
        let pkg = root.join("pkg").join("__init__.py");
        let util = root.join("pkg").join("util.py");
        std::fs::write(
            &entry,
            "from hello import hello\nfrom pkg import util\nimport pkg\nfrom missing import ignored\n",
        )
        .unwrap();
        std::fs::write(&hello, "def hello(): return 'hello'\n").unwrap();
        std::fs::write(&pkg, "value = 'pkg'\n").unwrap();
        std::fs::write(&util, "value = 'util'\n").unwrap();
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
runtime = "python"
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
                .module(&ModuleName::parse("worker.py").unwrap())
                .is_some()
        );
        assert!(
            bundle
                .module(&ModuleName::parse("hello.py").unwrap())
                .is_some()
        );
        assert!(
            bundle
                .module(&ModuleName::parse("pkg/__init__.py").unwrap())
                .is_some()
        );
        assert!(
            bundle
                .module(&ModuleName::parse("pkg/util.py").unwrap())
                .is_some()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_embeds_python_package_lock() {
        let root = std::env::temp_dir().join(format!("peren-python-lock-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let entry = root.join("worker.py");
        let lock = root.join("peren-python-packages.json");
        std::fs::write(&entry, "from workers import Response\n").unwrap();
        std::fs::write(&lock, r#"{"version":1,"packages":["micropip"]}"#).unwrap();
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
runtime = "python"
worker_bundle_path = "{}"
python_packages_lock_path = "{}"
compatibility_date = "2026-01-01"
"#,
            entry.display(),
            lock.display()
        );
        let config = toml::from_str::<peren_config::FleetConfig>(&config)
            .unwrap()
            .validate()
            .unwrap();

        let bundle = load(&config.raw.services[0]).unwrap();
        let lock_module = bundle
            .module(&ModuleName::parse(PYTHON_PACKAGE_LOCK_MODULE).unwrap())
            .unwrap();

        assert_eq!(
            lock_module.source(),
            br#"{"version":1,"packages":["micropip"]}"#
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_rejects_unsafe_python_package_lock() {
        let root = std::env::temp_dir().join(format!("peren-python-bad-lock-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let entry = root.join("worker.py");
        let lock = root.join("peren-python-packages.json");
        std::fs::write(&entry, "from workers import Response\n").unwrap();
        std::fs::write(
            &lock,
            r#"{"version":1,"packages":["https://example.invalid/pkg.whl"]}"#,
        )
        .unwrap();
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
runtime = "python"
worker_bundle_path = "{}"
python_packages_lock_path = "{}"
compatibility_date = "2026-01-01"
"#,
            entry.display(),
            lock.display()
        );
        let config = toml::from_str::<peren_config::FleetConfig>(&config)
            .unwrap()
            .validate()
            .unwrap();

        let error = load(&config.raw.services[0]).unwrap_err();

        assert!(error.to_string().contains("Python package lock"), "{error}");
        std::fs::remove_dir_all(root).unwrap();
    }
}
