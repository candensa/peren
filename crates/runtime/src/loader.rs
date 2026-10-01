use std::fmt::Write as _;

mod builtin;

use crate::{BundleError, ModuleKind, ModuleName, WorkerBundle, bundle::is_builtin};
use deno_core::{
    FastString, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader,
    ModuleResolveResponse, ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType,
    ResolutionKind,
};
use deno_error::JsErrorBox;

pub(crate) const MODULE_BASE: &str = "peren:///worker/";
const WASM_SOURCE_QUERY: &str = "?peren-wasm-source";

pub(crate) struct BundleLoader {
    bundle: WorkerBundle,
}

impl BundleLoader {
    pub(crate) const fn new(bundle: WorkerBundle) -> Self {
        Self { bundle }
    }

    pub(crate) fn specifier(name: &ModuleName) -> Result<ModuleSpecifier, BundleError> {
        ModuleSpecifier::parse(&format!("{MODULE_BASE}{name}"))
            .map_err(|_| BundleError::ModuleName(name.to_string()))
    }
}

impl ModuleLoader for BundleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        kind: ResolutionKind,
    ) -> ModuleResolveResponse {
        if matches!(kind, ResolutionKind::MainModule) {
            return ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err);
        }
        if is_builtin(specifier) {
            return ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err);
        }
        if specifier.starts_with("ext:") && is_builtin(referrer) {
            return ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err);
        }
        if let Some(name) = specifier
            .strip_prefix(MODULE_BASE)
            .and_then(|value| value.strip_suffix(WASM_SOURCE_QUERY))
        {
            let name =
                ModuleName::parse(name).map_err(|error| JsErrorBox::generic(error.to_string()))?;
            let module = self.bundle.module(&name).ok_or_else(|| {
                JsErrorBox::generic(format!("module {name:?} is absent from the Worker bundle"))
            })?;
            if module.kind() != ModuleKind::Wasm {
                return Err(JsErrorBox::generic(format!(
                    "module {name:?} is not a Wasm module"
                )));
            }
            return ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err);
        }
        if let Some(name) = specifier.strip_prefix(MODULE_BASE) {
            let name =
                ModuleName::parse(name).map_err(|error| JsErrorBox::generic(error.to_string()))?;
            if self.bundle.module(&name).is_none() {
                return Err(JsErrorBox::generic(format!(
                    "module {name:?} is absent from the Worker bundle"
                )));
            }
            return ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err);
        }
        let referrer = referrer
            .strip_prefix(MODULE_BASE)
            .ok_or_else(|| JsErrorBox::generic("module referrer is outside the Worker bundle"))?;
        let referrer =
            ModuleName::parse(referrer).map_err(|error| JsErrorBox::generic(error.to_string()))?;
        let resolved = referrer
            .resolve(specifier)
            .map_err(|error| JsErrorBox::generic(error.to_string()))?;
        Self::specifier(&resolved).map_err(|error| JsErrorBox::generic(error.to_string()))
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        let result = (|| {
            if let Some(source) = builtin::source(module_specifier.as_str()) {
                return Ok(ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(FastString::from_static(source)),
                    module_specifier,
                    None,
                ));
            }
            let relative = module_specifier
                .as_str()
                .strip_prefix(MODULE_BASE)
                .ok_or_else(|| JsErrorBox::generic("module is outside the Worker bundle"))?;
            let source_phase = relative.ends_with(WASM_SOURCE_QUERY);
            let name = relative.strip_suffix(WASM_SOURCE_QUERY).unwrap_or(relative);
            let name =
                ModuleName::parse(name).map_err(|error| JsErrorBox::generic(error.to_string()))?;
            let module = self.bundle.module(&name).ok_or_else(|| {
                JsErrorBox::generic(format!("module {name:?} is absent from the Worker bundle"))
            })?;
            if source_phase {
                if module.kind() != ModuleKind::Wasm {
                    return Err(JsErrorBox::generic(format!(
                        "module {name:?} is not a Wasm module"
                    )));
                }
                return Ok(ModuleSource::new(
                    ModuleType::Wasm,
                    ModuleSourceCode::Bytes(module.source().to_vec().into_boxed_slice().into()),
                    module_specifier,
                    None,
                ));
            }
            if module.kind() == ModuleKind::CommonJs {
                return Ok(ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(FastString::from(commonjs_source(
                        &self.bundle,
                        &name,
                    )?)),
                    module_specifier,
                    None,
                ));
            }
            if module.kind() == ModuleKind::Wasm {
                let raw = serde_json::to_string(&format!("{module_specifier}{WASM_SOURCE_QUERY}"))
                    .map_err(|error| JsErrorBox::generic(error.to_string()))?;
                let source = format!("import source compiled from {raw}; export default compiled;");
                return Ok(ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(FastString::from(source)),
                    module_specifier,
                    None,
                ));
            }
            if module.kind() == ModuleKind::Python {
                return Err(JsErrorBox::generic(format!(
                    "module {name:?} is a Python module and cannot be loaded by the JavaScript runtime"
                )));
            }
            let source = String::from_utf8(module.source().to_vec()).map_err(|_| {
                JsErrorBox::generic(format!("module {name:?} is not UTF-8 JavaScript"))
            })?;
            Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleSourceCode::String(FastString::from(source)),
                module_specifier,
                None,
            ))
        })();
        ModuleLoadResponse::Sync(result)
    }
}

fn commonjs_source(bundle: &WorkerBundle, entry: &ModuleName) -> Result<String, JsErrorBox> {
    let mut modules = String::new();
    let mut resolutions = String::new();
    for (name, module) in bundle.modules() {
        if module.kind() != ModuleKind::CommonJs {
            continue;
        }
        let name_json = json(name.as_ref())?;
        let source = String::from_utf8(module.source().to_vec())
            .map_err(|_| JsErrorBox::generic(format!("module {name:?} is not UTF-8 CommonJS")))?;
        let source_json = json(&source)?;
        writeln!(modules, "__modules.set({name_json}, {source_json});")
            .expect("writing to a string cannot fail");
        for (specifier, resolved) in require_edges(bundle, name, &source)? {
            let key = format!("{}:{}", name.as_ref(), specifier);
            writeln!(
                resolutions,
                "__resolutions.set({}, {});",
                json(&key)?,
                json(resolved.as_ref())?,
            )
            .expect("writing to a string cannot fail");
        }
    }
    let entry_json = json(entry.as_ref())?;
    Ok(format!(
        r#"
const __modules = new Map();
const __resolutions = new Map();
{modules}{resolutions}
const __cache = new Map();
function __load(name) {{
  if (__cache.has(name)) return __cache.get(name).exports;
  const source = __modules.get(name);
  if (source === undefined) throw new TypeError(`CommonJS module ${{name}} is absent from the Worker bundle`);
  const module = {{ exports: {{}} }};
  __cache.set(name, module);
  const require = (specifier) => {{
    const resolved = __resolutions.get(`${{name}}:${{specifier}}`);
    if (resolved === undefined) throw new TypeError(`CommonJS require ${{specifier}} from ${{name}} is not in the Worker bundle`);
    return __load(resolved);
  }};
  const execute = new Function("module", "exports", "require", source);
  execute(module, module.exports, require);
  return module.exports;
}}
const __exports = __load({entry_json});
export default (__exports && __exports.__esModule && "default" in __exports) ? __exports.default : __exports;
"#
    ))
}

fn require_edges(
    bundle: &WorkerBundle,
    referrer: &ModuleName,
    source: &str,
) -> Result<Vec<(String, ModuleName)>, JsErrorBox> {
    let mut edges = Vec::new();
    for quote in ['\'', '"'] {
        let needle = format!("require({quote}");
        let mut rest = source;
        while let Some(index) = rest.find(&needle) {
            let after = &rest[index + needle.len()..];
            let Some(end) = after.find(quote) else {
                break;
            };
            let specifier = &after[..end];
            let (resolved, module) = bundle
                .resolve(referrer, specifier)
                .map_err(|error| JsErrorBox::generic(error.to_string()))?;
            if module.kind() != ModuleKind::CommonJs {
                return Err(JsErrorBox::generic(format!(
                    "CommonJS require {specifier:?} from {referrer:?} does not resolve to CommonJS"
                )));
            }
            edges.push((specifier.to_string(), resolved.clone()));
            rest = &after[end + quote.len_utf8()..];
        }
    }
    Ok(edges)
}

fn json(value: &str) -> Result<String, JsErrorBox> {
    serde_json::to_string(value).map_err(|error| JsErrorBox::generic(error.to_string()))
}
