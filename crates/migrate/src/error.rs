#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("failed to parse wrangler.toml: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to parse wrangler.json/.jsonc: {0}")]
    ParseJson(#[from] serde_json::Error),
    #[error("failed to parse {format}: {message}")]
    ParseYaml {
        format: &'static str,
        message: String,
    },
    #[error("wrangler directive {0:?} has no safe Peren translation")]
    UnsupportedDirective(String),
    #[error("{0}")]
    UnsupportedLambdaDirective(String),
    #[error("wrangler.toml references compatibility_flags peren does not support: {0:?}")]
    UnsupportedCompatFlags(Vec<String>),
    #[error("binding {binding_name:?} requires operator input for {field}")]
    UnresolvedOperatorInput { binding_name: String, field: String },
    #[error("cron trigger {expression:?} is not standard 5-field cron syntax: {reason}")]
    MalformedCronExpression { expression: String, reason: String },
    #[error("module path {path:?} is not a safe relative path: {reason}")]
    UnsafeModulePath { path: String, reason: String },
    #[error("{format} contains a YAML construct this importer refuses: {reason}")]
    UnsafeYamlConstruct {
        format: &'static str,
        reason: String,
    },
    #[error("{format} is {bytes} bytes, which is over this importer's {max_bytes}-byte limit")]
    SourceTooLarge {
        format: &'static str,
        bytes: usize,
        max_bytes: usize,
    },
}
