//! Imports Cloudflare Wrangler, AWS SAM, and Serverless Framework configuration into Peren's
//! service model. Unsupported or ambiguous source behavior is reported instead of discarded.

use wrangler::WranglerToml;

mod binding;
mod cloudflare;
mod command;
mod compat;
mod error;
mod iam;
mod jsonc;
mod migration;
mod path;
mod sam;
mod serverless;
mod warning;
mod wrangler;
mod yaml;

const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;

fn refuse_if_source_too_large(format: &'static str, source: &str) -> Result<(), MigrateError> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(MigrateError::SourceTooLarge {
            format,
            bytes: source.len(),
            max_bytes: MAX_SOURCE_BYTES,
        });
    }
    Ok(())
}

pub use command::{
    Command, Error as CommandError, Imported, Notice, Receipt, Source, load_fleet, load_raw, run,
};
pub use error::MigrateError;
pub use migration::{MigratedDeployment, MigratedRunWorkerFirst, MigrationWarning};
pub use sam::import_sam_template;
pub use serverless::import_serverless_yml;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WranglerFormat {
    Toml,
    Json,
    Jsonc,
}

pub fn import_wrangler(
    source: &str,
    format: WranglerFormat,
) -> Result<MigratedDeployment, MigrateError> {
    refuse_if_source_too_large("wrangler config", source)?;
    let parsed: WranglerToml = match format {
        WranglerFormat::Toml => toml::from_str(source)?,
        WranglerFormat::Json => serde_json::from_str(source)?,
        WranglerFormat::Jsonc => {
            let stripped = jsonc::strip_comments_and_trailing_commas(source);
            serde_json::from_str(&stripped)?
        }
    };
    let mut migrated = cloudflare::translate(&parsed)?;

    let raw_keys: Vec<String> = match format {
        WranglerFormat::Toml => toml::from_str::<toml::Value>(source)
            .ok()
            .and_then(|v| v.as_table().map(|t| t.keys().cloned().collect()))
            .unwrap_or_default(),
        WranglerFormat::Json => serde_json::from_str::<serde_json::Value>(source)
            .ok()
            .and_then(|v| v.as_object().map(|o| o.keys().cloned().collect()))
            .unwrap_or_default(),
        WranglerFormat::Jsonc => {
            let stripped = jsonc::strip_comments_and_trailing_commas(source);
            serde_json::from_str::<serde_json::Value>(&stripped)
                .ok()
                .and_then(|v| v.as_object().map(|o| o.keys().cloned().collect()))
                .unwrap_or_default()
        }
    };
    for field in wrangler::unknown_top_level_fields(raw_keys.iter().map(String::as_str)) {
        migrated
            .warnings
            .push(MigrationWarning::FieldNotTranslated {
                field,
                reason: "a top-level field outside Peren's migration contract — no \
                         peren-native translation exists, and the importer cannot inspect this \
                         field safely enough to preserve it; the value was ignored",
            });
    }

    Ok(migrated)
}

pub fn import_wrangler_toml(toml_source: &str) -> Result<MigratedDeployment, MigrateError> {
    import_wrangler(toml_source, WranglerFormat::Toml)
}
