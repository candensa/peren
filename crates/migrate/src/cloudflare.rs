use crate::{
    compat::CompatFlagTable,
    error::MigrateError,
    migration::{
        MigratedDeployment, MigratedRunWorkerFirst, MigrationWarning, OPERATOR_INPUT_REQUIRED,
    },
    path::validate_relative_module_path,
    wrangler::{RunWorkerFirstValue, WranglerToml},
};
use peren_config::{Binding, D1Backend, KvBackend};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub(crate) fn translate(wrangler: &WranglerToml) -> Result<MigratedDeployment, MigrateError> {
    let mut warnings = validate(wrangler)?;
    let mut bindings = HashMap::new();
    bindings_from(wrangler, &mut bindings, &mut warnings)?;
    crate::warning::translate_unmapped(wrangler, &mut warnings);

    let plain_vars = wrangler
        .vars
        .iter()
        .filter_map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
        })
        .collect();
    let cron_expressions = wrangler
        .triggers
        .as_ref()
        .map(|triggers| triggers.crons.as_slice())
        .unwrap_or_default()
        .iter()
        .map(|expression| cloudflare_cron_to_peren_cron(expression))
        .collect::<Result<Vec<_>, _>>()?;
    let (assets_directory, assets_run_worker_first) =
        features_from(wrangler, &mut bindings, &mut warnings);
    let queue_consumers = wrangler
        .queues
        .as_ref()
        .map(|queues| {
            queues
                .consumers
                .iter()
                .map(|consumer| consumer.queue.clone())
                .collect()
        })
        .unwrap_or_default();

    Ok(MigratedDeployment {
        worker_name: wrangler.name.clone(),
        main_module_path: wrangler.main.clone(),
        compatibility_date: wrangler.compatibility_date.clone(),
        compatibility_flags: wrangler.compatibility_flags.clone(),
        bindings,
        plain_vars,
        cron_expressions,
        queue_consumers,
        warnings,
        assets_directory,
        assets_run_worker_first,
    })
}

fn validate(wrangler: &WranglerToml) -> Result<Vec<MigrationWarning>, MigrateError> {
    if let Err(reason) = validate_relative_module_path(&wrangler.main) {
        return Err(MigrateError::UnsafeModulePath {
            path: wrangler.main.clone(),
            reason,
        });
    }

    if !wrangler.send_email.is_empty() {
        return Err(MigrateError::UnsupportedDirective("send_email".into()));
    }
    if wrangler.browser.is_some() {
        return Err(MigrateError::UnsupportedDirective("browser".into()));
    }
    if wrangler.images.is_some() {
        return Err(MigrateError::UnsupportedDirective("images".into()));
    }

    let mut warnings = Vec::new();
    if wrangler.observability.as_ref().is_some_and(|o| o.enabled) {
        warnings.push(MigrationWarning::ObservabilityNotAutoTranslated);
    }
    if wrangler
        .observability
        .as_ref()
        .is_some_and(|o| o.head_sampling_rate.is_some())
    {
        warnings.push(MigrationWarning::FieldNotTranslated {
            field: "observability.head_sampling_rate".to_string(),
            reason: "Peren exporter sampling must be configured explicitly",
        });
    }
    let (unsupported_flags, partial_flags) = CompatFlagTable::check(&wrangler.compatibility_flags);
    if !unsupported_flags.is_empty() {
        return Err(MigrateError::UnsupportedCompatFlags(unsupported_flags));
    }
    for (flag, note) in partial_flags {
        warnings.push(MigrationWarning::PartiallySupportedCompatFlag { flag, note });
    }

    Ok(warnings)
}

fn bindings_from(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), MigrateError> {
    storage_from(wrangler, bindings, warnings);
    ai_from(wrangler, bindings, warnings);
    routes_from(wrangler, bindings, warnings);
    crate::binding::translate_flagship_bindings(wrangler, bindings, warnings);
    crate::binding::translate_analytics_engine(wrangler, bindings, warnings)
}

fn storage_from(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for kv in &wrangler.kv_namespaces {
        if kv.id.is_some() {
            warnings.push(MigrationWarning::FieldNotTranslated {
                field: format!("kv_namespaces[binding={}].id", kv.binding),
                reason: "the Cloudflare resource ID cannot identify a Peren KV namespace",
            });
        }
        let unique_key = mint_unique_key(&wrangler.name, "kv", &kv.binding);
        bindings.insert(
            kv.binding.clone(),
            Binding::Kv {
                namespace: kv.binding.clone(),
                unique_key,
                backend: KvBackend::default(),
            },
        );
    }

    for d1 in &wrangler.d1_databases {
        if d1.database_id.is_some() {
            warnings.push(MigrationWarning::FieldNotTranslated {
                field: format!("d1_databases[binding={}].database_id", d1.binding),
                reason: "the Cloudflare resource ID cannot identify a Peren database",
            });
        }
        let unique_key = mint_unique_key(&wrangler.name, "d1", &d1.database_name);
        bindings.insert(
            d1.binding.clone(),
            Binding::D1Database {
                database_name: d1.database_name.clone(),
                unique_key,
                backend: D1Backend::default(),
            },
        );
    }

    for r2 in &wrangler.r2_buckets {
        warnings.push(MigrationWarning::NeedsOperatorInput {
            binding_name: r2.binding.clone(),
            field: "endpoint (peren needs a real S3-compatible endpoint URL — Cloudflare's own R2 binding has none in wrangler.toml since it's internal to Cloudflare's network)",
        });
        warnings.push(MigrationWarning::NeedsOperatorInput {
            binding_name: r2.binding.clone(),
            field: "credential_scope (a ScopedCredential name the operator must mint separately — see credential-scoping-and-fleet-ca.md)",
        });
        bindings.insert(
            r2.binding.clone(),
            Binding::R2Bucket {
                endpoint: format!("{OPERATOR_INPUT_REQUIRED}-endpoint"),
                bucket: r2.bucket_name.clone(),
                credential_scope: format!(
                    "{OPERATOR_INPUT_REQUIRED}-credential_scope-{}",
                    r2.binding
                ),
                region: None,
                access_key_env: None,
                secret_key_env: None,
                token_env: None,
                allow_http: false,
                prefix: None,
                notifications: Vec::new(),
            },
        );
    }
}

fn ai_from(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    if let Some(ai) = &wrangler.ai {
        warnings.push(MigrationWarning::NeedsOperatorInput {
            binding_name: ai.binding.clone(),
            field: "endpoint (peren proxies env.AI.run() to a real, operator-supplied endpoint — \
                    any OpenAI-compatible server, Ollama, or a real cloud inference API; \
                    Cloudflare's own [ai] binding has none in wrangler.toml since it hosts the \
                    model internally)",
        });
        warnings.push(MigrationWarning::NeedsOperatorInput {
            binding_name: ai.binding.clone(),
            field: "credential_scope (a ScopedCredential name the operator must mint separately — see credential-scoping-and-fleet-ca.md)",
        });
        bindings.insert(
            ai.binding.clone(),
            Binding::Ai {
                endpoint: format!("{OPERATOR_INPUT_REQUIRED}-endpoint"),
                credential_scope: format!(
                    "{OPERATOR_INPUT_REQUIRED}-credential_scope-{}",
                    ai.binding
                ),
                provider: peren_config::AiProvider::Http,
            },
        );
    }
}

fn routes_from(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    if let Some(queues) = &wrangler.queues {
        for producer in &queues.producers {
            bindings.insert(
                producer.binding.clone(),
                Binding::Queue {
                    queue_name: producer.queue.clone(),
                },
            );
        }
    }

    if let Some(dos) = &wrangler.durable_objects {
        for binding in &dos.bindings {
            if binding.script_name.is_some() {
                warnings.push(MigrationWarning::FieldNotTranslated {
                    field: format!(
                        "durable_objects.bindings[name={}].script_name",
                        binding.name
                    ),
                    reason: "cross-service Durable Objects require explicit service wiring",
                });
            }
            let unique_key = mint_unique_key(&wrangler.name, "do", &binding.class_name);
            bindings.insert(
                binding.name.clone(),
                Binding::DurableObjectNamespace {
                    class_name: binding.class_name.clone(),
                    unique_key,
                },
            );
        }
    }

    for service in &wrangler.services {
        bindings.insert(
            service.binding.clone(),
            Binding::Service {
                entrypoint: service.service.clone(),
                props: serde_json::Value::Null,
                class_name: service.entrypoint.clone(),
            },
        );
    }
}

fn features_from(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) -> (Option<String>, Option<MigratedRunWorkerFirst>) {
    for loader in &wrangler.worker_loaders {
        let mut unknown_fields: Vec<&String> = loader.unknown.keys().collect();
        unknown_fields.sort_unstable();
        for field in unknown_fields {
            warnings.push(MigrationWarning::WorkerLoaderFieldNotTranslated {
                binding_name: loader.binding.clone(),
                field: field.clone(),
            });
        }
        bindings.insert(loader.binding.clone(), Binding::Loader);
    }

    for ns in &wrangler.dispatch_namespaces {
        let mut unknown_fields: Vec<&String> = ns.unknown.keys().collect();
        unknown_fields.sort_unstable();
        for field in unknown_fields {
            warnings.push(MigrationWarning::DispatchNamespaceFieldNotTranslated {
                binding_name: ns.binding.clone(),
                field: field.clone(),
            });
        }
        bindings.insert(
            ns.binding.clone(),
            Binding::Dispatcher {
                namespace: ns.namespace.clone(),
            },
        );
    }

    let mut assets_run_worker_first = None;
    let assets_directory = wrangler.assets.as_ref().map(|assets| {
        if let Some(binding_name) = &assets.binding {
            bindings.insert(binding_name.clone(), Binding::Assets);
        }
        if assets.html_handling.is_some() {
            warnings.push(MigrationWarning::AssetsFieldNotTranslated {
                field: "html_handling",
            });
        }
        if assets.not_found_handling.is_some() {
            warnings.push(MigrationWarning::AssetsFieldNotTranslated {
                field: "not_found_handling",
            });
        }
        assets_run_worker_first = assets.run_worker_first.as_ref().map(|rwf| match rwf {
            RunWorkerFirstValue::Always(b) => MigratedRunWorkerFirst::Always(*b),
            RunWorkerFirstValue::Patterns(patterns) => {
                MigratedRunWorkerFirst::Patterns(patterns.clone())
            }
        });
        assets.directory.clone()
    });

    (assets_directory, assets_run_worker_first)
}

fn cloudflare_cron_to_peren_cron(expression: &str) -> Result<String, MigrateError> {
    let field_count = expression.split_whitespace().count();
    if field_count != 5 {
        return Err(MigrateError::MalformedCronExpression {
            expression: expression.to_string(),
            reason: format!("expected 5 whitespace-separated fields, found {field_count}"),
        });
    }
    Ok(format!("0 {expression}"))
}

pub(crate) fn mint_unique_key(
    app_name: &str,
    binding_kind: &str,
    local_identifier: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(field_length(app_name).to_le_bytes());
    hasher.update(app_name.as_bytes());
    hasher.update(field_length(binding_kind).to_le_bytes());
    hasher.update(binding_kind.as_bytes());
    hasher.update(field_length(local_identifier).to_le_bytes());
    hasher.update(local_identifier.as_bytes());
    hex::encode(hasher.finalize())
}

fn field_length(value: &str) -> u32 {
    u32::try_from(value.len()).expect("migration input is capped below u32::MAX")
}
