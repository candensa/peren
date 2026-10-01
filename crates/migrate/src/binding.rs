use crate::error::MigrateError;
use crate::migration::MigrationWarning::{FieldNotTranslated, NeedsOperatorInput};
use crate::wrangler::WranglerToml;
use crate::{
    cloudflare::mint_unique_key,
    migration::{MigrationWarning, OPERATOR_INPUT_REQUIRED},
};
use peren_config::Binding;
const DEFAULT_IDLE_SLEEP_SECS: u64 = 300;
use std::collections::{BTreeMap, HashMap};

pub(crate) fn translate_analytics_engine(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), MigrateError> {
    for entry in &wrangler.analytics_engine_datasets {
        let malformed = || MigrateError::UnsupportedDirective("analytics_engine_datasets".into());
        let table = entry.as_table().ok_or_else(malformed)?;
        let binding_name = table
            .get("binding")
            .and_then(|v| v.as_str())
            .ok_or_else(malformed)?;
        let dataset = table
            .get("dataset")
            .and_then(|v| v.as_str())
            .ok_or_else(malformed)?;

        warnings.push(NeedsOperatorInput {
            binding_name: binding_name.to_string(),
            field: "credential_scope (a ScopedCredential name the operator must mint separately — \
                    see credential-scoping-and-fleet-ca.md; real Cloudflare's own Analytics Engine \
                    binding has no such concept, but peren applies its own credential-scoping seam \
                    uniformly across every binding)",
        });
        bindings.insert(
            binding_name.to_string(),
            Binding::AnalyticsEngine {
                dataset: dataset.to_string(),
                credential_scope: format!(
                    "{OPERATOR_INPUT_REQUIRED}-credential_scope-{binding_name}"
                ),
            },
        );
    }
    Ok(())
}

pub(crate) fn translate_flagship_bindings(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    translate_vectorize(wrangler, bindings, warnings);
    translate_hyperdrive(wrangler, bindings, warnings);
    translate_workflows(wrangler, bindings, warnings);
    translate_mtls_certificates(wrangler, bindings, warnings);
    translate_secrets_store_secrets(wrangler, bindings, warnings);
    translate_containers(wrangler, bindings, warnings);
}

fn translate_vectorize(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for v in &wrangler.vectorize {
        warnings.push(NeedsOperatorInput {
            binding_name: v.binding.clone(),
            field: "endpoint (peren needs a real vector-database endpoint URL — pgvector/Qdrant/\
                    LanceDB — Cloudflare's own Vectorize binding has none in wrangler.toml since \
                    the index is internal to Cloudflare's network)",
        });
        warnings.push(NeedsOperatorInput {
            binding_name: v.binding.clone(),
            field: "credential_scope (a ScopedCredential name the operator must mint separately — \
                    see credential-scoping-and-fleet-ca.md)",
        });
        bindings.insert(
            v.binding.clone(),
            Binding::Vectorize {
                endpoint: format!(
                    "{OPERATOR_INPUT_REQUIRED}-endpoint-for-vectorize-index-{}",
                    v.index_name
                ),
                credential_scope: format!(
                    "{OPERATOR_INPUT_REQUIRED}-credential_scope-{}",
                    v.binding
                ),
                provider: peren_config::VectorProvider::Local,
            },
        );
    }
}

fn translate_hyperdrive(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for h in &wrangler.hyperdrive {
        warnings.push(NeedsOperatorInput {
            binding_name: h.binding.clone(),
            field: "pgcat_endpoint (peren needs a real, operator-run PgCat endpoint — Cloudflare's \
                    own Hyperdrive binding has none in wrangler.toml since pooling happens inside \
                    Cloudflare's network)",
        });
        warnings.push(NeedsOperatorInput {
            binding_name: h.binding.clone(),
            field: "credential_scope (a ScopedCredential name the operator must mint separately — \
                    see credential-scoping-and-fleet-ca.md)",
        });
        bindings.insert(
            h.binding.clone(),
            Binding::Hyperdrive {
                pgcat_endpoint: format!(
                    "{OPERATOR_INPUT_REQUIRED}-pgcat_endpoint-for-hyperdrive-id-{}",
                    h.id
                ),
                credential_scope: format!(
                    "{OPERATOR_INPUT_REQUIRED}-credential_scope-{}",
                    h.binding
                ),
                caching_disabled: false,
                max_age_secs: 60,
                stale_while_revalidate_secs: 15,
                pool_max_connections: 10,
            },
        );
    }
}

fn translate_workflows(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for w in &wrangler.workflows {
        if w.script_name.is_some() {
            warnings.push(FieldNotTranslated {
                field: format!("workflows[binding={}].script_name", w.binding),
                reason: "cross-service workflow references require explicit service wiring",
            });
        }
        if w.schedules.is_some() {
            warnings.push(FieldNotTranslated {
                field: format!("workflows[binding={}].schedules", w.binding),
                reason: "workflow schedules have no direct Peren binding equivalent",
            });
        }
        let unique_key = mint_unique_key(&wrangler.name, "workflow", &w.name);
        bindings.insert(
            w.binding.clone(),
            Binding::Workflow {
                class_name: w.class_name.clone(),
                unique_key,
            },
        );
    }
}

fn translate_mtls_certificates(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for m in &wrangler.mtls_certificates {
        warnings.push(NeedsOperatorInput {
            binding_name: m.binding.clone(),
            field: "cert_pem_env (the name of an environment variable peren reads real client- \
                    certificate PEM material from at deploy time — wrangler.toml's own \
                    certificate_id has no peren-side equivalent, since peren does not integrate \
                    with Cloudflare's own certificate store)",
        });
        warnings.push(NeedsOperatorInput {
            binding_name: m.binding.clone(),
            field: "key_pem_env (same as cert_pem_env, for the matching private key)",
        });
        bindings.insert(
            m.binding.clone(),
            Binding::MtlsCertificate {
                cert_pem_env: format!(
                    "{OPERATOR_INPUT_REQUIRED}-cert_pem_env-for-certificate-id-{}",
                    m.certificate_id
                ),
                key_pem_env: format!(
                    "{OPERATOR_INPUT_REQUIRED}-key_pem_env-for-certificate-id-{}",
                    m.certificate_id
                ),
            },
        );
    }
}

fn translate_secrets_store_secrets(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for s in &wrangler.secrets_store_secrets {
        let Some(secret_name) = &s.secret_name else {
            warnings.push(FieldNotTranslated {
                field: format!("secrets_store_secrets[binding={}]", s.binding),
                reason: "missing secret_name — real Cloudflare requires it, and there is nothing \
                         to translate without it",
            });
            continue;
        };
        bindings.insert(
            s.binding.clone(),
            Binding::SecretsStoreSecret {
                secret_name: secret_name.clone(),
            },
        );
        if s.store_id.is_some() {
            warnings.push(FieldNotTranslated {
                field: format!("secrets_store_secrets[binding={}].store_id", s.binding),
                reason: "Peren's own [secrets_store] fleet config is a single flat namespace keyed \
                         by secret_name, not scoped per Cloudflare store_id — verify secret_name \
                         is unique across every store this Worker used, or rename it",
            });
        }
    }
}

fn translate_containers(
    wrangler: &WranglerToml,
    bindings: &mut HashMap<String, Binding>,
    warnings: &mut Vec<MigrationWarning>,
) {
    for c in &wrangler.containers {
        let matching_do_binding_name = wrangler
            .durable_objects
            .as_ref()
            .and_then(|dos| dos.bindings.iter().find(|b| b.class_name == c.class_name))
            .map(|b| b.name.clone());

        let Some(binding_name) = matching_do_binding_name else {
            warnings.push(FieldNotTranslated {
                field: format!("containers[class_name={}]", c.class_name),
                reason: "no [[durable_objects.bindings]] entry names this class_name — real \
                         Cloudflare wires a Container's JS binding through a Durable Object \
                         binding pointing at this class; without a matching binding name peren has \
                         nothing to attach this container config to",
            });
            continue;
        };

        warnings.push(NeedsOperatorInput {
            binding_name: binding_name.clone(),
            field: "default_port (the container's real listening port — Cloudflare's own \
                    [[containers]] config has no equivalent field; peren's env.NAME.fetch(request) \
                    needs a real port the container image listens on)",
        });
        if c.instance_type.is_some() {
            warnings.push(FieldNotTranslated {
                field: format!("containers[class_name={}].instance_type", c.class_name),
                reason: "Cloudflare's instance_type sizing preset (dev/basic/standard) has no \
                         peren-native mapping to memory_mb/cpu_millis — set those directly on the \
                         peren binding if you need specific resource limits",
            });
        }
        if c.max_instances.is_some() {
            warnings.push(FieldNotTranslated {
                field: format!("containers[class_name={}].max_instances", c.class_name),
                reason: "Peren's Container binding does not autoscale a pool of instances the way \
                         Cloudflare's max_instances does — one container runs per binding scope",
            });
        }
        if c.name.is_some() {
            warnings.push(FieldNotTranslated {
                field: format!("containers[class_name={}].name", c.class_name),
                reason: "Peren's Container binding is addressed only by its JS binding name (from \
                         the matching durable_objects binding) — Cloudflare's own container name \
                         field is purely a dashboard label with no peren equivalent",
            });
        }
        let mut unknown_fields: Vec<&String> = c.unknown.keys().collect();
        unknown_fields.sort_unstable();
        for field in unknown_fields {
            warnings.push(FieldNotTranslated {
                field: format!("containers[class_name={}].{field}", c.class_name),
                reason: "no peren-native mapping exists for this container config field",
            });
        }

        bindings.insert(
            binding_name,
            Binding::Container {
                image: c.image.clone(),
                default_port: 0,
                env: BTreeMap::new(),
                memory_mb: None,
                cpu_millis: None,
                idle_sleep_secs: DEFAULT_IDLE_SLEEP_SECS,
                allow_network_egress: false,
            },
        );
    }
}
