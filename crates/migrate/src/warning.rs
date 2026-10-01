use crate::{
    migration::{MigrationWarning, MigrationWarning::FieldNotTranslated},
    wrangler::WranglerToml,
};

pub(crate) fn translate_unmapped(wrangler: &WranglerToml, warnings: &mut Vec<MigrationWarning>) {
    lifecycle_warnings(wrangler, warnings);
    runtime_warnings(wrangler, warnings);
    build_warnings(wrangler, warnings);
    account_warnings(wrangler, warnings);
}

fn lifecycle_warnings(wrangler: &WranglerToml, warnings: &mut Vec<MigrationWarning>) {
    for (i, entry) in wrangler.migrations.iter().enumerate() {
        let tag = entry.get("tag").and_then(|v| v.as_str());
        let field = match tag {
            Some(tag) => format!("migrations[{i}, tag={tag}]"),
            None => format!("migrations[{i}]"),
        };
        warnings.push(FieldNotTranslated {
            field,
            reason: "Peren's own Durable Object binding model has no equivalent migration-tag \
                     lifecycle (SQLite-vs-non-SQLite class tracking, renames, transfers) — a peren \
                     DO binding just needs a class_name; add or remove \
                     [[durable_objects.bindings]] entries directly instead",
        });
    }

    if wrangler.exports.is_some() {
        warnings.push(FieldNotTranslated {
            field: "exports".to_string(),
            reason: "the newer declarative Durable Object class-lifecycle mechanism — same \
                     reasoning as [[migrations]]: peren's own class binding model has no \
                     equivalent ceremony to replay",
        });
    }

    for i in 0..wrangler.routes.len() {
        warnings.push(FieldNotTranslated {
            field: format!("routes[{i}]"),
            reason: "routing/TLS/Custom Domains are selvedge's/warden's responsibility, not \
                     peren's — configure the equivalent route in your selvedge/warden config \
                     directly",
        });
    }
    if wrangler.route.is_some() {
        warnings.push(FieldNotTranslated {
            field: "route".to_string(),
            reason: "routing/TLS/Custom Domains are selvedge's/warden's responsibility, not \
                     peren's — configure the equivalent route in your selvedge/warden config \
                     directly",
        });
    }

    let mut env_names: Vec<&String> = wrangler.env.keys().collect();
    env_names.sort_unstable();
    for name in env_names {
        warnings.push(FieldNotTranslated {
            field: format!("env.{name}"),
            reason: "peren has no single-file, multi-environment mechanism analogous to \
                     [env.<name>] — deploy each environment as its own separate peren service \
                     with its own fleet.toml and bindings",
        });
    }
}

fn runtime_warnings(wrangler: &WranglerToml, warnings: &mut Vec<MigrationWarning>) {
    if wrangler.placement.is_some() {
        warnings.push(FieldNotTranslated {
            field: "placement".to_string(),
            reason: "Cloudflare Smart Placement has no peren-native equivalent — peren's own \
                     scheduler places cells independently and has no per-Worker placement hint \
                     today",
        });
    }

    if wrangler.limits.is_some() {
        warnings.push(FieldNotTranslated {
            field: "limits".to_string(),
            reason: "wrangler's limits.cpu_ms/subrequests configure Cloudflare's own per-Worker \
                     execution ceilings — peren's own [limits] table (fleet.toml) governs \
                     different, fleet-wide knobs and is not a substitute; configure peren's own \
                     limits directly if you need equivalent ceilings",
        });
    }

    if wrangler.logpush == Some(true) {
        warnings.push(FieldNotTranslated {
            field: "logpush".to_string(),
            reason: "Cloudflare Logpush has no peren-native destination — see the observability \
                     warning for peren's real, different answer (chronicle_export)",
        });
    }
}

fn build_warnings(wrangler: &WranglerToml, warnings: &mut Vec<MigrationWarning>) {
    if let Some(build) = &wrangler.build {
        if build.command.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.command".to_string(),
                reason: "peren deploys an already-bundled Worker bundle; it does not run a build \
                         step itself",
            });
        }
        if build.cwd.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.cwd".to_string(),
                reason: "a build-step working directory has no meaning when peren never runs a \
                         build step",
            });
        }
        if build.watch_dir.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.watch_dir".to_string(),
                reason: "wrangler dev's own file-watch/rebuild loop has no peren equivalent",
            });
        }
        if build.no_bundle.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.no_bundle".to_string(),
                reason: "a bundler opt-out has no meaning when peren never runs a build step",
            });
        }
        if build.minify.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.minify".to_string(),
                reason: "a bundler minification setting has no meaning when peren never runs a \
                         build step",
            });
        }
        if build.find_additional_modules.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.find_additional_modules".to_string(),
                reason: "wrangler's own module-discovery heuristic has no peren equivalent — \
                         additional modules for a peren deployment are named explicitly",
            });
        }
        if build.base_dir.is_some() {
            warnings.push(FieldNotTranslated {
                field: "build.base_dir".to_string(),
                reason: "a bundler base directory has no meaning when peren never runs a build \
                         step",
            });
        }
        let mut unknown_fields: Vec<&String> = build.unknown.keys().collect();
        unknown_fields.sort_unstable();
        for field in unknown_fields {
            warnings.push(FieldNotTranslated {
                field: format!("build.{field}"),
                reason: "no peren-native mapping exists for this build config field",
            });
        }
    }
}

fn account_warnings(wrangler: &WranglerToml, warnings: &mut Vec<MigrationWarning>) {
    if wrangler.workers_dev.is_some() {
        warnings.push(FieldNotTranslated {
            field: "workers_dev".to_string(),
            reason: "a Cloudflare-hosted-edge *.workers.dev preview subdomain has no \
                     self-hosted-peren equivalent",
        });
    }
    if wrangler.account_id.is_some() {
        warnings.push(FieldNotTranslated {
            field: "account_id".to_string(),
            reason: "identifies a Cloudflare account — meaningless for a self-hosted peren fleet, \
                     which has no Cloudflare account concept at all",
        });
    }
    if wrangler.preview_urls.is_some() {
        warnings.push(FieldNotTranslated {
            field: "preview_urls".to_string(),
            reason: "Cloudflare-hosted preview-deployment URLs have no peren equivalent",
        });
    }
    if wrangler.keep_vars.is_some() {
        warnings.push(FieldNotTranslated {
            field: "keep_vars".to_string(),
            reason: "controls whether a Cloudflare deploy preserves vars already live on the \
                     dashboard — peren has no separate 'live dashboard state' a deploy could \
                     diverge from",
        });
    }
    if wrangler.send_metrics.is_some() {
        warnings.push(FieldNotTranslated {
            field: "send_metrics".to_string(),
            reason: "Cloudflare's own anonymous usage-telemetry opt-in/out — not a peren concept",
        });
    }
    if wrangler.tsconfig.is_some() {
        warnings.push(FieldNotTranslated {
            field: "tsconfig".to_string(),
            reason: "a local wrangler-tooling/type-generation path with no runtime effect peren \
                     could translate",
        });
    }
    for i in 0..wrangler.rules.len() {
        warnings.push(FieldNotTranslated {
            field: format!("rules[{i}]"),
            reason: "custom bundler module-type rules are a build-time wrangler concept — peren \
                     has no equivalent module-loading customization surface",
        });
    }
    if !wrangler.define.is_empty() {
        warnings.push(FieldNotTranslated {
            field: "define".to_string(),
            reason: "build-time string substitution performed by wrangler's own bundler, not by \
                     peren at runtime",
        });
    }
    for i in 0..wrangler.tail_consumers.len() {
        warnings.push(FieldNotTranslated {
            field: format!("tail_consumers[{i}]"),
            reason: "Tail Workers (real-time log consumers) have no peren-native equivalent — see \
                     the observability warning for peren's real, different answer \
                     (chronicle_export)",
        });
    }
    if let Some(secrets) = &wrangler.secrets {
        for name in &secrets.required {
            warnings.push(FieldNotTranslated {
                field: format!("secrets.required[{name}]"),
                reason: "wrangler.toml only names a required secret; it never carries the actual \
                         secret value — configure this secret directly in your peren \
                         ServiceConfig's [services.secrets] table",
            });
        }
    }
}
