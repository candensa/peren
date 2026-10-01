use std::{collections::BTreeSet, str::FromStr};

use crate::Problem;

pub(super) fn services(config: &crate::FleetConfig, problems: &mut Vec<Problem>) {
    let service_names: BTreeSet<_> = config
        .services
        .iter()
        .map(|service| &service.name)
        .collect();
    for (index, service) in config.services.iter().enumerate() {
        for secret in service.secrets_store_refs.values() {
            if !config.secrets_store.contains_key(secret) {
                problems.push(Problem::new(
                    format!("services[{index}].secrets_store_refs"),
                    format!("unknown secret {secret:?}"),
                ));
            }
        }
        let entry_name = service
            .worker_bundle_path
            .file_name()
            .and_then(|name| name.to_str());
        if entry_name.is_some_and(|name| service.additional_modules.contains_key(name)) {
            problems.push(Problem::new(
                format!("services[{index}].additional_modules"),
                "entry module is also listed as an additional module",
            ));
        }
        if service
            .assets
            .as_ref()
            .and_then(|assets| assets.run_worker_first.as_ref())
            .is_some_and(|value| matches!(value, crate::RunWorkerFirst::Patterns(patterns) if patterns.len() > 100))
        {
            problems.push(Problem::new(
                format!("services[{index}].assets.run_worker_first"),
                "must contain at most 100 patterns",
            ));
        }
        super::validate_service_limits(service, index, problems);
        if chrono::NaiveDate::parse_from_str(&service.compatibility_date, "%Y-%m-%d").is_err() {
            problems.push(Problem::new(
                format!("services[{index}].compatibility_date"),
                "must be a valid date in YYYY-MM-DD format",
            ));
        }
        for (trigger_index, trigger) in service.cron_triggers.iter().enumerate() {
            if cron::Schedule::from_str(&trigger.expression).is_err() {
                problems.push(Problem::new(
                    format!("services[{index}].cron_triggers[{trigger_index}].expression"),
                    "must be a valid cron expression",
                ));
            }
        }
        for (name, binding) in &service.bindings {
            validate(
                binding,
                name,
                index,
                &service_names,
                service.assets.is_some(),
                problems,
            );
            if matches!(binding, crate::Binding::Container { .. }) && config.containers.is_none() {
                problems.push(Problem::new(
                    format!("services[{index}].bindings.{name}"),
                    "container binding requires the containers provider",
                ));
            }
        }
        if matches!(
            &service.entrypoint,
            crate::Entrypoint::DurableObject {
                container: Some(_),
                ..
            }
        ) && config.containers.is_none()
        {
            problems.push(Problem::new(
                format!("services[{index}].entrypoint.container"),
                "container entrypoint requires the containers provider",
            ));
        }
        for (tail_index, tail) in service.tail_consumers.iter().enumerate() {
            if !service_names.contains(&tail.service) {
                problems.push(Problem::new(
                    format!("services[{index}].tail_consumers[{tail_index}].service"),
                    "unknown tail consumer service",
                ));
            }
        }
    }
}

fn validate(
    binding: &crate::Binding,
    name: &str,
    service_index: usize,
    services: &BTreeSet<&String>,
    has_assets: bool,
    problems: &mut Vec<Problem>,
) {
    let field = format!("services[{service_index}].bindings.{name}");
    match binding {
        crate::Binding::Service { entrypoint, .. } if !services.contains(entrypoint) => {
            problems.push(Problem::new(field, "unknown service binding target"));
        }
        crate::Binding::R2Bucket { notifications, .. } if notifications.len() > 100 => {
            problems.push(Problem::new(
                format!("{field}.notifications"),
                "must contain at most 100 notification rules",
            ));
        }
        crate::Binding::R2Bucket { notifications, .. } => {
            for (index, notification) in notifications.iter().enumerate() {
                if notification.event_types.is_empty() {
                    problems.push(Problem::new(
                        format!("{field}.notifications[{index}].event_types"),
                        "must contain at least one event type",
                    ));
                }
            }
        }
        crate::Binding::Vectorize { provider, .. } => {
            super::provider::vector(provider, &field, problems);
        }
        crate::Binding::Ai { provider, .. } => {
            super::provider::ai(provider, &field, problems);
        }
        crate::Binding::AwsSigv4 {
            allowed_hosts,
            region,
            service,
            ..
        } => {
            if allowed_hosts.is_empty() {
                problems.push(Problem::new(
                    format!("{field}.allowed_hosts"),
                    "must contain at least one host",
                ));
            }
            if region.trim().is_empty() {
                problems.push(Problem::new(format!("{field}.region"), "must not be empty"));
            }
            if service.trim().is_empty() {
                problems.push(Problem::new(
                    format!("{field}.service"),
                    "must not be empty",
                ));
            }
        }
        crate::Binding::Hyperdrive {
            pool_max_connections: 0,
            ..
        } => {
            problems.push(Problem::new(
                format!("{field}.pool_max_connections"),
                "must be greater than zero",
            ));
        }
        crate::Binding::RateLimiter { limit: 0, .. } => {
            problems.push(Problem::new(
                format!("{field}.limit"),
                "must be greater than zero",
            ));
        }
        crate::Binding::RateLimiter { period_secs: 0, .. } => {
            problems.push(Problem::new(
                format!("{field}.period_secs"),
                "must be greater than zero",
            ));
        }
        crate::Binding::Images { provider } => super::provider::image(provider, &field, problems),
        crate::Binding::Assets if !has_assets => {
            problems.push(Problem::new(
                field,
                "assets binding requires service assets configuration",
            ));
        }
        _ => {}
    }
}
